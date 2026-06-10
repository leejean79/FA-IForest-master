#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1 de-risk harness — direction 2(a).
Reference: HANDOVER_direction2a_phase1_derisk{,_2}.md

Scientific question:
    Does per-feature marginal drift (IKS on single-feature value streams)
    predict joint forest degradation (frozen-forest AUC dropping over time)?

Three stages per dataset (see HANDOVER §3-§5):
    GT-A:        frozen-forest sliding AUC(t) timeline + degradation onset
    feature-IKS: per-feature KS(t) + STABLE->fired rising edges (IKSSW, W=2000, p=0.001)
    align:       k-scan, latency / FP / FN metrics, verdict

Outputs (analysis/out/):
    derisk_summary.csv     dataset, GT_A_onset, best_k, agg_onset, latency, FP, FN, ...
    derisk_ksweep.csv      dataset, k, latency, FP, FN
    derisk_<dataset>.png   AUC(t) curve + GT-A onset + per-feature fire rug + driftspec markers

CLI:
    python3 analysis/derisk_proxy.py                            # run all datasets in datasets.yml
    python3 analysis/derisk_proxy.py --dataset <name>           # run just one (e.g. for INSECTS on leejean's box)
    python3 analysis/derisk_proxy.py --list                     # list datasets the yml exposes

Data-agnostic loader (HANDOVER §10):
- Honors deploy/datasets.yml fields hasHeader / hasId / hasLabel / labelPosition
  / anomalyLabel / dimensions exactly; no hardcoded column names or dim counts.
- Auto-sniffs delimiter (',' or tab).
- Sanity-checks the parsed feature count against the yml's `dimensions`.

Notes:
- KS inside the harness is value-based two-sample (np.searchsorted), which the
  deployed composite-key IKSSW (Java) upper-bounds by <= 1/W. For W=2000 that
  bias is ~5e-4, well under the fire threshold ca*sqrt(2/W) ~ 5.9e-2 at p=0.001,
  so fire decisions are equivalent. The faithful port lives in analysis/ikssw.py
  for byte-level equivalence checks on smaller slices.
- Datasets whose CSV is missing are auto-skipped (summary row records the skip).
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import numpy as np
import yaml

# matplotlib without an X display
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.ensemble import IsolationForest
from sklearn.metrics import roc_auc_score


# ---------------- project paths ----------------

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = PROJECT_ROOT / "analysis" / "out"
DATASETS_YML = PROJECT_ROOT / "deploy" / "datasets.yml"


# ---------------- parameters (HANDOVER §3-§5) ----------------

# IKSSW
W = 2000
P_VALUE = 0.001
# ca = sqrt(-0.5 * ln(p)); same constant as IKSConfig.ca in Java
CA = math.sqrt(-0.5 * math.log(P_VALUE))
KS_THRESHOLD = CA * math.sqrt(2.0 / W)
REBASE_ON_FIRE = True

# GT-A sliding window
GT_WIN = 2000
GT_STEP = 500
GT_DELTA = 0.05      # AUC degradation tolerance below baseline
GT_M = 2             # sustained windows to confirm degradation onset
GT_TRAIN_K = 4000    # normals sampled for the frozen-forest pre-drift training
IF_N_ESTIMATORS = 100
IF_MAX_SAMPLES = 256
SEED = 0

# alignment
AGG_WIN = W          # how far back agg onset looks for rising edges
TAU = GT_WIN         # FN tolerance window (samples)


# ---------------- dataset registry (datasets.yml driven) ----------------

# datasets where GT-A signal is weak (low absolute ceiling). Per HANDOVER §7, use
# a relative-drop delta for those. The default GT_DELTA is for synth ~1.0 baselines.
WEAK_GT_A_DATASETS = {"insects_abrupt_imbalanced", "insects_gradual_imbalanced"}


@dataclass
class DatasetSpec:
    """Carries everything the harness needs about one dataset, sourced from datasets.yml."""
    name: str
    csv_path: Path
    has_header: bool
    has_id: bool
    has_label: bool
    label_position: Any           # "last" | "first" | int (0-based)
    anomaly_label: str            # string per yml convention (e.g. "1")
    dimensions: int
    drift_starts: List[int] = field(default_factory=list)
    delta: float = GT_DELTA       # AUC degradation tolerance below baseline


def _load_yml() -> Dict[str, Any]:
    with open(DATASETS_YML) as f:
        return yaml.safe_load(f) or {}


def _resolve_drift_starts(name: str, meta: Dict[str, Any]) -> List[int]:
    """drift_starts source priority: meta['drift_events'] (yml inline) -> drift_spec JSON."""
    if "drift_events" in meta:
        return [int(x) for x in meta["drift_events"]]
    spec_rel = meta.get("drift_spec")
    if spec_rel:
        spec_path = PROJECT_ROOT / spec_rel
        if spec_path.exists():
            with open(spec_path) as f:
                spec = json.load(f) or {}
            return [int(e["start_line"]) for e in spec.get("drift_events", [])]
    return []


def all_dataset_specs() -> List[DatasetSpec]:
    yml = _load_yml()
    datasets = yml.get("datasets", {}) or {}
    specs: List[DatasetSpec] = []
    for name, meta in datasets.items():
        # only consider entries we actually want to evaluate; the harness skips
        # gracefully on missing CSVs, so list everything driver provides.
        csv_path = PROJECT_ROOT / meta["path"]
        delta = 0.10 if name in WEAK_GT_A_DATASETS else GT_DELTA
        specs.append(DatasetSpec(
            name=name,
            csv_path=csv_path,
            has_header=bool(meta.get("hasHeader", True)),
            has_id=bool(meta.get("hasId", False)),
            has_label=bool(meta.get("hasLabel", True)),
            label_position=meta.get("labelPosition", "last"),
            anomaly_label=str(meta.get("anomalyLabel", "1")),
            dimensions=int(meta.get("dimensions", 0)),
            drift_starts=_resolve_drift_starts(name, meta),
            delta=delta,
        ))
    return specs


# ---------------- CSV loader (data-agnostic, HANDOVER §10) ----------------

def _sniff_delimiter(csv_path: Path) -> str:
    with open(csv_path) as f:
        sample = f.read(8192)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        return dialect.delimiter
    except csv.Error:
        # fall back: pick whichever common delim shows up most in first line
        first = sample.splitlines()[0] if sample else ""
        return "\t" if first.count("\t") > first.count(",") else ","


def _resolve_label_index(label_position: Any, n_cols: int) -> int:
    if isinstance(label_position, int):
        return int(label_position)
    if isinstance(label_position, str):
        lp = label_position.strip().lower()
        if lp == "last":
            return n_cols - 1
        if lp == "first":
            return 0
        if lp.lstrip("-").isdigit():
            return int(lp)
    raise ValueError(f"unrecognized labelPosition: {label_position!r}")


def load_dataset(spec: DatasetSpec) -> Tuple[np.ndarray, np.ndarray]:
    """
    Returns X (n, D) float and y (n,) int from `spec.csv_path`. Pure yml-driven:
    no column-name lookup, label by position, anomalyLabel string comparison.
    """
    delim = _sniff_delimiter(spec.csv_path)
    rows: List[List[str]] = []
    n_cols: Optional[int] = None
    with open(spec.csv_path) as f:
        r = csv.reader(f, delimiter=delim)
        if spec.has_header:
            header = next(r)
            n_cols = len(header)
        for line in r:
            if not line:
                continue
            if n_cols is None:
                n_cols = len(line)
            rows.append(line)
    if n_cols is None:
        raise RuntimeError(f"{spec.csv_path}: empty CSV")
    li = _resolve_label_index(spec.label_position, n_cols) if spec.has_label else -1
    id_col = 0 if spec.has_id else -1
    feat_idx = [i for i in range(n_cols) if i != li and i != id_col]
    if spec.dimensions and len(feat_idx) != spec.dimensions:
        raise RuntimeError(
            f"{spec.name}: parsed {len(feat_idx)} feature cols, yml says dimensions={spec.dimensions}; "
            f"check hasHeader / hasId / labelPosition")
    X = np.array([[float(row[i]) for i in feat_idx] for row in rows], dtype=float)
    if spec.has_label:
        anom = spec.anomaly_label
        y = np.array([1 if row[li].strip() == anom else 0 for row in rows], dtype=int)
    else:
        y = np.zeros(len(rows), dtype=int)
    return X, y


# ---------------- GT-A: frozen forest sliding AUC ----------------

def _sample_normals(Xs: np.ndarray, ys: np.ndarray, k: int,
                    rng: np.random.RandomState) -> np.ndarray:
    idx = np.where(ys == 0)[0]
    if len(idx) > k:
        idx = rng.choice(idx, k, replace=False)
    return Xs[idx]


@dataclass
class GTAResult:
    centers: np.ndarray         # window centers (sample seq)
    aucs: np.ndarray            # per-window AUC (NaN if undefined)
    baseline: float             # mean AUC across pre-drift windows
    degradation_onset: Optional[int]  # first sustained center where auc < baseline-delta
    pre_end: int


def gt_a(X: np.ndarray, y: np.ndarray, pre_end: int, *,
         seed: int = SEED, delta: float = GT_DELTA) -> GTAResult:
    """Train frozen forest on pre-drift normals, slide AUC across the whole stream."""
    rng = np.random.RandomState(seed)
    train_X = _sample_normals(X[:pre_end], y[:pre_end], GT_TRAIN_K, rng)
    clf = IsolationForest(n_estimators=IF_N_ESTIMATORS,
                          max_samples=min(IF_MAX_SAMPLES, len(train_X)),
                          random_state=seed, contamination='auto')
    clf.fit(train_X)
    scores = -clf.score_samples(X)  # higher = more anomalous

    n = len(X)
    starts = np.arange(0, n - GT_WIN + 1, GT_STEP)
    centers = starts + GT_WIN // 2
    aucs = np.full(len(starts), np.nan)
    for k, i in enumerate(starts):
        ywin = y[i:i + GT_WIN]
        if ywin.min() == ywin.max():
            continue  # one-class window
        aucs[k] = roc_auc_score(ywin, scores[i:i + GT_WIN])

    # baseline = mean of pre-drift windows (centers < pre_end)
    pre_mask = (centers < pre_end) & ~np.isnan(aucs)
    baseline = float(np.mean(aucs[pre_mask])) if pre_mask.any() else float("nan")

    # degradation onset: first center > pre_end where auc < baseline-delta sustained m windows
    deg_onset: Optional[int] = None
    if not math.isnan(baseline):
        below = (aucs < baseline - delta) & (centers > pre_end)
        # convolve to enforce sustained m windows
        if GT_M <= 1:
            sustained_mask = below
        else:
            kern = np.ones(GT_M, dtype=int)
            conv = np.convolve(below.astype(int), kern, mode="valid")
            sustained = conv >= GT_M
            sustained_mask = np.concatenate([sustained,
                                             np.zeros(GT_M - 1, dtype=bool)])
        hits = np.where(sustained_mask)[0]
        if len(hits):
            deg_onset = int(centers[hits[0]])

    return GTAResult(centers=centers, aucs=aucs, baseline=baseline,
                     degradation_onset=deg_onset, pre_end=pre_end)


# ---------------- per-feature IKSSW (vectorized, value-based KS) ----------------

@dataclass
class FeatureFire:
    feature: int
    onsets: List[int]   # sample seq where Test transitioned False -> True
    ks_seq: np.ndarray  # KS at each evaluation point (length n - W)
    eval_seq: np.ndarray  # the seq positions at which ks_seq was recorded


class _VFastIKSSW:
    """
    Vectorized value-based IKSSW. Public surface (Increment / KS / Test / Update)
    matches analysis/ikssw.py. Used inside the harness inner loop.

    Mathematical equivalence to the deployed composite-key IKSSW: this gives the
    true value-based KS; the deployed IKSSW upper-bounds it by <= 1/W. For
    W=2000 that bias is ~5e-4, ~2 orders of magnitude below the fire threshold,
    so Test() decisions match. See ikssw.py for the byte-level mirror.
    """

    def __init__(self, reference: np.ndarray):
        ref_arr = np.asarray(reference, dtype=float)
        self.W = len(ref_arr)
        # ref must be a frozen sorted snapshot; cur is FIFO in TEMPORAL order
        # (so the next Increment evicts the temporally oldest, not the smallest).
        self._ref = np.sort(ref_arr)
        self._cur = deque(ref_arr.tolist(), maxlen=self.W)
        # maintain sorted current via bisect (O(W) per op but tight C-level)
        from bisect import bisect_left, insort
        self._bisect_left = bisect_left
        self._insort = insort
        self._cur_sorted = sorted(self._cur)

    def Increment(self, v: float) -> None:
        old = self._cur[0]   # leftmost — the one deque will evict on append
        self._cur.append(float(v))
        idx = self._bisect_left(self._cur_sorted, old)
        del self._cur_sorted[idx]
        self._insort(self._cur_sorted, float(v))

    def KS(self) -> float:
        ref = self._ref
        cur = np.asarray(self._cur_sorted)
        merged = np.concatenate([ref, cur])
        merged.sort()
        F_cur = np.searchsorted(cur, merged, side='right')
        F_ref = np.searchsorted(ref, merged, side='right')
        return float(np.max(np.abs(F_cur - F_ref))) / self.W

    def Test(self, ca: float) -> bool:
        return self.KS() > ca * math.sqrt(2.0 / self.W)

    def Update(self) -> None:
        self._ref = np.asarray(self._cur_sorted, dtype=float).copy()


def per_feature_iks(X: np.ndarray, *, ca: float = CA,
                    rebase_on_fire: bool = REBASE_ON_FIRE,
                    eval_stride: int = 1) -> List[FeatureFire]:
    """
    For each feature column, run IKSSW: warm-up on X[:W, d], then slide for
    i in [W, n). Records (seq, KS, fired) every `eval_stride` samples and
    returns rising-edge onsets. If rebase_on_fire, calls Update() once per
    fired onset (matches deployed reset-on-COMMITTED semantics).
    """
    n, D = X.shape
    out: List[FeatureFire] = []
    for d in range(D):
        col = X[:, d]
        ikssw = _VFastIKSSW(col[:W])
        onsets: List[int] = []
        # pre-allocate eval arrays
        eval_positions = list(range(W, n, eval_stride))
        ks_seq = np.zeros(len(eval_positions), dtype=float)
        last_fired = False
        ep_iter = iter(eval_positions)
        try:
            next_ep = next(ep_iter)
        except StopIteration:
            next_ep = -1
        write_idx = 0
        eval_seq_list: List[int] = []
        for i in range(W, n):
            ikssw.Increment(float(col[i]))
            if i == next_ep:
                ks = ikssw.KS()
                fired = ks > ca * math.sqrt(2.0 / W)
                ks_seq[write_idx] = ks
                eval_seq_list.append(i)
                write_idx += 1
                if fired and not last_fired:
                    onsets.append(i)
                    if rebase_on_fire:
                        ikssw.Update()
                        fired = False  # post-rebase KS is small again
                last_fired = fired
                try:
                    next_ep = next(ep_iter)
                except StopIteration:
                    next_ep = -1
        out.append(FeatureFire(feature=d, onsets=onsets,
                               ks_seq=ks_seq[:write_idx],
                               eval_seq=np.asarray(eval_seq_list, dtype=int)))
    return out


# ---------------- alignment ----------------

@dataclass
class AggResult:
    k: int
    agg_onsets: List[int]   # seq positions where >= k features fired within agg_win
    latency: Optional[int]  # first agg_onset after gta_onset minus gta_onset
    fp: int                 # agg onsets falling in stable region
    fn: int                 # GT-A degradation segments without an agg onset within tau


def aggregate_onsets(features: List[FeatureFire], n: int, k: int,
                     agg_win: int = AGG_WIN) -> List[int]:
    """
    Returns seq positions where the count of features with at least one rising
    edge in [i - agg_win, i] transitions from < k to >= k.
    Evaluated only at positions where some feature fires (others are no-ops).
    """
    # Flatten all feature onsets with their feature id, sort by seq.
    events: List[Tuple[int, int]] = []
    for f in features:
        for o in f.onsets:
            events.append((o, f.feature))
    events.sort()
    # Sliding window over events.
    agg_onsets: List[int] = []
    window: Deque[Tuple[int, int]] = deque()
    distinct: Dict[int, int] = {}
    armed = False  # need a False->True transition for "onset"
    for seq, feat in events:
        # evict out-of-window events
        while window and window[0][0] < seq - agg_win:
            _, old_feat = window.popleft()
            distinct[old_feat] -= 1
            if distinct[old_feat] == 0:
                del distinct[old_feat]
        window.append((seq, feat))
        distinct[feat] = distinct.get(feat, 0) + 1
        if len(distinct) >= k:
            if not armed:
                agg_onsets.append(seq)
                armed = True
        else:
            armed = False
    return agg_onsets


def gta_degradation_segments(gta: GTAResult, n: int) -> List[Tuple[int, int]]:
    """Returns [(start_seq, end_seq), ...] segments where AUC stays below baseline-delta."""
    if math.isnan(gta.baseline):
        return []
    below = (gta.aucs < gta.baseline - GT_DELTA)
    segs: List[Tuple[int, int]] = []
    in_seg = False
    seg_start = 0
    for i in range(len(gta.centers)):
        if below[i] and not in_seg:
            seg_start = int(gta.centers[i])
            in_seg = True
        elif not below[i] and in_seg:
            segs.append((seg_start, int(gta.centers[i - 1])))
            in_seg = False
    if in_seg:
        segs.append((seg_start, int(gta.centers[-1])))
    return segs


def aggregate_metrics(features: List[FeatureFire], gta: GTAResult, n: int,
                      k: int, *, drift_starts: List[int]) -> AggResult:
    agg_onsets = aggregate_onsets(features, n, k)

    # latency: signed offset of first agg onset that falls in the "useful
    # anticipation window" [first_drift - TAU, GT-A onset + TAU]. The proxy
    # is allowed to anticipate as far back as the first drift event (which is
    # the earliest moment the underlying distribution has actually moved);
    # late detection is bounded by TAU past GT-A. None only if no agg onset
    # exists in this window.
    latency: Optional[int] = None
    first_drift_for_lat = drift_starts[0] if drift_starts else gta.degradation_onset
    if gta.degradation_onset is not None and first_drift_for_lat is not None:
        lo = first_drift_for_lat - TAU
        hi = gta.degradation_onset + TAU
        near = [o for o in agg_onsets if lo <= o <= hi]
        if near:
            latency = near[0] - gta.degradation_onset

    # FP = agg onsets in the *pre-drift* stable region (before any known drift
    # event minus TAU). Anticipatory fires during a known transition are not
    # false; they're correct early signal. If there are no drift events, treat
    # the whole stream as stable for FP purposes.
    first_drift = drift_starts[0] if drift_starts else n
    fp = sum(1 for o in agg_onsets if o < first_drift - TAU)

    # FN: GT-A degradation segments without agg onset within TAU
    segs = gta_degradation_segments(gta, n)
    fn = 0
    for s_start, s_end in segs:
        hit = any(s_start - TAU <= o <= s_end + TAU for o in agg_onsets)
        if not hit:
            fn += 1

    return AggResult(k=k, agg_onsets=agg_onsets, latency=latency, fp=fp, fn=fn)


def features_fired_near(features: List[FeatureFire], anchor: int,
                        win: int = AGG_WIN) -> List[int]:
    """Which feature ids fire within +/- win of `anchor` (for interpretability)."""
    if anchor is None:
        return []
    out: List[int] = []
    for f in features:
        if any(anchor - win <= o <= anchor + win for o in f.onsets):
            out.append(f.feature)
    return out


def pick_verdict(best: AggResult, gta: GTAResult) -> str:
    if gta.degradation_onset is None:
        if best.fp == 0:
            return "no_GT_signal+no_FP"
        return "no_GT_signal+spurious_fires(mode1?)"
    # latency is None only if no agg onset in [first_drift-TAU, GT_A+TAU].
    # Within that window, negative = anticipation (good), positive <= TAU = aligned.
    aligned = best.latency is not None
    if aligned and best.fp == 0 and best.fn == 0:
        return "proxy_holds"
    if best.fn > 0:
        return "mode2_miss"
    if best.fp > 0:
        return "mode1_false_fire"
    return "indeterminate"


# ---------------- plotting ----------------

def overlay_plot(spec: DatasetSpec, gta: GTAResult,
                 features: List[FeatureFire], best: AggResult, out_path: Path):
    fig, ax = plt.subplots(figsize=(12, 5))
    # AUC curve
    ax.plot(gta.centers, gta.aucs, color="steelblue", lw=1.2, label="GT-A AUC(t)")
    if not math.isnan(gta.baseline):
        ax.axhline(gta.baseline, color="navy", lw=0.8, ls=":", label=f"baseline={gta.baseline:.3f}")
        ax.axhline(gta.baseline - GT_DELTA, color="navy", lw=0.6, ls="--",
                   label=f"baseline-delta={gta.baseline - GT_DELTA:.3f}")
    if gta.degradation_onset is not None:
        ax.axvline(gta.degradation_onset, color="red", lw=1.0,
                   label=f"GT-A onset={gta.degradation_onset}")
    # driftspec markers
    for s in spec.drift_starts:
        ax.axvline(s, color="gray", lw=0.6, ls=":", alpha=0.7)
    # agg onsets
    for o in best.agg_onsets:
        ax.axvline(o, color="orange", lw=0.8, alpha=0.7)
    # per-feature fire rug
    n_feat = len(features)
    rug_y0 = ax.get_ylim()[0] - 0.05
    for f in features:
        y_band = rug_y0 - 0.04 * (f.feature + 1)
        if f.onsets:
            ax.scatter(f.onsets, [y_band] * len(f.onsets), s=20, marker="|",
                       color=f"C{f.feature % 10}", label=f"f{f.feature} fire")
    ax.set_xlabel("seq")
    ax.set_ylabel("AUC")
    ax.set_title(f"{spec.name}  (best k={best.k}, latency={best.latency}, FP={best.fp}, FN={best.fn})")
    # avoid label duplication in legend
    h, l = ax.get_legend_handles_labels()
    seen = set()
    keep = [(hh, ll) for hh, ll in zip(h, l) if not (ll in seen or seen.add(ll))]
    ax.legend([k[0] for k in keep], [k[1] for k in keep], fontsize=7, loc="lower left")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)


# ---------------- driver ----------------

def run_dataset(spec: DatasetSpec, k_grid: List[int]) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """Returns (summary_row, [ksweep_rows...]). Skips gracefully if CSV is missing."""
    if not spec.csv_path.exists():
        return ({
            "dataset": spec.name,
            "GT_A_onset": "NA",
            "best_k": "NA",
            "agg_onset": "NA",
            "latency": "NA",
            "FP": "NA",
            "FN": "NA",
            "features_fired": "NA",
            "verdict": f"skipped:no_csv({spec.csv_path.relative_to(PROJECT_ROOT)})"
        }, [])

    print(f"=== {spec.name} ===")
    print(f"  load {spec.csv_path}")
    X, y = load_dataset(spec)
    n, D = X.shape
    print(f"  n={n}, D={D}, anomaly_rate={y.mean():.4f}, drift_starts={spec.drift_starts}, delta={spec.delta}")
    if not spec.drift_starts:
        return ({
            "dataset": spec.name, "GT_A_onset": "NA", "best_k": "NA",
            "agg_onset": "NA", "latency": "NA", "FP": "NA", "FN": "NA",
            "features_fired": "NA", "verdict": "skipped:no_drift_starts",
        }, [])
    pre_end = spec.drift_starts[0]

    print(f"  GT-A: train on X[:{pre_end}] normals, slide win={GT_WIN} step={GT_STEP}")
    gta = gt_a(X, y, pre_end=pre_end, seed=SEED, delta=spec.delta)
    print(f"    baseline={gta.baseline:.4f}  degradation_onset={gta.degradation_onset}")

    # k_grid scoped to D
    k_grid = sorted(set([k for k in k_grid if 1 <= k <= D] + [1, D]))

    # per-feature IKS — eval at GT-A stride to keep things fast; rising edges
    # are still detected at GT_STEP resolution which matches the alignment granularity.
    print(f"  per-feature IKSSW: W={W} p={P_VALUE} ca={CA:.4f} thr={KS_THRESHOLD:.5f}")
    features = per_feature_iks(X, ca=CA, rebase_on_fire=REBASE_ON_FIRE,
                               eval_stride=max(1, GT_STEP // 4))
    for f in features:
        print(f"    f{f.feature}: {len(f.onsets)} fire onsets" +
              (f" first@{f.onsets[0]}" if f.onsets else ""))

    print(f"  alignment k-scan over {k_grid}")
    ksweep_rows: List[Dict[str, str]] = []
    results: Dict[int, AggResult] = {}
    for k in k_grid:
        ar = aggregate_metrics(features, gta, n, k, drift_starts=spec.drift_starts)
        results[k] = ar
        ksweep_rows.append({
            "dataset": spec.name, "k": str(k),
            "latency": "" if ar.latency is None else str(ar.latency),
            "FP": str(ar.fp), "FN": str(ar.fn),
            "n_agg_onsets": str(len(ar.agg_onsets)),
        })

    # pick best k: prefer FN=0 + FP=0 + smallest latency; tie-break smaller k
    def score(ar: AggResult) -> Tuple[int, int, float, int]:
        lat = ar.latency if ar.latency is not None else 10 ** 9
        return (ar.fn, ar.fp, lat, ar.k)
    best_k = min(results, key=lambda kk: score(results[kk]))
    best = results[best_k]

    fired = features_fired_near(features, gta.degradation_onset or 0)
    summary = {
        "dataset": spec.name,
        "GT_A_onset": "" if gta.degradation_onset is None else str(gta.degradation_onset),
        "best_k": str(best_k),
        "agg_onset": "" if not best.agg_onsets else str(best.agg_onsets[0]),
        "latency": "" if best.latency is None else str(best.latency),
        "FP": str(best.fp),
        "FN": str(best.fn),
        "features_fired": "|".join(str(x) for x in fired),
        "verdict": pick_verdict(best, gta),
    }
    print(f"  verdict: {summary['verdict']}  best_k={best_k}  latency={best.latency}  FP={best.fp}  FN={best.fn}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plot_path = OUT_DIR / f"derisk_{spec.name}.png"
    overlay_plot(spec, gta, features, best, plot_path)
    print(f"  -> {plot_path.relative_to(PROJECT_ROOT)}")

    return summary, ksweep_rows


def main():
    ap = argparse.ArgumentParser(description="Phase 1 de-risk harness")
    ap.add_argument("--dataset", default=None,
                    help="run a single dataset by yml name (e.g. insects_abrupt_imbalanced); "
                         "default = run every dataset in deploy/datasets.yml")
    ap.add_argument("--list", action="store_true",
                    help="list datasets the yml exposes and exit")
    ap.add_argument("--k-grid", default="1,2,3",
                    help="comma-separated initial k values to scan; 1 and D are always added")
    args = ap.parse_args()

    all_specs = all_dataset_specs()
    if args.list:
        for s in all_specs:
            present = "ok" if s.csv_path.exists() else "missing"
            print(f"  {s.name:36s}  D={s.dimensions:<3d}  drift_starts={s.drift_starts}  "
                  f"csv={s.csv_path.relative_to(PROJECT_ROOT)} [{present}]")
        return

    if args.dataset:
        specs = [s for s in all_specs if s.name == args.dataset]
        if not specs:
            raise SystemExit(f"unknown --dataset {args.dataset!r}; try --list")
    else:
        specs = all_specs

    k_grid_base = [int(x) for x in args.k_grid.split(",") if x.strip()]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_rows: List[Dict[str, str]] = []
    ksweep_rows: List[Dict[str, str]] = []
    for spec in specs:
        srow, krows = run_dataset(spec, list(k_grid_base))
        summary_rows.append(srow)
        ksweep_rows.extend(krows)

    # When --dataset is used, merge into existing summary/ksweep so a subsequent
    # INSECTS run on leejean's box augments the synth run sitting in the repo.
    summary_path = OUT_DIR / "derisk_summary.csv"
    ksweep_path = OUT_DIR / "derisk_ksweep.csv"
    if args.dataset:
        existing_summary = _read_existing_rows(summary_path)
        existing_ksweep = _read_existing_rows(ksweep_path)
        existing_summary = [r for r in existing_summary if r.get("dataset") != args.dataset]
        existing_ksweep = [r for r in existing_ksweep if r.get("dataset") != args.dataset]
        summary_rows = existing_summary + summary_rows
        ksweep_rows = existing_ksweep + ksweep_rows

    with open(summary_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "dataset", "GT_A_onset", "best_k", "agg_onset",
            "latency", "FP", "FN", "features_fired", "verdict",
        ])
        w.writeheader()
        for row in summary_rows:
            w.writerow(row)
    print(f"\n  -> {summary_path.relative_to(PROJECT_ROOT)}")

    with open(ksweep_path, "w", newline="") as f:
        if ksweep_rows:
            w = csv.DictWriter(f, fieldnames=list(ksweep_rows[0].keys()))
            w.writeheader()
            for row in ksweep_rows:
                w.writerow(row)
    print(f"  -> {ksweep_path.relative_to(PROJECT_ROOT)}")


def _read_existing_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


if __name__ == "__main__":
    main()
