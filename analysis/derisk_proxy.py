#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1 de-risk harness — direction 2(a).
Reference: HANDOVER_direction2a_phase1_derisk.md

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

Notes:
- KS inside the harness is value-based two-sample (np.searchsorted), which the
  deployed composite-key IKSSW (Java) upper-bounds by <= 1/W. For W=2000 that
  bias is ~5e-4, well under the fire threshold ca*sqrt(2/W) ~ 5.9e-2 at p=0.001,
  so fire decisions are equivalent. The faithful port lives in analysis/ikssw.py
  for byte-level equivalence checks on smaller slices.
- INSECTS datasets are auto-skipped if their CSV is missing; the summary records
  the skip so the Phase-1 verdict against real data can be reproduced when the
  data is provided.
"""
from __future__ import annotations

import csv
import json
import math
import os
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

import numpy as np

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


# ---------------- dataset registry ----------------

@dataclass
class DatasetSpec:
    name: str
    csv_path: Path
    drift_starts: List[int]   # for GT-A: drift_starts[0] = pre_end; later events used by overlay only
    label_col: Optional[str] = "label"   # column NAME if hasHeader; else None and use last
    drop_id: bool = True
    delta: float = GT_DELTA


def _load_synth_drift_starts(name: str) -> List[int]:
    spec_path = DATA_DIR / "synth" / f"{name}.driftspec.json"
    with open(spec_path) as f:
        spec = json.load(f)
    return [int(e["start_line"]) for e in spec.get("drift_events", [])]


def _load_insects_drift_events_from_yml(name: str) -> List[int]:
    """
    Parse `drift_events: [a, b, ...]` line from datasets.yml without pulling in
    a YAML dep — datasets.yml writes drift_events on a single line as a flow seq.
    """
    if not DATASETS_YML.exists():
        return []
    in_section = False
    with open(DATASETS_YML) as f:
        for raw in f:
            line = raw.rstrip()
            stripped = line.strip()
            # naive but sufficient: enter the named block, scan its body lines
            if stripped == f"{name}:":
                in_section = True
                continue
            if in_section:
                if stripped == "" or (line and not line.startswith(" ") and not line.startswith("\t")):
                    in_section = False
                    continue
                if stripped.startswith("drift_events:"):
                    rhs = stripped.split(":", 1)[1].strip()
                    # expect: [a, b, c]   (possibly with trailing comment)
                    if "#" in rhs:
                        rhs = rhs.split("#", 1)[0].strip()
                    rhs = rhs.strip("[]")
                    return [int(x.strip()) for x in rhs.split(",") if x.strip()]
    return []


def all_dataset_specs() -> List[DatasetSpec]:
    specs: List[DatasetSpec] = []
    for name in ("synth_abrupt", "synth_gradual", "synth_incremental", "synth_reoccurring"):
        csv_path = DATA_DIR / "synth" / f"{name}.csv"
        starts = _load_synth_drift_starts(name)
        specs.append(DatasetSpec(name=name, csv_path=csv_path, drift_starts=starts))
    # INSECTS — wider delta because GT-A signal is weaker (ceiling ~0.75, see HANDOVER §7)
    insects_name = "insects_abrupt_imbalanced"
    insects_csv = DATA_DIR / "insects" / "INSECTS_abrupt_imbalanced_transformed.csv"
    insects_starts = _load_insects_drift_events_from_yml(insects_name)
    specs.append(DatasetSpec(name=insects_name, csv_path=insects_csv,
                             drift_starts=insects_starts, delta=0.10))
    return specs


# ---------------- CSV loader ----------------

def load_csv(csv_path: Path, label_col: str = "label",
             drop_id: bool = True) -> Tuple[np.ndarray, np.ndarray]:
    """
    Returns X (n, D) float and y (n,) int. Mirrors the oracle_ceiling_test.py
    column logic (skip id, label by name, the rest are features).
    """
    rows: List[List[str]] = []
    header: List[str] = []
    with open(csv_path) as f:
        r = csv.reader(f)
        header = next(r)
        for line in r:
            rows.append(line)
    li = header.index(label_col)
    # skip id (first column) and label
    feat_idx = [i for i in range(len(header))
                if i != li and not (drop_id and i == 0 and header[i].lower() in ("id", "seq"))]
    X = np.array([[float(row[i]) for i in feat_idx] for row in rows], dtype=float)
    y = np.array([int(float(row[li])) for row in rows], dtype=int)
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
    X, y = load_csv(spec.csv_path)
    n, D = X.shape
    print(f"  n={n}, D={D}, anomaly_rate={y.mean():.4f}, drift_starts={spec.drift_starts}")
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
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # k grid spans 1, mid, ceil(D/2), D ; per-dataset will fold in 1 and D
    k_grid_base = [1, 2, 3]
    summary_rows: List[Dict[str, str]] = []
    ksweep_rows: List[Dict[str, str]] = []
    for spec in all_dataset_specs():
        srow, krows = run_dataset(spec, list(k_grid_base))
        summary_rows.append(srow)
        ksweep_rows.extend(krows)

    summary_path = OUT_DIR / "derisk_summary.csv"
    with open(summary_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "dataset", "GT_A_onset", "best_k", "agg_onset",
            "latency", "FP", "FN", "features_fired", "verdict",
        ])
        w.writeheader()
        for row in summary_rows:
            w.writerow(row)
    print(f"\n  -> {summary_path.relative_to(PROJECT_ROOT)}")

    ksweep_path = OUT_DIR / "derisk_ksweep.csv"
    with open(ksweep_path, "w", newline="") as f:
        if ksweep_rows:
            w = csv.DictWriter(f, fieldnames=list(ksweep_rows[0].keys()))
            w.writeheader()
            for row in ksweep_rows:
                w.writerow(row)
    print(f"  -> {ksweep_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
