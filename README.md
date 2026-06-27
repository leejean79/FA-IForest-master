# FA-IForest: A Distributed, Drift-Adaptive Isolation Forest for Stream Processing on Apache Flink

Reference implementation for the paper *“FA-IForest: A Distributed, Drift-Adaptive
Isolation Forest for Stream Processing on Apache Flink.”* This repository contains
the full system, the analysis scripts, and the experiment-orchestration tooling
used to produce every table and figure in the paper.

- **Paper branch / default branch:** `feature/per-feature-hddm_w`
- **Design (Direction A):** per-feature drift detection + **cross-feature
  aggregation** driving online retraining. (An earlier *voting-protocol* prototype,
  v3.4, has been superseded and removed; see [§10](#10-relation-to-earlier-versions).)
- **Language / build:** Java 8, Maven; **Apache Flink 1.13.6**, **Apache Kafka 2.6.3**.

---

## 1. What this system does

FA-IForest performs unsupervised anomaly detection on a high-volume data stream
while adapting online to concept drift, on a genuinely event-at-a-time engine
(Flink) rather than a micro-batch one. Two ideas distinguish it:

1. **Per-feature detection + cross-feature aggregation.** Drift is detected
   independently per feature and a retraining round is *committed* only when at
   least `aggK` distinct features confirm drift within a window. Because the unit
   of detection is the **feature** (orthogonal to the **record** sharding used for
   scoring), the drift response is **invariant to the scoring parallelism** — this
   dissolves the signal *dilution* that degrades naive row-parallel detection.
2. **Event-at-a-time latency.** Each record is processed in tens of milliseconds.
   The single-partition source needed to preserve arrival order caps end-to-end
   *throughput*, but not *latency* — a property micro-batch architectures do not
   provide.

---

## 2. Architecture

Two decoupled Flink jobs communicate through Kafka topics.

```
 file (.csv)                          ┌──────────────────────── LocalProcessor Job (parallelism = N) ───────────────────────┐
     │  FileToKafkaProducer           │                                                                                       │
     ▼  (rate-limited)                │  SCORING PLANE  (row-parallel, N)                                                     │
 [source-topic] ────────────────────►│   keyBy(uniform key) → LocalProcessorFunction (3-phase state machine) → [output-scores]│
                                      │                         │ Phase B cold-start train iTrees ─────────────► [tree-topic] │
                                      │                         │ Phase A backlog scoring                                     │
                                      │                         │ Phase C online scoring (+ COOLDOWN retrain on COMMIT)       │
                                      │                                                                                       │
                                      │  DETECTION PLANE  (column-parallel, P_d = detectionParallelism)                       │
                                      │   flatMap(FeatureSplitFlatMap) → keyBy(featureId)                                     │
                                      │     → per-feature detector (--detector iks | hddm_w) ───────────► [feature-drift-topic]│
                                      └───────────────────────────────────────────────────────────────────────────────────────┘
                                                          ▲ model-topic (broadcast forest)        │ feature-drift-topic
                                                          │                                        ▼
                                      ┌──────────────────── CoordinatorJob (parallelism = 1) ──────────────────────────────────┐
                                      │  Pipeline 1: [tree-topic] → CoordinatorFunction → assemble global forest → [model-topic] │
                                      │  Pipeline 2: [feature-drift-topic] → DriftAggregatorFunction(aggK, aggWin, refractory)   │
                                      │              → COMMITTED retraining round → [drift-round-topic]                          │
                                      └─────────────────────────────────────────────────────────────────────────────────────────┘
```

**Topics:** `source-topic`, `tree-topic`, `model-topic` (broadcast forest),
`feature-drift-topic` (per-feature onsets), `drift-round-topic` (committed rounds),
`output-scores`.

**Two parallelism knobs (paper §3.3):** `N` (scoring parallelism) and `P_d`
(`detectionParallelism`). Their independence is what makes the drift response
parallelism-invariant.

**Detection plane (paper §3.4, §4).** `FeatureSplitFlatMap` explodes each record
into per-feature values; `keyBy(featureId)` routes each feature to one detector
instance (`PerFeatureIKSFunction` or `PerFeatureHDDMFunction`). The IKS detector
uses an incremental Kolmogorov–Smirnov test with a peak-KS confirmation gate;
HDDM\_W uses an EWMA Hoeffding bound. The aggregator (`DriftAggregatorFunction`,
paper §4.3) commits a round when `≥ aggK` distinct features confirm onset inside a
sliding window `aggWin`, with a `refractory` debounce.

---

## 3. Repository structure

```
FA-IForest/
├── pom.xml
├── EXPERIMENTS_RUNBOOK_section7_rewrite.md   # AUTHORITATIVE experiment orchestration (use this)
├── EXPERIMENTS_RUNBOOK.md                     # earlier runbook (superseded by the above)
├── src/main/java/com/leejean/
│   ├── main/        LocalProcessor.java, CoordinatorJob.java          # job entry points
│   ├── flink/       LocalProcessorFunction, CoordinatorFunction,
│   │                DriftAggregatorFunction, FeatureSplitFlatMap,
│   │                PerFeatureIKSFunction, PerFeatureHDDMFunction, ...
│   ├── drift/       IKS.java, IKSConfig.java, Treap.java,             # incremental KS (IKSSW semantics)
│   │                HDDM_W.java, HDDM_A*, DriftDetector, DriftStatus
│   ├── beans/       DataPoint, FeatureValue, FeatureDrift,
│   │                ForestMessage, ITreeMessage, DriftRoundMessage, ScoreResult
│   ├── tree/        Forest, ITree, ITreeBuilder, ITreeNode, RingBuffer
│   ├── source/      FileToKafkaProducer.java
│   └── common_utils/ ParallelismKeys, CsvToDataPointFunction, KafkaHelper, ...
├── src/test/java/com/leejean/                # JUnit 5 suite (operators, detectors, beans)
├── analysis/        analyze.py, count_committed.py,                   # paper metrics
│                    exp2_distributed_accuracy.py, exp2_false_alarm.py,
│                    exp3_throughput_batch.py, exp3_throughput_prom.py,
│                    exp3_scalability_from_scores.py,
│                    gen_synth_drift*.py, transform_insects.py, shuffle_csv.py,
│                    derisk_proxy.py            # OFFLINE proxy only — not used as paper evidence
├── deploy/
│   ├── datasets.yml, experiment-configs.yml, .env / env.example
│   ├── compose/     docker-compose.{master,worker}.yml, prometheus.yml.template
│   ├── docker/      Dockerfile.flink
│   └── scripts/     0-prepare-local … 9-teardown, run-batch.sh, run-exp1-acrossP.sh,
│                    run-exp3-batch.sh, cfg_query.py, clean-topics.sh, pull-results.sh, ...
└── docs/            EXP{2,3,4}_*_skeleton.md, SENSITIVITY_appendix_*.md,
                     PROJECT_SUMMARY_directionA.md, HANDOVER_*.md (historical)
```

---

## 4. Prerequisites

| Component | Version | Purpose |
|---|---|---|
| JDK | 1.8 | build/run |
| Maven | 3.x | build |
| Apache Flink | 1.13.6 (Scala 2.12) | stream engine |
| Apache Kafka | 2.6.3 | transport |
| Python | 3.9+ | analysis scripts (`numpy`, `pandas`, `scikit-learn`, `pyyaml`) |
| Docker + Compose | recent | 3-node cluster (optional, for full reproduction) |
| Prometheus | recent | throughput metrics for EXP3 |

ML libraries (via Maven): Smile 2.6.0, JSAT 0.0.9. The incremental KS detector
(`IKS.java` + `Treap.java`) is a self-contained implementation.

---

## 5. Build and test

```bash
mvn clean package            # shaded fat jar for cluster submission
mvn test                     # full JUnit 5 suite
mvn test -Dtest=DriftAggregatorFunctionTest   # a single class
```

---

## 6. Quick start (single node)

Start Kafka/Flink locally, then:

```bash
# 1) feed data
mvn exec:java -Dexec.mainClass="com.leejean.source.FileToKafkaProducer" \
  -Dexec.args="--broker localhost:9092 --topic source-topic --file data/insects_abrupt.csv --rate 2000"

# 2) coordinator (forest assembly + cross-feature drift aggregation)
mvn exec:java -Dexec.mainClass="com.leejean.main.CoordinatorJob" \
  -Dexec.args="--broker localhost:9092 --treeTopic tree-topic --modelTopic model-topic \
               --featureDriftTopic feature-drift-topic --driftRoundTopic drift-round-topic \
               --parallelism 4 --totalTrees 100 --aggK 2 --aggWin 2000 --refractory 5000"

# 3) local processor (scoring plane + per-feature detection plane)
mvn exec:java -Dexec.mainClass="com.leejean.main.LocalProcessor" \
  -Dexec.args="--broker localhost:9092 --topic source-topic --hasHeader true --hasId true --hasLabel true \
               --modelTopic model-topic --scoreTopic output-scores --treeTopic tree-topic \
               --featureDriftTopic feature-drift-topic --driftRoundTopic drift-round-topic \
               --detector iks --detectionParallelism 1 --parallelism 4 --totalTrees 100"
```

---

## 7. Key configuration knobs (paper cross-reference)

| Knob (CLI `--key`) | Default | Where in paper | Notes |
|---|---|---|---|
| `parallelism` (N) | 4 | §3.3, §6.1, §6.4 | scoring parallelism |
| `detectionParallelism` (P_d) | 1 | §3.3 | detection-plane parallelism |
| `detector` | `iks` | §3.4, §6.1, §6.3 | `iks` (primary, IKSSW) or `hddm_w` |
| `aggK` | 2 | §4.3 | distinct features required to commit a round |
| `aggWin` | 2000 | §4.3 | aggregation window |
| `refractory` | 5000 | §4.3 | debounce between committed rounds |
| `iksWindowSize` | 2000 | App. A (Table A1) | IKS window; robust |
| `ringBufferSize` | 1000 | App. A (Table A1) | training pool; robust |
| `cooldownSamples` | 2000 | App. A (Table A1) | **has a lower bound (≥ 2000)** |
| `confirmWin`, `ksConfirm` | — | §4.2 | peak-KS confirmation gate |
| `hddmLambda`, `hddmScaleMode`, `hddmWarmup` | — | §6.3, App. A (Table A2) | HDDM\_W; sensitive (needs tuning) |

---

## 8. Reproducing the paper

The authoritative orchestration is **`EXPERIMENTS_RUNBOOK_section7_rewrite.md`**.
Experiments are expanded from `deploy/experiment-configs.yml` and run via
`run-batch.sh --plan <name>` (master-side, autonomous tmux, resumable). Datasets are
declared in `deploy/datasets.yml`.

**Cluster bring-up (3-node):**

```bash
deploy/scripts/0-prepare-local.sh        # build + stage artifacts
deploy/scripts/1-sync-to-nodes.sh        # scp to master/worker1/worker2
deploy/scripts/2-up-all.sh               # docker compose up (Flink + Kafka + Prometheus)
deploy/scripts/3-create-topics.sh
deploy/scripts/5-load-data.sh
# ... run plans ...
deploy/scripts/9-teardown.sh
```

**Experiment → plan → analysis → paper artifact:**

| Paper | Plan(s) (`run-batch.sh --plan`) | Analysis | Output |
|---|---|---|---|
| λ sweep (prereq for EXP1 HDDM\_W arm) | `lambda_sweep` | `analyze.py` | per-dataset λ\* |
| **EXP1** detector selection + parallelism invariance | `exp1_iks`, `exp1_hddm_w` (and `run-exp1-acrossP.sh`) | `analyze.py`, `count_committed.py` | **Table 2, Table 2b** |
| **EXP2** distributed accuracy + false-alarm | `exp2` | `exp2_distributed_accuracy.py`, `exp2_false_alarm.py` | **Table 3, Fig 3** |
| **EXP3** throughput + latency scalability | `exp3` (+ `run-exp3-batch.sh`; set `.env SOURCE_PARTITIONS` + `clean-topics.sh` between forks) | `exp3_throughput_batch.py` / `exp3_throughput_prom.py`, `exp3_scalability_from_scores.py` | **Fig 4, Fig 5** |
| **EXP4** detector morphology (no new runs) | — (cross-analysis of EXP1 arms) | `analyze.py` | **Table 4** |
| **Appendix A** sensitivity | `sensitivity_iks`, `sensitivity_hddm_w` | `analyze.py` | **Table A1, Table A2** |

Notes for reviewers:
- `overall_auc` is the primary, less-confounded metric throughout. `count_committed.py`
  computes the committed-round count `n_c` used for the parallelism-invariance result
  (Table 2b): `n_c` is invariant in `N`, whereas the AUC rises slightly with `N` as a
  **retraining-frequency side-effect** — these are reported as distinct quantities.
- `analysis/derisk_proxy.py` is an **offline** proxy used only for de-risking; its
  outputs are **not** used as evidence in the paper (see Limitations §8).
- Throughput (EXP3) is read from Prometheus (`numRecordsIn` rate); per-record latency
  is reported on `donors` only (paper §8).

---

## 9. Datasets

Stationary benchmarks (`forestcover`, `donors`, `http`) and drifting streams
(`synth_abrupt/gradual/incremental/reoccurring`, `insects_abrupt_imbalanced`,
`insects_gradual_imbalanced`). Helpers: `gen_synth_drift*.py` (synthetic streams),
`transform_insects.py` (INSECTS preprocessing), `shuffle_csv.py` (EXP2 shuffles).
Paths and per-dataset parsing flags are declared in `deploy/datasets.yml`.

---

## 10. Relation to earlier versions

Up to **v3.4** the system used a *federated drift-voting* protocol (subtasks voting
on a global drift round). That design has been **superseded and removed**: drift
detection now runs on the per-feature detection plane and rounds are committed by
**cross-feature aggregation** (`DriftAggregatorFunction`). Historical voting-era
documents under `docs/` (e.g. `HANDOVER_v3.4*.md`, `EXP1_voting_findings.md`,
`findings.md`) are retained for provenance only and **do not describe the current
system or the paper**. The current design corresponds to
`docs/PROJECT_SUMMARY_directionA.md`.

---

## 11. Citation

```bibtex
@article{faiforest,
  title   = {FA-IForest: A Distributed, Drift-Adaptive Isolation Forest for Stream Processing on Apache Flink},
  author  = {<authors>},
  journal = {Journal of Big Data},
  year    = {<year>},
  note    = {To appear}
}
```
