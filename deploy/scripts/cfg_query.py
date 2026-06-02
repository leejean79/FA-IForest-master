#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cfg_query.py - 供 bash 脚本查询 datasets.yml / experiment-configs.yml 的字段

bash 原生不能解析 yaml, 用这个 helper 集中查询.
每个查询打印单个值到 stdout, bash 用 $(...) 捕获.

用法 / Usage:
  # 查数据集字段
  python3 cfg_query.py dataset http path
  python3 cfg_query.py dataset http n_samples
  python3 cfg_query.py dataset http hasHeader

  # 查 drift_events (逗号分隔)
  python3 cfg_query.py dataset insects_abrupt_imbalanced drift_events
  # → 51255,74381,100944,142291,152207

  # 查配置 (C1-C4)
  python3 cfg_query.py config C1 pauseMode
  python3 cfg_query.py config C1 warnTimeoutBehavior

  # 查 plan (展开实验矩阵, 每行一个实验)
  python3 cfg_query.py plan exp1
  # → 每行: dataset config algo parallelism

  # 列出所有数据集名
  python3 cfg_query.py list-datasets
"""
import sys
import yaml
from pathlib import Path

# yaml 文件位置 (相对本脚本: ../deploy/)
# cfg_query.py 在 deploy/scripts/, datasets.yml 在 deploy/
SCRIPT_DIR = Path(__file__).resolve().parent
DEPLOY_DIR = SCRIPT_DIR.parent          # scripts 的上级就是 deploy
DATASETS_YML = DEPLOY_DIR / "datasets.yml"
CONFIGS_YML = DEPLOY_DIR / "experiment-configs.yml"


def load(path):
    with open(path) as f:
        return yaml.safe_load(f)


def query_dataset(name, field):
    data = load(DATASETS_YML)
    ds = data["datasets"]
    if name not in ds:
        sys.stderr.write(f"ERROR: dataset '{name}' not in datasets.yml\n")
        sys.exit(1)
    cfg = ds[name]
    if field not in cfg:
        # drift_events 等可选字段不存在时返回空, 不报错
        if field in ("drift_events", "drift_spec"):
            return ""
        sys.stderr.write(f"ERROR: field '{field}' not in dataset '{name}'\n")
        sys.exit(1)
    val = cfg[field]
    # drift_events 是列表 → 逗号分隔
    if isinstance(val, list):
        return ",".join(str(x) for x in val)
    # bool → 小写 true/false (bash 比较 + Java 参数要小写)
    if isinstance(val, bool):
        return "true" if val else "false"
    return str(val)


def query_config(cid, field):
    data = load(CONFIGS_YML)
    configs = data["configs"]
    if cid not in configs:
        sys.stderr.write(f"ERROR: config '{cid}' not in experiment-configs.yml\n")
        sys.exit(1)
    cfg = configs[cid]
    if field not in cfg:
        sys.stderr.write(f"ERROR: field '{field}' not in config '{cid}'\n")
        sys.exit(1)
    return str(cfg[field])


def query_algorithm(aid, field):
    data = load(CONFIGS_YML)
    algos = data.get("algorithms", {})
    if aid not in algos:
        sys.stderr.write(f"ERROR: algorithm '{aid}' not in experiment-configs.yml\n")
        sys.exit(1)
    return str(algos[aid][field])


def expand_plan(plan_name):
    """展开一个 plan 成实验列表, 每行: dataset config algo parallelism"""
    data = load(CONFIGS_YML)
    plans = data["plans"]
    if plan_name not in plans:
        sys.stderr.write(f"ERROR: plan '{plan_name}' not in experiment-configs.yml\n")
        sys.exit(1)
    plan = plans[plan_name]

    if not plan.get("enabled", True):
        sys.stderr.write(f"WARN: plan '{plan_name}' is disabled\n")
        return []

    datasets = plan.get("datasets", [])
    configs = plan.get("configs", ["C1"])
    algorithms = plan.get("algorithms", ["_"])           # _ = 不指定算法 (用 default)
    parallelism_grid = plan.get("parallelism_grid", ["_"])  # _ = 用 .env 默认
    repeats = plan.get("repeats", 1)

    # sensitivity 类型的 plan 特殊处理 (单数据集, 扫参数网格)
    if "grids" in plan:
        return expand_sensitivity(plan)

    if "configurations" in plan:
        return expand_configurations(plan)

    lines = []
    for ds in datasets:
        for cfg in configs:
            for algo in algorithms:
                for par in parallelism_grid:
                    for r in range(1, repeats + 1):
                        lines.append(f"{ds} {cfg} {algo} {par} {r}")
    return lines


def expand_sensitivity(plan):
    """sensitivity plan: 单数据集单配置, 扫描每个参数的网格"""
    ds = plan["dataset"]
    cfg = plan["config"]
    repeats = plan.get("repeats", 1)
    grids = plan["grids"]
    lines = []
    # 每个参数独立扫描 (one-at-a-time), 其他参数用默认
    for param, values in grids.items():
        for v in values:
            for r in range(1, repeats + 1):
                # 格式: dataset config _ _ run param=value
                lines.append(f"{ds} {cfg} _ _ {r} {param}={v}")
    return lines

def expand_configurations(plan):
    """sensitivity plan (点列表版): 每个 configuration 是一组 (k=v, ...) 配对,
    所有参数一起传给一次实验; 与 OAT 的 expand_sensitivity 不同, 这里点之间独立。"""
    ds = plan["dataset"]
    cfg = plan["config"]
    repeats = plan.get("repeats", 1)
    confs = plan["configurations"]   # list of dicts
    lines = []
    for conf in confs:
        # 把 dict 拼成 "k1=v1;k2=v2", 多对参数用分号分隔
        extra = ";".join(f"{k}={v}" for k, v in conf.items())
        for r in range(1, repeats + 1):
            lines.append(f"{ds} {cfg} _ _ {r} {extra}")
    return lines


def main():
    if len(sys.argv) < 2:
        sys.stderr.write(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "dataset":
        print(query_dataset(sys.argv[2], sys.argv[3]))
    elif cmd == "config":
        print(query_config(sys.argv[2], sys.argv[3]))
    elif cmd == "algorithm":
        print(query_algorithm(sys.argv[2], sys.argv[3]))
    elif cmd == "plan":
        for line in expand_plan(sys.argv[2]):
            print(line)
    elif cmd == "list-datasets":
        data = load(DATASETS_YML)
        for name in data["datasets"]:
            print(name)
    else:
        sys.stderr.write(f"Unknown command: {cmd}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
