#!/usr/bin/env python3
"""
离线判定:v2 的 0.79 是【森林构建问题(可修)】还是【数据天花板(该停)】。
关键对比:同样 1000 个零异常点,但【随机无偏抽样】 vs pipeline 的【z-score 门选出】(=arm B 的 0.79)。

用法:
  python3 oracle_ceiling_test.py <dataset.csv> --drift-point 50000 [--label-col label]
  # 假设行序 = seq,前 drift_point 行为漂移前;label: 1=异常 0=正常
"""
import sys, argparse
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import roc_auc_score

ap = argparse.ArgumentParser()
ap.add_argument("dataset")
ap.add_argument("--drift-point", type=int, default=50000)
ap.add_argument("--label-col", default="label")
ap.add_argument("--seed", type=int, default=0)
a = ap.parse_args()

# ---- 读数据(CSV,带表头;label 列名按需改)----
import csv
rows=[]; header=None
with open(a.dataset) as f:
    r = csv.reader(f)
    header = next(r)
    for line in r:
        rows.append(line)
li = header.index(a.label_col)
feat_idx = [i for i in range(len(header)) if i != li]
X = np.array([[float(row[i]) for i in feat_idx] for row in rows], float)
y = np.array([int(float(row[li])) for row in rows], int)
print("总样本 %d,特征维 %d,异常率 %.1f%%" % (len(y), X.shape[1], 100*y.mean()))

dp = a.drift_point
pre_X, pre_y = X[:dp], y[:dp]
post_X, post_y = X[dp:], y[dp:]
rng = np.random.RandomState(a.seed)

def fit_score_auc(train_X, test_X, test_y, n_est=100, max_samples=256):
    clf = IsolationForest(n_estimators=n_est, max_samples=min(max_samples, len(train_X)),
                          random_state=a.seed, contamination='auto')
    clf.fit(train_X)
    s = -clf.score_samples(test_X)   # 越大越异常
    return roc_auc_score(test_y, s)

def sample_normals(Xs, ys, k):
    idx = np.where(ys == 0)[0]
    if len(idx) > k: idx = rng.choice(idx, k, replace=False)
    return Xs[idx]

print("\n--- 方法学 sanity:漂移前正常点训练 → 评漂移前(应 ~0.99,验证 oracle 流程)---")
print("  pre 干净 4000:  AUC = %.4f" % fit_score_auc(sample_normals(pre_X, pre_y, 4000), pre_X, pre_y))

print("\n--- 决定性:漂移后 AUC 上限 ---")
print("  [A] post 干净·随机·无偏 1000:  AUC = %.4f   ← 与 arm B(1000零异常但门选)0.79 直接对比" %
      fit_score_auc(sample_normals(post_X, post_y, 1000), post_X, post_y))
print("  [B] post 干净·随机·无偏 4000:  AUC = %.4f   ← 充足样本的上限" %
      fit_score_auc(sample_normals(post_X, post_y, 4000), post_X, post_y))
n_post = (post_y==0).sum()
# arm C 预览:不设门、连异常一起进池(~base rate),1000 点
allidx = rng.choice(len(post_X), min(1000,len(post_X)), replace=False)
print("  [C] post 未过滤·随机 1000(含~base rate 异常):AUC = %.4f   ← arm C(去掉 z-score 门)预览" %
      fit_score_auc(post_X[allidx], post_X, post_y))

print("""
判读:
 · [A] 回到 ~0.95+  → 同样 1000 个零异常点,随机无偏能行、门选不行
        ⇒ 坐实【z-score 门的选择偏置】是元凶(不是池大小、不是污染、不是数据天花板)
        ⇒ 修复 = 不要用失效旧森林的分去筛池
 · [A] 也停在 ~0.79 → 1000 个无偏干净点都救不回 → 是【池太小】或【数据天花板】
        看 [B]:[B] 高而 [A] 低 ⇒ 池太小(加大 ringBufferSize);[B] 也低 ⇒ 数据天花板,该停
 · [C] 和 [A] 接近 ⇒ 去掉门即可(异常少量无所谓,arm A/B 已证);[C] 明显低 ⇒ 还得防异常
""")
