import argparse, csv, random
parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--seed", type=int, required=True)
args = parser.parse_args()

# 读全部行（保留 header）
with open(args.input) as f:
    reader = csv.reader(f)
    header = next(reader)
    rows = list(reader)

# 用 seed shuffle
random.Random(args.seed).shuffle(rows)

# 写回
with open(args.output, "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)