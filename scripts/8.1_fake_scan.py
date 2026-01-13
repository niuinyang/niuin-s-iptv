#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
8.1_fake_scan.py

读取 output/hash/merge/ 目录下所有 *-hash-merge.json 文件，
基于 phash 等哈希值判定每个 URL 的画面状态（静态、冻结、无效、活跃），
统计多次检测结果，给出综合判定标签，
并将结果追加到 deep_total_ok.csv，
输出 fake-ok.csv（通过）和 fake-not.csv（未通过）两个文件。
"""

import csv
import json
import os
import glob
from collections import defaultdict
import imagehash

# 参数区
HASH_DIR = "output/hash/merge"
INPUT_CSV = "output/middle/deep/deep_total_ok.csv"
OUTPUT_DIR = "output/middle/fake-scan"

# 汉明距离阈值
PHASH_STATIC_THRESHOLD = 5

# 以下阈值为“次数阈值”，会基于采样文件数量动态计算
STATIC_RATIO_THRESHOLD = 0.5   # 静态判定比例阈值（超过50%次采样为静态即判假）
FROZEN_RATIO_THRESHOLD = 0.33  # 冻结判定比例阈值（超过33%次采样为冻结即判假）
INVALID_RATIO_THRESHOLD = 0.5  # 无效判定比例阈值（超过50%次采样为无效即判假）

# 工具函数：计算两个哈希的汉明距离
def hamming(h1, h2):
    return imagehash.hex_to_hash(h1) - imagehash.hex_to_hash(h2)

# 判定是否黑屏或无效画面（根据 ahash 和 dhash 简单判定）
def is_black_or_invalid(ahash, dhash):
    if not ahash or not dhash or len(ahash) < 3 or len(dhash) < 3:
        return True
    # 只要连续两帧间哈明距离大于1则认为不是黑屏/无效
    for i in range(2):
        if hamming(ahash[i], ahash[i + 1]) > 1:
            return False
    return True

# 核心判定逻辑，输入3个时间点的phash/ahash/dhash数组，输出状态类别
def classify_sample(phash, ahash, dhash):
    if len(phash) < 3:
        return "invalid"

    if is_black_or_invalid(ahash, dhash):
        return "invalid"

    d1 = hamming(phash[0], phash[1])
    d2 = hamming(phash[1], phash[2])

    if d1 <= PHASH_STATIC_THRESHOLD and d2 <= PHASH_STATIC_THRESHOLD:
        return "static"

    if d1 > PHASH_STATIC_THRESHOLD and d2 <= PHASH_STATIC_THRESHOLD:
        return "frozen"

    return "active"

# 读取所有hash文件，统计每个URL在各状态的出现次数
def build_fake_scan_map():
    files = sorted(glob.glob(os.path.join(HASH_DIR, "*-hash-merge.json")))
    if not files:
        raise RuntimeError(f"❌ 未找到任何 hash-merge.json 文件，路径：{HASH_DIR}")

    total_files = len(files)

    stats = defaultdict(lambda: defaultdict(int))

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        for url, h in data.items():
            cls = classify_sample(
                h.get("phash", []),
                h.get("ahash", []),
                h.get("dhash", [])
            )
            stats[url][cls] += 1

    # 基于阈值做最终判定
    result = {}

    for url, c in stats.items():
        static_ratio = c["static"] / total_files
        frozen_ratio = c["frozen"] / total_files
        invalid_ratio = c["invalid"] / total_files

        if static_ratio >= STATIC_RATIO_THRESHOLD:
            result[url] = "fake_static"
        elif frozen_ratio >= FROZEN_RATIO_THRESHOLD:
            result[url] = "fake_frozen"
        elif invalid_ratio >= INVALID_RATIO_THRESHOLD:
            result[url] = "invalid"
        else:
            result[url] = "pass"

    return result

def main():
    fake_map = build_fake_scan_map()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames + ["fake_scan"]

        ok_rows = []
        not_rows = []

        for row in reader:
            url = row.get("地址", "").strip()
            scan = fake_map.get(url, "pass")
            row["fake_scan"] = scan

            if scan == "pass":
                ok_rows.append(row)
            else:
                not_rows.append(row)

    with open(f"{OUTPUT_DIR}/fake-ok.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ok_rows)

    with open(f"{OUTPUT_DIR}/fake-not.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(not_rows)

    print(f"✅ 8.1_fake_scan 完成，总采样文件数: {len(glob.glob(os.path.join(HASH_DIR, '*-hash-merge.json')))}")
    print(f"通过数量: {len(ok_rows)}  未通过数量: {len(not_rows)}")

if __name__ == "__main__":
    main()
