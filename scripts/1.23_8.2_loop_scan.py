#!/usr/bin/env python3
# coding: utf-8

import os
import glob
import json
import pandas as pd
from collections import Counter
from pathlib import Path

# ========== 配置区 ==========
INPUT_CSV = "output/middle/fake/fake_scan.csv"
HASH_DIR = "output/hash/merge"
OUTPUT_CSV_LOOP = "output/middle/loop/loop_scan.csv"
OUTPUT_CSV_OK = "output/middle/loop/loop_scan_ok.csv"
OUTPUT_CSV_NOT = "output/middle/loop/loop_scan_not.csv"

# 权重定义（方便后续调整）
WEIGHTS = {
    "phash_concentration": 0.35,
    "single_detection_consistency": 0.18,
    "start_frame_stability": 0.18,
    "phash_diversity": 0.18,
    "cross_detection_pattern": 0.18,
    "time_span": 0.0  # 目前暂时无用，后续可调整
}

# 判定阈值
LOOP_THRESHOLD = 0.6

# 汉明距离阈值
HAMMING_THRESHOLD = 5
MAX_TIME_SPAN_DAYS = 30
# ==========================


def hamming_distance(hash1, hash2):
    try:
        if not isinstance(hash1, str) or not isinstance(hash2, str):
            return 64
        b1 = bin(int(hash1, 16))[2:].zfill(64)
        b2 = bin(int(hash2, 16))[2:].zfill(64)
        return sum(c1 != c2 for c1, c2 in zip(b1, b2))
    except Exception:
        return 64


def merge_similar_hashes(phash_list):
    merged = []
    for h in phash_list:
        if not any(hamming_distance(h, mh) <= HAMMING_THRESHOLD for mh in merged):
            merged.append(h)
    return merged


def load_hash_data(hash_dir):
    hash_files = glob.glob(os.path.join(hash_dir, "*-hash-merge.json"))
    data = {}
    for f in hash_files:
        with open(f, "r", encoding="utf-8") as fin:
            d = json.load(fin)
        for addr, v in d.items():
            if addr not in data:
                data[addr] = {
                    "phash_all": [],
                    "error_stats": {
                        "fail_count": 0,
                        "timeout_count": 0,
                        "network_error_count": 0,
                        "other_error_count": 0
                    }
                }
            data[addr]["phash_all"].extend(v.get("phash", []))
            err = v.get("error", {})
            for k in data[addr]["error_stats"]:
                data[addr]["error_stats"][k] += err.get(k, 0)
    return data


def phash_concentration_score(phash_list):
    if not phash_list:
        return 0.0
    c = Counter(phash_list)
    return c.most_common(1)[0][1] / len(phash_list)


def single_detection_consistency_score(hash_sets):
    if not hash_sets:
        return 0.0

    def score_one(hs):
        pairs = [(0, 1), (1, 2), (0, 2)]
        return sum(hamming_distance(hs[i], hs[j]) <= HAMMING_THRESHOLD for i, j in pairs) / 3

    scores = [score_one(hs) for hs in hash_sets if len(hs) == 3]
    return sum(scores) / len(scores) if scores else 0.0


def start_frame_stability_score(hash_sets):
    if not hash_sets:
        return 0.0
    first = [hs[0] for hs in hash_sets if hs]
    c = Counter(first)
    return c.most_common(1)[0][1] / len(first)


def phash_diversity_score(phash_list):
    if not phash_list:
        return 0.0
    merged = merge_similar_hashes(list(set(phash_list)))
    return max(0.0, 1 - len(merged) / len(phash_list))


def cross_detection_pattern_score(hash_sets):
    if not hash_sets:
        return 0.0
    first = [hs[0] for hs in hash_sets if hs]
    c = Counter(first)
    concentration = c.most_common(1)[0][1] / len(first)

    seq = first
    n = len(seq)
    time_offset = 0.0
    for p in range(1, n // 2 + 1):
        if all(seq[i] == seq[i + p] for i in range(n - p)):
            time_offset = 1.0
            break
    return max(concentration, time_offset)


def confidence_score(error_stats, total):
    if total == 0:
        return 0.0
    err = sum(error_stats.values())
    return max(0.0, 1 - err / total)


def main():
    Path(os.path.dirname(OUTPUT_CSV_LOOP)).mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV)
    hash_data = load_hash_data(HASH_DIR)

    results = []

    for _, row in df.iterrows():
        addr = row["地址"]
        info = hash_data.get(addr)

        if not info:
            results.append({
                **row.to_dict(),
                "轮播级": 0,
                "检测置信度": 0.0,
                "phash有效率": 0.0,
                "phash置信度": 0.0,
                "是否轮播": False
            })
            continue

        raw = info["phash_all"]
        phash = [h for h in raw if isinstance(h, str) and h]
        phash_valid_ratio = len(phash) / max(len(raw), 1)

        hash_sets = [phash[i:i + 3] for i in range(0, len(phash), 3) if len(phash[i:i + 3]) == 3]

        total_score = (
            phash_concentration_score(phash) * WEIGHTS["phash_concentration"] +
            single_detection_consistency_score(hash_sets) * WEIGHTS["single_detection_consistency"] +
            start_frame_stability_score(hash_sets) * WEIGHTS["start_frame_stability"] +
            phash_diversity_score(phash) * WEIGHTS["phash_diversity"] +
            cross_detection_pattern_score(hash_sets) * WEIGHTS["cross_detection_pattern"]
        )

        loop_level = int(round(total_score * 10))
        is_loop = total_score >= LOOP_THRESHOLD
        conf = confidence_score(info["error_stats"], len(phash))
        phash_conf = conf * phash_valid_ratio

        results.append({
            **row.to_dict(),
            "轮播级": loop_level,
            "检测置信度": round(conf, 4),
            "phash有效率": round(phash_valid_ratio, 4),
            "phash置信度": round(phash_conf, 4),
            "是否轮播": is_loop
        })

    df_out = pd.DataFrame(results)
    df_out.to_csv(OUTPUT_CSV_LOOP, index=False, encoding="utf-8-sig")

    df_ok = df_out[df_out["是否轮播"] == False]
    df_not = df_out[df_out["是否轮播"] == True]

    df_ok.to_csv(OUTPUT_CSV_OK, index=False, encoding="utf-8-sig")
    df_not.to_csv(OUTPUT_CSV_NOT, index=False, encoding="utf-8-sig")

    print("✔ 轮播扫描完成")
    print(f"✔ 非轮播数量: {len(df_ok)}")
    print(f"✔ 轮播数量: {len(df_not)}")
    print(f"✔ 总计: {len(df_out)}")
    print(f"📄 全部结果: {OUTPUT_CSV_LOOP}")
    print(f"📄 非轮播: {OUTPUT_CSV_OK}")
    print(f"📄 轮播源: {OUTPUT_CSV_NOT}")


if __name__ == "__main__":
    main()
