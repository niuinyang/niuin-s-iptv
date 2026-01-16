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
OUTPUT_CSV_LOOP = "output/middle/loop_scan.csv"
OUTPUT_CSV_OK = "output/middle/loop_scan_ok.csv"
OUTPUT_CSV_NOT = "output/middle/loop_scan_not.csv"

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

# 汉明距离阈值，用于phash合并（可调）
HAMMING_THRESHOLD = 5

# 最大时间跨度天数（暂时无用）
MAX_TIME_SPAN_DAYS = 30

# ==========================


def hamming_distance(hash1: str, hash2: str) -> int:
    """计算两个十六进制hash字符串的汉明距离"""
    b1 = bin(int(hash1, 16))[2:].zfill(64)
    b2 = bin(int(hash2, 16))[2:].zfill(64)
    return sum(c1 != c2 for c1, c2 in zip(b1, b2))


def merge_similar_hashes(phash_list, threshold=HAMMING_THRESHOLD):
    """合并相近phash，返回合并后列表"""
    merged = []
    for h in phash_list:
        found = False
        for mh in merged:
            if hamming_distance(h, mh) <= threshold:
                found = True
                break
        if not found:
            merged.append(h)
    return merged


def load_hash_data(hash_dir):
    """加载所有hash json数据，合并为一个dict: {address: {phash_all:[], ...}}"""
    hash_files = glob.glob(os.path.join(hash_dir, "*-hash-merge.json"))
    data = {}
    for f in hash_files:
        with open(f, "r", encoding="utf-8") as fin:
            d = json.load(fin)
        for addr, v in d.items():
            if addr not in data:
                data[addr] = {
                    "phash_all": [],
                    "ahash_all": [],
                    "dhash_all": [],
                    "whash_all": [],
                    "error_stats": {
                        "fail_count": 0,
                        "timeout_count": 0,
                        "network_error_count": 0,
                        "other_error_count": 0,
                        "final_error": None
                    }
                }
            data[addr]["phash_all"].extend(v.get("phash", []))
            data[addr]["ahash_all"].extend(v.get("ahash", []))
            data[addr]["dhash_all"].extend(v.get("dhash", []))
            data[addr]["whash_all"].extend(v.get("whash", []))
            for k in data[addr]["error_stats"]:
                if k != "final_error":
                    data[addr]["error_stats"][k] += v.get("error", {}).get(k, 0)
    return data


def phash_concentration_score(phash_list):
    if not phash_list:
        return 0.0
    count = Counter(phash_list)
    max_count = count.most_common(1)[0][1]
    return max_count / len(phash_list)


def single_detection_consistency_score(hash_sets):
    if not hash_sets:
        return 0.0

    def pairwise_consistency(phashes):
        pairs = [(0, 1), (1, 2), (0, 2)]
        similar_count = 0
        for i, j in pairs:
            d = hamming_distance(phashes[i], phashes[j])
            if d <= HAMMING_THRESHOLD:
                similar_count += 1
        return similar_count / 3

    scores = []
    for phashes in hash_sets:
        if len(phashes) < 3:
            continue
        scores.append(pairwise_consistency(phashes))
    if not scores:
        return 0.0
    return sum(scores) / len(scores)


def start_frame_stability_score(hash_sets):
    if not hash_sets:
        return 0.0
    first_hashes = [phashes[0] for phashes in hash_sets if len(phashes) >= 1]
    count = Counter(first_hashes)
    max_count = count.most_common(1)[0][1]
    return max_count / len(first_hashes)


def phash_diversity_score(phash_list):
    if not phash_list:
        return 0.0
    unique_phash = list(set(phash_list))
    merged_phash = merge_similar_hashes(unique_phash)
    diversity_ratio = len(merged_phash) / len(phash_list)
    score = 1 - diversity_ratio
    return max(0.0, min(score, 1.0))


def cross_detection_pattern_score(hash_sets):
    if not hash_sets:
        return 0.0

    first_hashes = [phashes[0] for phashes in hash_sets if len(phashes) >= 1]
    count = Counter(first_hashes)
    max_count = count.most_common(1)[0][1]
    concentration_score = max_count / len(first_hashes)

    hashes_seq = first_hashes
    seq_len = len(hashes_seq)
    if seq_len < 4:
        time_offset_score = 0.0
    else:
        time_offset_score = 0.0
        for period in range(1, seq_len // 2 + 1):
            matched = True
            for i in range(seq_len - period):
                if hashes_seq[i] != hashes_seq[i + period]:
                    matched = False
                    break
            if matched:
                time_offset_score = 1.0
                break

    return max(concentration_score, time_offset_score)


def time_span_score(span_days):
    if span_days is None or span_days <= 0:
        return 0.0
    return min(span_days, MAX_TIME_SPAN_DAYS) / MAX_TIME_SPAN_DAYS


def confidence_score(error_stats, total_hashes):
    if total_hashes == 0:
        return 0.0
    fail = error_stats.get("fail_count", 0)
    timeout = error_stats.get("timeout_count", 0)
    neterr = error_stats.get("network_error_count", 0)
    other = error_stats.get("other_error_count", 0)
    error_total = fail + timeout + neterr + other
    ratio = max(0.0, 1 - error_total / total_hashes)
    return ratio


def main():
    Path(os.path.dirname(OUTPUT_CSV_LOOP)).mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV)
    hash_data = load_hash_data(HASH_DIR)

    results = []

    for idx, row in df.iterrows():
        addr = row["地址"]

        addr_hash_info = hash_data.get(addr, None)
        if addr_hash_info is None:
            loop_level = 0
            confidence = 0.0
            is_loop = False
            results.append({**row.to_dict(), "loop_level": loop_level, "confidence": confidence, "is_loop": is_loop})
            continue

        phash_all = addr_hash_info["phash_all"]

        hash_sets = []
        for i in range(0, len(phash_all), 3):
            group = phash_all[i:i + 3]
            if len(group) == 3:
                hash_sets.append(group)

        score_phash_conc = phash_concentration_score(phash_all)
        score_single_consistency = single_detection_consistency_score(hash_sets)
        score_start_stability = start_frame_stability_score(hash_sets)
        score_diversity = phash_diversity_score(phash_all)
        score_cross_pattern = cross_detection_pattern_score(hash_sets)

        span_days = row.get("主phash时间跨度(天)", 0)
        score_time_span = time_span_score(span_days)

        total_hashes = len(phash_all)
        confidence = confidence_score(addr_hash_info["error_stats"], total_hashes)

        total_score = (
            score_phash_conc * WEIGHTS["phash_concentration"] +
            score_single_consistency * WEIGHTS["single_detection_consistency"] +
            score_start_stability * WEIGHTS["start_frame_stability"] +
            score_diversity * WEIGHTS["phash_diversity"] +
            score_cross_pattern * WEIGHTS["cross_detection_pattern"] +
            score_time_span * WEIGHTS["time_span"]
        )

        loop_level = int(round(total_score * 10))
        is_loop = total_score >= LOOP_THRESHOLD

        results.append({**row.to_dict(), "loop_level": loop_level, "confidence": confidence, "is_loop": is_loop})

    df_out = pd.DataFrame(results)
    df_out.to_csv(OUTPUT_CSV_LOOP, index=False, encoding="utf-8-sig")

    # 分别输出轮播和非轮播文件
    df_out_ok = df_out[df_out["is_loop"] == False]
    df_out_not = df_out[df_out["is_loop"] == True]
    df_out_ok.to_csv(OUTPUT_CSV_OK, index=False, encoding="utf-8-sig")
    df_out_not.to_csv(OUTPUT_CSV_NOT, index=False, encoding="utf-8-sig")

    print(f"[INFO] 轮播检测完成，结果已输出：")
    print(f"  全部结果：{OUTPUT_CSV_LOOP}")
    print(f"  非轮播结果：{OUTPUT_CSV_OK}")
    print(f"  轮播结果：{OUTPUT_CSV_NOT}")


if __name__ == "__main__":
    main()