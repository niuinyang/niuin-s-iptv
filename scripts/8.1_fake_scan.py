#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
8.1_fake_scan.py

目标：
- 排除长期画面不动的伪直播
- 包括：测试卡 / LOGO 页 / 停播页 / 起播后卡死

设计原则：
- 只通过“时间维度上的画面活性”判定
- 不因单次异常误杀真实直播
"""

import json
import os
from collections import defaultdict
import imagehash

# =========================
# 可调参数（非常重要）
# =========================

# phash 汉明距离阈值（<= 认为是同一画面）
PHASH_STATIC_THRESHOLD = 5

# 至少多少次采样被判定为 static 才认为是静态伪直播
STATIC_COUNT_THRESHOLD = 9

# frozen 判定阈值
FROZEN_COUNT_THRESHOLD = 6

# invalid（黑屏 / 无效帧）阈值
INVALID_COUNT_THRESHOLD = 6

# =========================
# 工具函数
# =========================

def hamming_distance(h1, h2):
    """计算两个 hash 的汉明距离"""
    return imagehash.hex_to_hash(h1) - imagehash.hex_to_hash(h2)


def is_black_or_invalid(ahash_list, dhash_list):
    """
    判断一组帧是否是黑屏 / 无效帧

    设计思路：
    - 黑屏的 ahash / dhash 熵极低
    - 3 帧都非常接近，且 hash 形态异常
    """
    if not ahash_list or not dhash_list:
        return True

    # 三帧彼此极度接近
    for i in range(2):
        if hamming_distance(ahash_list[i], ahash_list[i+1]) > 1:
            return False

    return True


def classify_sample(phash_list, ahash_list, dhash_list):
    """
    对一次采样（2s / 5s / 20s）进行分类

    返回：
    - invalid : 黑屏 / 无效
    - static  : 三帧几乎一致
    - frozen  : 前动后停
    - active  : 正常变化
    """

    # Step 1：先排除无效帧
    if is_black_or_invalid(ahash_list, dhash_list):
        return "invalid"

    # Step 2：phash 判断画面是否变化
    d_1_2 = hamming_distance(phash_list[0], phash_list[1])
    d_2_3 = hamming_distance(phash_list[1], phash_list[2])

    # 三帧几乎完全一致 → 静态画面
    if d_1_2 <= PHASH_STATIC_THRESHOLD and d_2_3 <= PHASH_STATIC_THRESHOLD:
        return "static"

    # 前面动，后面不动 → 卡死
    if d_1_2 > PHASH_STATIC_THRESHOLD and d_2_3 <= PHASH_STATIC_THRESHOLD:
        return "frozen"

    # 其余情况 → 正常直播
    return "active"


# =========================
# 主逻辑
# =========================

def main():
    input_file = "output/hash/hash_total.json"

    if not os.path.exists(input_file):
        print("❌ 找不到 hash_total.json")
        return

    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = {}

    fake_static = []
    fake_frozen = []
    invalid_stream = []
    passed = []

    for url, samples in data.items():
        counts = defaultdict(int)

        # 用于跨采样比较 20s phash
        static_20s_hashes = []

        for ts, hashes in samples.items():
            phash = hashes.get("phash", [])
            ahash = hashes.get("ahash", [])
            dhash = hashes.get("dhash", [])

            if len(phash) < 3:
                counts["invalid"] += 1
                continue

            cls = classify_sample(phash, ahash, dhash)
            counts[cls] += 1

            if cls == "static":
                static_20s_hashes.append(phash[2])

        # =========================
        # 最终判定
        # =========================

        # 静态伪直播
        if counts["static"] >= STATIC_COUNT_THRESHOLD:
            # 验证 20s phash 是否高度一致
            base = static_20s_hashes[0]
            same = sum(
                1 for h in static_20s_hashes
                if hamming_distance(base, h) <= PHASH_STATIC_THRESHOLD
            )

            if same >= STATIC_COUNT_THRESHOLD - 1:
                fake_static.append(url)
                result[url] = "fake_static"
                continue

        # 卡死伪直播
        if counts["frozen"] >= FROZEN_COUNT_THRESHOLD and counts["active"] <= 2:
            fake_frozen.append(url)
            result[url] = "fake_frozen"
            continue

        # 无效流（不判定为假）
        if counts["invalid"] >= INVALID_COUNT_THRESHOLD:
            invalid_stream.append(url)
            result[url] = "invalid"
            continue

        # 其余通过
        passed.append(url)
        result[url] = "pass"

    # =========================
    # 输出结果
    # =========================

    os.makedirs("output/8.1", exist_ok=True)

    def save_list(name, lst):
        with open(f"output/8.1/{name}.txt", "w", encoding="utf-8") as f:
            for u in lst:
                f.write(u + "\n")

    save_list("fake_static", fake_static)
    save_list("fake_frozen", fake_frozen)
    save_list("invalid", invalid_stream)
    save_list("pass", passed)

    with open("output/8.1/summary.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("✅ 8.1 fake scan 完成")
    print(f"static={len(fake_static)} frozen={len(fake_frozen)} invalid={len(invalid_stream)} pass={len(passed)}")


if __name__ == "__main__":
    main()
