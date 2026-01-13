#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import glob
from collections import defaultdict
import imagehash

HASH_DIR = "output/hash/merge"
INPUT_CSV = "output/middle/deep/deep_total_ok.csv"
OUTPUT_DIR = "output/middle/fake-scan"

PHASH_STATIC_THRESHOLD = 5

STATIC_RATIO_THRESHOLD = 0.5
FROZEN_RATIO_THRESHOLD = 0.33
INVALID_RATIO_THRESHOLD = 0.5

def hamming(h1, h2):
    if not h1 or not h2:
        return 9999
    try:
        return imagehash.hex_to_hash(h1) - imagehash.hex_to_hash(h2)
    except Exception:
        return 9999

def is_black_or_invalid(ahash, dhash):
    if not ahash or not dhash or len(ahash) < 3 or len(dhash) < 3:
        return True
    if any(x is None or x == "" for x in ahash[:3]):
        return True
    if any(x is None or x == "" for x in dhash[:3]):
        return True
    for i in range(2):
        if hamming(ahash[i], ahash[i + 1]) > 1:
            return False
    return True

def classify_sample(phash, ahash, dhash):
    if not phash or len(phash) < 3 or any(x is None or x == "" for x in phash[:3]):
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

def build_fake_scan_map():
    files = sorted(glob.glob(os.path.join(HASH_DIR, "*-hash-merge.json")))
    if not files:
        raise RuntimeError(f"❌ 未找到任何 hash-merge.json 文件，路径：{HASH_DIR}")

    stats = defaultdict(lambda: defaultdict(int))
    valid_count = defaultdict(int)

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        for url, h in data.items():
            phash = h.get("phash")
            ahash = h.get("ahash")
            dhash = h.get("dhash")

            if (
                phash is None or ahash is None or dhash is None or
                any(x is None for x in phash) or
                any(x is None for x in ahash) or
                any(x is None for x in dhash)
            ):
                continue

            cls = classify_sample(phash, ahash, dhash)
            stats[url][cls] += 1
            valid_count[url] += 1

    result = {}

    for url, c in stats.items():
        total = valid_count.get(url, 0)
        if total == 0:
            result[url] = "invalid"
            continue
        static_ratio = c["static"] / total
        frozen_ratio = c["frozen"] / total
        invalid_ratio = c["invalid"] / total

        if static_ratio >= STATIC_RATIO_THRESHOLD:
            result[url] = "fake_static"
        elif frozen_ratio >= FROZEN_RATIO_THRESHOLD:
            result[url] = "fake_frozen"
        elif invalid_ratio >= INVALID_RATIO_THRESHOLD:
            result[url] = "invalid"
        else:
            result[url] = "pass"

    return result, valid_count

def main():
    fake_map, valid_count = build_fake_scan_map()
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

    print(f"✅ 8.1_fake_scan 完成，有效采样URL数: {len(valid_count)}")
    print(f"通过数量: {len(ok_rows)}  未通过数量: {len(not_rows)}")

if __name__ == "__main__":
    main()
