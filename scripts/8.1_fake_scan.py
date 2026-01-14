#!/usr/bin/env python3
import os
import glob
import json
import pandas as pd
from collections import Counter
from datetime import datetime   # <<< MODIFIED

# --- 配置 ---
DEEP_CSV = "output/middle/deep/deep_total_ok.csv"
HASH_DIR = "output/hash/merge"

# <<< MODIFIED: 输出文件名使用 yymmddhhmm >>>
timestamp = datetime.now().strftime("%y%m%d%H%M")
OUTPUT_CSV = f"output/middle/{timestamp}_fake_scan.csv"
# <<< MODIFIED END >>>

# --- 读取deep_total_ok.csv ---
df = pd.read_csv(DEEP_CSV)

# --- 读取hash合并json ---
hash_files = sorted(glob.glob(os.path.join(HASH_DIR, "*-hash-merge.json")))
if len(hash_files) == 0:
    raise RuntimeError("没有找到任何hash文件，请检查路径和文件名。")

# 保证时间顺序：文件名越大越新，这里升序，索引0是最远，最后是最近
print(f"读取到 {len(hash_files)} 个hash文件。")

# 解析所有hash数据到列表，顺序: 最远 -> 最近
all_hash_data = []
for fpath in hash_files:
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)
    all_hash_data.append(data)

N = len(all_hash_data)
WEIGHTS = list(range(1, N + 1))  # 权重从1递增到N，1对应最远，N对应最近

# --- 辅助函数 ---
def phash_major_and_count(phash_lists):
    flat = []
    for phs in phash_lists:
        if phs:
            flat.extend(phs)
    if not flat:
        return None, 0
    c = Counter(flat)
    most_common = c.most_common(1)[0]
    return most_common[0], most_common[1]

def count_failures(error_dict):
    if not error_dict:
        return 0
    return error_dict.get("fail_count", 0)

def compare_hash(h1, h2):
    if h1 is None or h2 is None:
        return False
    return h1 == h2

# --- 主处理 ---
results = []

for idx, row in df.iterrows():
    url = row["地址"]
    dynamic_score = 0
    total_fail_count = 0
    avg_fetch_time_latest = None

    all_phash_lists = []
    last_phash_list = None

    detection_appear_count = 0  # 统计该地址在hash文件中出现的次数（无论数据是否有效）

    for i, hash_data in enumerate(all_hash_data):
        if url in hash_data:
            detection_appear_count += 1  # 出现就计数

        info = hash_data.get(url)
        if not info:
            continue

        if i == N - 1:
            total_fail_count = count_failures(info.get("error", {}))
            avg_fetch_time_latest = info.get("stats", {}).get("avg_fetch_time", None)

        phash_list = info.get("phash", [])
        all_phash_lists.append(phash_list)

        if i == N - 1:
            last_phash_list = phash_list

        # 动态等级计算，两两比较3组
        ph = phash_list + [None] * (3 - len(phash_list))
        comparisons = [
            (ph[0], ph[1]),
            (ph[0], ph[2]),
            (ph[1], ph[2]),
        ]

        score = 0
        for h1, h2 in comparisons:
            if not compare_hash(h1, h2):
                score += 1

        dynamic_score += score * WEIGHTS[i]

    max_phash, max_count = phash_major_and_count(all_phash_lists)

    if max_count == 1:
        if last_phash_list:
            max_phash = last_phash_list[-1] if last_phash_list[-1] is not None else None

    row_dict = row.to_dict()
    row_dict["检测次数"] = detection_appear_count
    row_dict["动态级别"] = dynamic_score
    row_dict["平均抓帧时间"] = avg_fetch_time_latest
    row_dict["抓帧失败次数"] = total_fail_count
    row_dict["出现最多次数phash的次数"] = max_count
    row_dict["出现最多次数的phash值"] = max_phash

    results.append(row_dict)

# --- 输出结果 ---
df_out = pd.DataFrame(results)
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print(f"完成，结果已保存至 {OUTPUT_CSV}")