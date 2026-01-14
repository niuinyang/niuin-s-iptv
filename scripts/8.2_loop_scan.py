#!/usr/bin/env python3
import os
import glob
import json
import pandas as pd
from collections import Counter
from datetime import datetime

# --- 配置 ---
HASH_MERGE_DIR = "output/hash/merge"
FAKE_SCAN_CSV = "output/middle/fake_scan.csv"
OUTPUT_DIR = "output/middle/loop"

# --- 工具函数 ---

def load_all_hash_files(directory):
    """加载目录下所有hash合并json文件，返回 {address: {hash_type: [list of hashes]}} 的合并字典"""
    all_data = dict()
    files = sorted(glob.glob(os.path.join(directory, "*-hash-merge.json")))
    for f in files:
        with open(f, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        for addr, hdict in data.items():
            if addr not in all_data:
                all_data[addr] = {
                    "phash": [],
                    "ahash": [],
                    "dhash": [],
                    "whash": [],
                }
            for htype in ["phash", "ahash", "dhash", "whash"]:
                vals = hdict.get(htype, [])
                # 过滤 None 或 null
                vals = [v for v in vals if v]
                all_data[addr][htype].extend(vals)
    return all_data

def safe_hamming_distance(s1, s2):
    """计算两个16进制hash字符串的汉明距离，长度相等"""
    # 转二进制字符串
    if not s1 or not s2 or len(s1) != len(s2):
        return None
    b1 = bin(int(s1, 16))[2:].zfill(len(s1)*4)
    b2 = bin(int(s2, 16))[2:].zfill(len(s2)*4)
    return sum(c1 != c2 for c1, c2 in zip(b1, b2))

def merge_similar_hashes(hash_list, threshold=5):
    """
    对hash列表做简单两两比较，合并汉明距离小于阈值的为一组
    返回合并后的代表hash列表
    """
    if not hash_list:
        return []
    groups = []
    for h in hash_list:
        placed = False
        for g in groups:
            # 和组内任意一个比较
            if any(safe_hamming_distance(h, gh) is not None and safe_hamming_distance(h, gh) <= threshold for gh in g):
                g.append(h)
                placed = True
                break
        if not placed:
            groups.append([h])
    # 返回每组第一个作为代表
    return [g[0] for g in groups]

def calc_concentration_score(hash_list):
    """
    计算hash集中度得分
    集中度定义为：主hash出现次数占比（最高频率）
    返回0~1浮点数
    """
    if not hash_list:
        return 0.0
    c = Counter(hash_list)
    most_common_count = c.most_common(1)[0][1]
    return most_common_count / len(hash_list)

def calculate_loop_level_and_confidence(addr_hashes):
    """
    计算一个地址的 loop_level 和 loop_confidence
    逻辑：
    - 主phash集中度(30分)
    - 同次检测时间跨度内2s/5s/20s phash一致性(辅助，20分)
      这里用hash数量估算，简单近似
    - phash种类数（15分）
    - 汉明距离合并后种类数调整（0~10分）
    - 时间点横向比较集中度（20分）
    - 缺失率影响置信度
    """
    phash_list = addr_hashes.get("phash", [])
    ahash_list = addr_hashes.get("ahash", [])
    dhash_list = addr_hashes.get("dhash", [])
    whash_list = addr_hashes.get("whash", [])

    total_phash = len(phash_list)
    if total_phash == 0:
        # 无数据，评级最低置信度最低
        return 100.0, 0.0

    # 主phash集中度
    phash_conc = calc_concentration_score(phash_list)  # 0~1
    score_phash = phash_conc * 30  # 0~30

    # 同次检测时间跨度一致性 - 这里简单估算：假设hash数/12次的比例来估计
    # 12次为满分，少于12按比例扣分
    times_detected = total_phash / 3 / 4  # 432=3*12*4*3,粗略估计，实际稍有误差
    times_ratio = min(times_detected / 12, 1.0)
    score_time_span = times_ratio * 20  # 0~20

    # phash种类数和合并种类数
    distinct_phash = len(set(phash_list))
    merged_phash = merge_similar_hashes(phash_list)
    merged_count = len(merged_phash)

    # 种类越少越轮播，15分满分对应1种
    score_species = 0
    if distinct_phash > 0:
        score_species = max(0, (15 * (1 - (merged_count / distinct_phash))))  # 0~15

    # 时间点横向比较集中度 - 用ahash替代近似
    ahash_conc = calc_concentration_score(ahash_list)
    score_horizontal = ahash_conc * 20  # 0~20

    # 总分
    total_score = score_phash + score_time_span + score_species + score_horizontal
    total_score = min(total_score, 100)

    # 计算置信度，基于数据完整度和空缺率
    total_hash_count = sum(len(lst) for lst in addr_hashes.values())
    expected_hash_count = 12 * 4 * 3  # 理论最大432
    data_completeness = min(total_hash_count / expected_hash_count, 1.0)
    confidence = data_completeness * 100

    # loop_level = 100 - 总分，数值越小越轮播
    loop_level = 100 - total_score
    loop_level = max(0, min(100, loop_level))

    loop_confidence = max(0, min(100, confidence))

    return loop_level, loop_confidence


# --- 主流程 ---

def main():
    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 读取fake_scan.csv
    df_fake = pd.read_csv(FAKE_SCAN_CSV)

    # 读取所有hash数据
    print("加载所有hash合并文件...")
    all_hash_data = load_all_hash_files(HASH_MERGE_DIR)
    print(f"加载地址数：{len(all_hash_data)}")

    # 准备新增列
    loop_levels = []
    loop_confidences = []

    # 逐地址计算评级
    print("计算轮播评级...")
    for idx, row in df_fake.iterrows():
        addr = row['地址']
        addr_hashes = all_hash_data.get(addr, {
            "phash": [],
            "ahash": [],
            "dhash": [],
            "whash": [],
        })
        loop_level, loop_confidence = calculate_loop_level_and_confidence(addr_hashes)
        loop_levels.append(loop_level)
        loop_confidences.append(loop_confidence)

        if idx % 100 == 0:
            print(f"处理 {idx+1}/{len(df_fake)} 条数据")

    # 插入新列
    df_fake['loop_level'] = loop_levels
    df_fake['loop_confidence'] = loop_confidences

    # 输出csv，按时间命名
    now_str = datetime.now().strftime("%y%m%d%H%M")
    output_path = os.path.join(OUTPUT_DIR, f"{now_str}_loop_scan.csv")
    df_fake.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"轮播评级完成，结果保存至：{output_path}")


if __name__ == "__main__":
    main()
