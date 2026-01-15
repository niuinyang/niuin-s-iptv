#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import json
import pandas as pd
from datetime import datetime

# -------------- 配置区 --------------

# 输入 CSV 文件（deep_total_ok.csv）
INPUT_CSV = "output/middle/deep/deep_total_ok.csv"

# Hash JSON 文件路径通配符，自动加载所有符合的文件
HASH_JSON_PATTERN = "output/hash/merge/*-hash-merge.json"

# 输出 CSV 文件
OUTPUT_CSV = "output/middle/fake/fake_scan.csv"

# -------------- 工具函数 --------------

def hamming_distance_hash(s1, s2):
    """计算两个16进制字符串的汉明距离"""
    if s1 is None or s2 is None:
        return None
    try:
        b1 = int(s1, 16)
        b2 = int(s2, 16)
        x = b1 ^ b2
        return bin(x).count('1')
    except Exception:
        return None

def compare_phash_level(phashes):
    """
    第一层当前动态级判定，输入3个phash列表（可含None）
    返回1~6级别
    """
    p = phashes
    none_count = sum(x is None for x in p)

    if none_count >= 2:
        return 5

    if none_count == 1:
        valid = [x for x in p if x is not None]
        if valid[0] == valid[1]:
            return 4
        else:
            return 3

    diffs = 0
    if p[0] != p[1]:
        diffs += 1
    if p[0] != p[2]:
        diffs += 1
    if p[1] != p[2]:
        diffs += 1

    if diffs == 3:
        return 1
    elif diffs == 2:
        return 2
    elif diffs == 0:
        return 6
    else:
        return 4

def map_history_dynamic_level(dynamic_ratio):
    """
    历史动态级映射，输入动态比例（0~1），返回0~9等级
    """
    thresholds = [0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1]
    for i, t in enumerate(thresholds):
        if dynamic_ratio >= t:
            return i
    return 9

def map_phash_repeat_index(repeat_count):
    """
    phash重复指数映射，0~9
    """
    if repeat_count <= 1:
        return 0
    elif repeat_count <= 4:
        return 1 + (repeat_count - 2) * (3 / 2)
    elif repeat_count <= 8:
        return 4 + (repeat_count - 5) * (2 / 3)
    elif repeat_count <= 13:
        return 7 + (repeat_count - 9) * (1 / 4)
    elif repeat_count <= 36:
        return 8 + (repeat_count - 14) * (1 / 22)
    else:
        return 9

def safe_get(d, key):
    """安全取值"""
    return d.get(key) if d and key in d else None

def classify_source(row):
    # 空值保护
    sample_count = row['phash有效样本数'] if pd.notna(row['phash有效样本数']) else 0
    current_level = row['当前动态级'] if pd.notna(row['当前动态级']) else 99
    history_level = row['历史动态级'] if pd.notna(row['历史动态级']) else 99
    repeat_index = row['phash重复指数'] if pd.notna(row['phash重复指数']) else 0
    unique_index = row['phash唯一指数'] if pd.notna(row['phash唯一指数']) else 99

    if sample_count < 5:
        return "待观察"
    elif current_level <= 3 or history_level <= 4:
        return "动态源"
    elif (current_level >= 5 and
          5 <= history_level <= 6 and
          5 <= repeat_index <= 7 and
          unique_index >= 4):
        return "待观察"
    elif (current_level >= 5 and
          history_level >= 7 and
          repeat_index >= 7 and
          unique_index <= 3):
        return "静态假源"
    else:
        return "待观察"

# -------------- 主流程 --------------

def main():
    print("开始读取输入 CSV 文件...")
    df = pd.read_csv(INPUT_CSV)

    print("加载 hash JSON 文件...")
    json_files = sorted(glob.glob(HASH_JSON_PATTERN))
    if not json_files:
        print("未找到任何 hash JSON 文件，退出")
        return

    all_hashes = dict()

    for jf in json_files:
        file_time = os.path.getmtime(jf)
        with open(jf, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except Exception as e:
                print(f"解析文件 {jf} 出错: {e}")
                continue
        for url, info in data.items():
            phash_list = safe_get(info, 'phash')
            if not phash_list or len(phash_list) != 3:
                continue
            if url not in all_hashes:
                all_hashes[url] = []
            all_hashes[url].append({
                'time': file_time,
                'phash': [x if x else None for x in phash_list]
            })

    for url in all_hashes:
        all_hashes[url].sort(key=lambda x: x['time'])

    current_dynamic_level_col = []
    history_dynamic_level_col = []
    phash_repeat_index_col = []
    main_phash_col = []
    phash_unique_index_col = []
    phash_sample_count_col = []
    main_phash_span_days_col = []

    print("开始逐条分析每个源的动态特征...")

    from collections import Counter

    for idx, row in df.iterrows():
        url = row['地址']
        hash_records = all_hashes.get(url, [])

        if not hash_records:
            current_dynamic_level_col.append(None)
            history_dynamic_level_col.append(None)
            phash_repeat_index_col.append(None)
            main_phash_col.append(None)
            phash_unique_index_col.append(None)
            phash_sample_count_col.append(0)
            main_phash_span_days_col.append(0)
            continue

        last_record = hash_records[-1]
        cdl = compare_phash_level(last_record['phash'])
        current_dynamic_level_col.append(cdl)

        dynamic_counts = 0
        total_counts = len(hash_records)
        for rec in hash_records:
            level = compare_phash_level(rec['phash'])
            if level in [1,2,3]:
                dynamic_counts += 1
        dynamic_ratio = dynamic_counts / total_counts if total_counts > 0 else 0
        hdl = map_history_dynamic_level(dynamic_ratio)
        history_dynamic_level_col.append(hdl)

        all_phashes = []
        for rec in hash_records:
            all_phashes.extend(rec['phash'])
        all_phashes = [x for x in all_phashes if x]

        phash_sample_count_col.append(len(all_phashes))

        if not all_phashes:
            phash_repeat_index_col.append(None)
            main_phash_col.append(None)
            phash_unique_index_col.append(0)
            main_phash_span_days_col.append(0)
            continue

        counter = Counter(all_phashes)
        max_repeat = max(counter.values())
        unique_count = len(counter)
        phash_unique_index_col.append(unique_count)

        pri = int(round(map_phash_repeat_index(max_repeat)))
        phash_repeat_index_col.append(pri)

        max_repeat_phashes = [ph for ph, cnt in counter.items() if cnt == max_repeat]

        candidate = None
        candidate_time = -1
        for rec in reversed(hash_records):
            phs = rec['phash']
            for i in range(2, -1, -1):
                ph = phs[i]
                if ph in max_repeat_phashes:
                    if rec['time'] > candidate_time:
                        candidate = ph
                        candidate_time = rec['time']
                    break
            if candidate is not None:
                break
        main_phash_col.append(candidate)

        times_of_main = [rec['time'] for rec in hash_records if candidate in rec['phash']]
        if times_of_main:
            span_seconds = max(times_of_main) - min(times_of_main)
            span_days = span_seconds / (3600*24)
        else:
            span_days = 0
        main_phash_span_days_col.append(round(span_days, 4))

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    df['当前动态级'] = current_dynamic_level_col
    df['历史动态级'] = history_dynamic_level_col
    df['phash重复指数'] = phash_repeat_index_col
    df['主phash值'] = main_phash_col
    df['phash唯一指数'] = phash_unique_index_col
    df['phash有效样本数'] = phash_sample_count_col
    df['主phash时间跨度(天)'] = main_phash_span_days_col

    # 新增 源动态分类 列
    df['源动态分类'] = df.apply(classify_source, axis=1)

    print(f"开始写入输出文件：{OUTPUT_CSV}")
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')

    output_dir = os.path.dirname(OUTPUT_CSV)
    base_name = os.path.splitext(os.path.basename(OUTPUT_CSV))[0]

    # 动态源和待观察合并输出 ok 文件
    df_ok = df[df['源动态分类'].isin(['动态源', '待观察'])]
    output_ok = os.path.join(output_dir, base_name + "-ok.csv")
    df_ok.to_csv(output_ok, index=False, encoding='utf-8-sig')

    # 静态假源输出 not 文件
    df_not = df[df['源动态分类'] == '静态假源']
    output_not = os.path.join(output_dir, base_name + "-not.csv")
    df_not.to_csv(output_not, index=False, encoding='utf-8-sig')

    print(f"处理完成，生成文件：\n  {OUTPUT_CSV}\n  {output_ok}\n  {output_not}")

if __name__ == "__main__":
    main()
