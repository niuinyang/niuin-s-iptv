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
    # 空值数量统计
    none_count = sum(x is None for x in p)

    # 若两个以上空，等级5
    if none_count >= 2:
        return 5

    # 若一个空，判断剩余两个是否相同
    if none_count == 1:
        valid = [x for x in p if x is not None]
        if valid[0] == valid[1]:
            return 4
        else:
            return 3

    # 无空值，计算两两是否相同
    # 两两比较次数 3 次： (0,1),(0,2),(1,2)
    diffs = 0
    if p[0] != p[1]:
        diffs += 1
    if p[0] != p[2]:
        diffs += 1
    if p[1] != p[2]:
        diffs += 1

    if diffs == 3:
        return 1  # 全不同
    elif diffs == 2:
        return 2  # 两不同一相同
    elif diffs == 0:
        return 6  # 全相同
    else:
        # 理论上不会出现1次不同两次相同情况，兜底
        return 4

def map_history_dynamic_level(dynamic_ratio):
    """
    历史动态级映射
    输入动态比例（0~1）
    返回0~9等级
    """
    thresholds = [0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1]
    for i, t in enumerate(thresholds):
        if dynamic_ratio >= t:
            return i
    return 9

def map_phash_repeat_index(repeat_count):
    """
    phash重复指数映射，0~9
    根据重复次数分段映射，参考设计
    """
    if repeat_count <= 1:
        return 0
    elif repeat_count <= 4:
        return 1 + (repeat_count - 2) * (3 / 2)  # 线性映射2-4到1-3
    elif repeat_count <= 8:
        return 4 + (repeat_count - 5) * (2 / 3)  # 线性映射5-8到4-6
    elif repeat_count <= 13:
        return 7 + (repeat_count - 9) * (1 / 4)  # 线性映射9-13到7-8
    elif repeat_count <= 36:
        # 14-36 映射到8-9
        return 8 + (repeat_count - 14) * (1 / 22)
    else:
        return 9

def safe_get(d, key):
    """安全取值"""
    return d.get(key) if d and key in d else None

# -------------- 主流程 --------------

def main():
    print("开始读取输入 CSV 文件...")
    df = pd.read_csv(INPUT_CSV)

    # 加载所有 hash json 文件，按文件名时间排序（从旧到新）
    print("加载 hash JSON 文件...")
    json_files = sorted(glob.glob(HASH_JSON_PATTERN))
    if not json_files:
        print("未找到任何 hash JSON 文件，退出")
        return

    # 结构：{ url: {检测时间1: [phash3个], 检测时间2: [phash3个], ... } }
    all_hashes = dict()

    for jf in json_files:
        # 文件名示例 2601150143-hash-merge.json ，前缀可解析成时间点方便后续排序
        # 简单用文件修改时间作为时间戳
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
                # 无效数据跳过
                continue
            if url not in all_hashes:
                all_hashes[url] = []
            all_hashes[url].append({
                'time': file_time,
                'phash': [x if x else None for x in phash_list]
            })

    # 对每个 url，按时间排序其检测记录
    for url in all_hashes:
        all_hashes[url].sort(key=lambda x: x['time'])

    # 建立辅助索引，从 df 里地址到行索引映射，方便后续赋值
    url_to_idx = {url: idx for idx, url in enumerate(df['地址'])}

    # 初始化输出列
    current_dynamic_level_col = []
    history_dynamic_level_col = []
    phash_repeat_index_col = []
    main_phash_col = []
    phash_unique_index_col = []
    phash_sample_count_col = []
    main_phash_span_days_col = []

    print("开始逐条分析每个源的动态特征...")

    for idx, row in df.iterrows():
        url = row['地址']
        hash_records = all_hashes.get(url, [])

        if not hash_records:
            # 无检测记录，填默认值
            current_dynamic_level_col.append(None)
            history_dynamic_level_col.append(None)
            phash_repeat_index_col.append(None)
            main_phash_col.append(None)
            phash_unique_index_col.append(None)
            phash_sample_count_col.append(0)
            main_phash_span_days_col.append(0)
            continue

        # 当前动态级 - 用最近一次检测的3个phash
        last_record = hash_records[-1]
        cdl = compare_phash_level(last_record['phash'])
        current_dynamic_level_col.append(cdl)

        # 历史动态级 - 统计所有检测的当前动态级是否属于动态等级(1/2/3)
        dynamic_counts = 0
        total_counts = len(hash_records)
        for rec in hash_records:
            level = compare_phash_level(rec['phash'])
            if level in [1,2,3]:
                dynamic_counts += 1
        dynamic_ratio = dynamic_counts / total_counts if total_counts > 0 else 0
        hdl = map_history_dynamic_level(dynamic_ratio)
        history_dynamic_level_col.append(hdl)

        # 统计所有 phash 的重复次数
        all_phashes = []
        for rec in hash_records:
            all_phashes.extend(rec['phash'])
        # 过滤 None
        all_phashes = [x for x in all_phashes if x]

        phash_sample_count_col.append(len(all_phashes))

        if not all_phashes:
            # 无有效 phash
            phash_repeat_index_col.append(None)
            main_phash_col.append(None)
            phash_unique_index_col.append(0)
            main_phash_span_days_col.append(0)
            continue

        # 计算重复次数统计
        from collections import Counter
        counter = Counter(all_phashes)
        max_repeat = max(counter.values())
        unique_count = len(counter)
        phash_unique_index_col.append(unique_count)

        pri = int(round(map_phash_repeat_index(max_repeat)))
        phash_repeat_index_col.append(pri)

        # 主phash值选取逻辑（多值重复次数相同时优先最近检测，时间相同优先50s）
        max_repeat_phashes = [ph for ph, cnt in counter.items() if cnt == max_repeat]

        # 找出时间最近且在max_repeat_phashes中的phash
        candidate = None
        candidate_time = -1
        for rec in reversed(hash_records):
            # 三个时间点 phash
            phs = rec['phash']
            # 50s 对应索引2
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

        # 计算主phash时间跨度（单位天）
        times_of_main = [rec['time'] for rec in hash_records if candidate in rec['phash']]
        if times_of_main:
            span_seconds = max(times_of_main) - min(times_of_main)
            span_days = span_seconds / (3600*24)
        else:
            span_days = 0
        main_phash_span_days_col.append(round(span_days, 4))

    # 确保输出目录存在
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # 将新增列合并入 df
    df['当前动态级'] = current_dynamic_level_col
    df['历史动态级'] = history_dynamic_level_col
    df['phash重复指数'] = phash_repeat_index_col
    df['主phash值'] = main_phash_col
    df['phash唯一指数'] = phash_unique_index_col
    df['phash有效样本数'] = phash_sample_count_col
    df['主phash时间跨度(天)'] = main_phash_span_days_col

    print(f"开始写入输出文件：{OUTPUT_CSV}")
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')

    print("处理完成！")

if __name__ == "__main__":
    main()
