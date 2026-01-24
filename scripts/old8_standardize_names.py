#!/usr/bin/env python3
# standardize_iptv.py (CSV 版本，无 Excel)

import os
import re
import csv
import sys
import time
import chardet
import pandas as pd
from opencc import OpenCC
from rapidfuzz import fuzz, process
from tqdm import tqdm

# 配置文件路径，全部采用 CSV 格式
MY_SUM_PATH = "output/middle/merge/mysource_total.csv"
WORKING_PATH = "output/middle/working.csv"
CHANNEL_DATA_PATH = "input/channel_data.csv"        # channel_data CSV 文件路径（读写）
NETWORK_CHANNELS_PATH = "input/iptv-org/database/data/channels.csv"

OUTPUT_TOTAL_FINAL = "output/total_final.csv"       # 最终输出总表路径
OUTPUT_CHANNEL_DATA = CHANNEL_DATA_PATH              # channel_data 保存路径

cc = OpenCC('t2s')  # 繁体转简体转换器

# 自动检测文件编码并读取 CSV 文件
def read_csv_auto_encoding(filepath):
    with open(filepath, 'rb') as f:
        raw = f.read(10000)
        result = chardet.detect(raw)
        encoding = result['encoding'] or 'utf-8'
    return pd.read_csv(filepath, encoding=encoding)

# 机械式标准化频道名（去括号、空格、小写等）
def mechanical_standardize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.strip()
    s = cc.convert(s)
    s = s.lower()
    s = re.sub(r"\（.*?\）", "", s)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\[.*?\]", "", s)
    s = re.sub(r"\【.*?\】", "", s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^a-z0-9\u4e00-\u9fa5\+\！]", "", s)
    return s

# 清理网络频道库中的频道名格式（首字母大写等）
def clean_network_std_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = ' '.join([
        w.capitalize() if re.match(r'[a-zA-Z]+$', w) else w
        for w in name.split(" ")
    ])
    return name

def main():
    print("开始读取文件...")

    # 读取输入数据 my_sum 和 working
    my_sum = read_csv_auto_encoding(MY_SUM_PATH)
    working = read_csv_auto_encoding(WORKING_PATH)

    # 如果 channel_data 文件不存在，创建空表格（带基础列）
    if not os.path.exists(CHANNEL_DATA_PATH):
        pd.DataFrame(columns=["原始名", "标准名", "拟匹配频道名", "分组"]).to_csv(
            CHANNEL_DATA_PATH, index=False, encoding="utf-8-sig"
        )

    # 读取 channel_data，准备后续使用
    channel_data = read_csv_auto_encoding(CHANNEL_DATA_PATH)

    # 新增并填充 channel_data 的 “来源” 列，从输入文件的来源字段匹配填充
    source_dict = {}
    for df in [my_sum, working]:
        for idx, row in df.iterrows():
            orig_name = row.get("频道名", "")
            src = row.get("来源", "")
            if orig_name and src:
                if orig_name not in source_dict:
                    source_dict[orig_name] = src

    if "来源" not in channel_data.columns:
        channel_data["来源"] = channel_data["原始名"].map(source_dict).fillna("")
    else:
        channel_data["来源"] = channel_data.apply(
            lambda row: source_dict.get(row["原始名"], row["来源"]) if not row["来源"] else row["来源"],
            axis=1
        )

    # 新增“输出顺序”列，默认所有值为“未排序”
    if "输出顺序" not in channel_data.columns:
        channel_data["输出顺序"] = "未排序"

    # 新增“是否已维护”列，默认所有值为“否”
    if "是否已维护" not in channel_data.columns:
        channel_data["是否已维护"] = "否"

    # 读取网络频道数据库，确定频道名所在列并去空
    network_channels_df = read_csv_auto_encoding(NETWORK_CHANNELS_PATH)
    if "channel" in network_channels_df.columns:
        network_col = "channel"
    elif "name" in network_channels_df.columns:
        network_col = "name"
    else:
        print("网络数据库无频道名列！")
        sys.exit(1)

    network_channels_df = network_channels_df.dropna(subset=[network_col])
    network_channels_df["std_key"] = network_channels_df[network_col].apply(mechanical_standardize)
    network_channels = dict(zip(network_channels_df["std_key"], network_channels_df[network_col]))

    # 统一输入数据字段，确保以下字段存在，不存在则新建空列
    for df in [my_sum, working]:
        for col in ["视频编码", "分辨率", "帧率", "音频", "相似度"]:
            if col not in df.columns:
                df[col] = ""

    # 合并两个输入数据集，合并后所有字段齐全
    total_before = pd.concat([my_sum, working], ignore_index=True, sort=False)

    # 确保所有必须字段存在，不存在补空
    required_cols = ["频道名", "地址", "来源", "图标", "检测时间",
                     "分组", "视频编码", "分辨率", "帧率", "音频", "相似度"]
    for col in required_cols:
        if col not in total_before.columns:
            total_before[col] = ""

    # 新增“轮回相似度”列，来源输入文件，缺失填“无”
    if "轮回相似度" not in total_before.columns:
        total_before["轮回相似度"] = "无"
    else:
        total_before["轮回相似度"] = total_before["轮回相似度"].fillna("无")

    # 频道名标准化字段（用于匹配）
    total_before["std_key"] = total_before["频道名"].apply(mechanical_standardize)

    # channel_data 中添加标准化辅助列，方便匹配
    channel_data["标准名_std_key"] = channel_data["标准名"].apply(mechanical_standardize)
    channel_data["原始名_std_key"] = channel_data["原始名"].apply(mechanical_standardize)

    # 现有原始名集合，避免重复新增
    existing_orig_names = set(channel_data["原始名"].fillna("").unique())

    # channel_data 映射字典，方便查找标准名和拟匹配频道名
    std_name_dict = dict(zip(channel_data["标准名_std_key"], channel_data["标准名"]))
    std_key_to_pending = dict(zip(channel_data["标准名_std_key"], channel_data["拟匹配频道名"]))

    # 准备映射标准名到输出顺序，方便后续填充
    std_name_to_output_order = dict(zip(channel_data["标准名"], channel_data["输出顺序"]))

    # 用于存储匹配结果的列表
    matched_standard_names = []
    matched_match_info = []
    matched_match_score = []

    precise_match_count = 0
    fuzzy_match_count = 0

    # 辅助函数：新增 channel_data 记录（避免重复）
    def add_channel_data_if_not_exists(orig_name, std_name, group_label):
        nonlocal channel_data, existing_orig_names
        if orig_name not in existing_orig_names:
            new_row = {
                "原始名": orig_name,
                "标准名": std_name,
                "拟匹配频道名": std_name,
                "分组": group_label,
                "来源": source_dict.get(orig_name, ""),
                "输出顺序": "未排序",
                "是否已维护": "否"
            }
            channel_data = pd.concat(
                [channel_data, pd.DataFrame([new_row])],
                ignore_index=True
            )
            existing_orig_names.add(orig_name)

    print("开始匹配标准化频道名...")

    total_len = len(total_before)
    batch_size = 50
    last_print_time = time.time()

    # 批量遍历，逐条匹配频道名
    for start_idx in tqdm(range(0, total_len, batch_size), desc="匹配进度"):
        end_idx = min(start_idx + batch_size, total_len)
        batch = total_before.iloc[start_idx:end_idx]

        for idx, row in batch.iterrows():
            original_name = row["频道名"]
            key = row["std_key"]

            matched_name = None
            match_info = "未匹配"
            match_score = 0.0

            # 精准匹配逻辑：必须 channel_data 中存在原始名且“是否已维护”为“是”
            if original_name in existing_orig_names:
                matched_std_name = channel_data.loc[
                    channel_data["原始名"] == original_name, "标准名"
                ].values
                maintained_val = channel_data.loc[
                    channel_data["原始名"] == original_name, "是否已维护"
                ].values

                if len(matched_std_name) > 0 and len(maintained_val) > 0:
                    mv = maintained_val[0]
                    if isinstance(mv, str) and mv.strip() == "是":
                        matched_name = matched_std_name[0]
                        match_info = "精准匹配"
                        match_score = 100.0
                        precise_match_count += 1
                    else:
                        matched_name = None
                else:
                    matched_name = None

            # 模糊匹配逻辑
            if matched_name is None:
                choices = list(network_channels.keys())
                matches = process.extract(key, choices, scorer=fuzz.ratio, limit=1)

                if matches:
                    best_key, score, _ = matches[0]
                    if score > 90:
                        matched_name = clean_network_std_name(network_channels[best_key])
                        match_info = "模糊匹配（>90%）"
                        match_score = float(score)
                        fuzzy_match_count += 1
                        add_channel_data_if_not_exists(original_name, matched_name, "待确认分组")
                    else:
                        matched_name = original_name
                        add_channel_data_if_not_exists(original_name, matched_name, "待标准化")
                else:
                    matched_name = original_name
                    add_channel_data_if_not_exists(original_name, matched_name, "待标准化")

            matched_standard_names.append(matched_name)
            matched_match_info.append(match_info)
            matched_match_score.append(match_score)

        if time.time() - last_print_time >= 5:
            print(f"已处理 {end_idx}/{total_len} 条，精准 {precise_match_count}，模糊 {fuzzy_match_count}")
            last_print_time = time.time()

    # 更新 total_before 表中匹配相关字段
    total_before["频道名"] = matched_standard_names
    total_before["匹配信息"] = matched_match_info
    total_before["匹配值"] = matched_match_score

    # 从 channel_data 映射“分组”字段
    std_name_to_group = dict(zip(channel_data["标准名"], channel_data["分组"]))
    total_before["分组"] = total_before["频道名"].apply(lambda x: std_name_to_group.get(x, "未分类"))

    # 从 channel_data 映射“输出顺序”字段
    total_before["输出顺序"] = total_before["频道名"].apply(lambda x: std_name_to_output_order.get(x, "未排序"))

    # channel_data 去重，保留原始名唯一的第一条
    channel_data = channel_data.drop_duplicates(subset=["原始名"], keep='first')

    print("保存输出文件...")

    # 保存总表 total_final.csv，包含新增的轮回相似度和输出顺序列
    total_before.to_csv(
        OUTPUT_TOTAL_FINAL, index=False, encoding="utf-8-sig",
        columns=[
            "频道名", "地址", "来源", "图标", "检测时间", "分组",
            "视频编码", "分辨率", "帧率", "音频", "相似度", "匹配信息", "匹配值",
            "轮回相似度", "输出顺序"
        ]
    )

    # 保存 channel_data.csv，带新增的三列
    channel_data.to_csv(
        OUTPUT_CHANNEL_DATA, index=False, encoding="utf-8-sig",
        columns=["原始名", "标准名", "拟匹配频道名", "分组", "来源", "输出顺序", "是否已维护"]
    )

    print("🎉 CSV 标准化处理完成！")

if __name__ == "__main__":
    main()
