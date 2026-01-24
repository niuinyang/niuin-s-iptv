#!/usr/bin/env python3
# standardize_iptv.py（CSV 版本，适配 loop_scan_yes + 在线网络库兜底）

import os
import re
import csv
import sys
import time
import chardet
import requests
import pandas as pd
from opencc import OpenCC
from rapidfuzz import fuzz, process
from tqdm import tqdm

# =========================
# 路径配置（★ 修改）
# =========================

MY_SUM_PATH = "output/middle/merge/mysource_total.csv"

# ★ 修改：working 输入改为 loop_scan_yes
WORKING_PATH = "output/middle/loop/loop_scan_yes.csv"

# ★ 修改：channel_data 新路径
CHANNEL_DATA_PATH = "output/middle/channel_data/channel_data.csv"

# ★ 修改：网络频道库（在线 + 本地兜底）
NETWORK_CHANNELS_URL = (
    "https://raw.githubusercontent.com/iptv-org/database/"
    "refs/heads/master/data/channels.csv"
)
NETWORK_CHANNELS_LOCAL = "input/netdata/channels.csv"

OUTPUT_TOTAL_FINAL = "output/total_final.csv"

cc = OpenCC("t2s")

# =========================
# 工具函数
# =========================

def read_csv_auto_encoding(filepath):
    with open(filepath, "rb") as f:
        raw = f.read(10000)
        encoding = chardet.detect(raw)["encoding"] or "utf-8"
    return pd.read_csv(filepath, encoding=encoding)

def mechanical_standardize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = cc.convert(name.strip()).lower()
    s = re.sub(r"[（(【\[].*?[）)】\]]", "", s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^a-z0-9\u4e00-\u9fa5\+！]", "", s)
    return s

def clean_network_std_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = re.sub(r"\s+", " ", name.strip())
    return " ".join(
        w.capitalize() if re.fullmatch(r"[a-zA-Z]+", w) else w
        for w in name.split(" ")
    )

# ★ 修改：网络频道库加载（在线 + 本地兜底）
def load_network_channels():
    try:
        print("🌐 尝试在线获取网络频道数据库...")
        r = requests.get(NETWORK_CHANNELS_URL, timeout=10)
        r.raise_for_status()

        os.makedirs(os.path.dirname(NETWORK_CHANNELS_LOCAL), exist_ok=True)
        with open(NETWORK_CHANNELS_LOCAL, "wb") as f:
            f.write(r.content)

        print("✅ 网络频道数据库已更新")
        return pd.read_csv(NETWORK_CHANNELS_LOCAL)

    except Exception as e:
        print(f"⚠️ 网络失败，使用本地缓存：{e}")
        if os.path.exists(NETWORK_CHANNELS_LOCAL):
            return pd.read_csv(NETWORK_CHANNELS_LOCAL)
        print("❌ 本地频道数据库不存在，终止")
        sys.exit(1)

# =========================
# 主流程
# =========================

def main():
    print("开始读取输入文件...")

    my_sum = read_csv_auto_encoding(MY_SUM_PATH)
    working = read_csv_auto_encoding(WORKING_PATH)

    # =========================
    # 字段兼容处理（★ 修改）
    # =========================

    for df in [my_sum, working]:
        # 来源字段兼容
        if "来源" not in df.columns:
            if "来源文件" in df.columns:
                df["来源"] = df["来源文件"]
            else:
                df["来源"] = ""

    # =========================
    # channel_data 初始化
    # =========================

    if not os.path.exists(CHANNEL_DATA_PATH):
        os.makedirs(os.path.dirname(CHANNEL_DATA_PATH), exist_ok=True)
        pd.DataFrame(
            columns=["原始名", "标准名", "拟匹配频道名", "分组", "来源", "输出顺序", "是否已维护"]
        ).to_csv(CHANNEL_DATA_PATH, index=False, encoding="utf-8-sig")

    channel_data = read_csv_auto_encoding(CHANNEL_DATA_PATH)

    # =========================
    # 来源学习
    # =========================

    source_dict = {}
    for df in [my_sum, working]:
        for _, row in df.iterrows():
            if row.get("频道名") and row.get("来源"):
                source_dict.setdefault(row["频道名"], row["来源"])

    channel_data["来源"] = channel_data.apply(
        lambda r: r["来源"] if r["来源"] else source_dict.get(r["原始名"], ""),
        axis=1,
    )

    channel_data.setdefault("输出顺序", "未排序")
    channel_data.setdefault("是否已维护", "否")

    # =========================
    # 网络频道库
    # =========================

    net_df = load_network_channels()
    name_col = "channel" if "channel" in net_df.columns else "name"
    net_df = net_df.dropna(subset=[name_col])
    net_df["std_key"] = net_df[name_col].apply(mechanical_standardize)
    network_channels = dict(zip(net_df["std_key"], net_df[name_col]))

    # =========================
    # 合并输入源
    # =========================

    for df in [my_sum, working]:
        for col in ["视频编码", "分辨率", "帧率", "音频", "相似度", "检测时间"]:
            df.setdefault(col, "")

    total = pd.concat([my_sum, working], ignore_index=True)
    total["std_key"] = total["频道名"].apply(mechanical_standardize)

    # =========================
    # channel_data 辅助列
    # =========================

    channel_data["原始名_std_key"] = channel_data["原始名"].apply(mechanical_standardize)
    existing_orig_names = set(channel_data["原始名"])

    # =========================
    # 匹配流程
    # =========================

    matched_names = []
    match_info = []
    match_score = []

    def add_channel_data(orig, std, group):
        nonlocal channel_data
        if orig not in existing_orig_names:
            channel_data = pd.concat(
                [
                    channel_data,
                    pd.DataFrame([{
                        "原始名": orig,
                        "标准名": std,
                        "拟匹配频道名": std,
                        "分组": group,
                        "来源": source_dict.get(orig, ""),
                        "输出顺序": "未排序",
                        "是否已维护": "否",
                    }])
                ],
                ignore_index=True,
            )
            existing_orig_names.add(orig)

    print("开始标准化匹配...")

    for _, row in tqdm(total.iterrows(), total=len(total)):
        orig = row["频道名"]
        key = row["std_key"]

        matched = None
        info = "未匹配"
        score = 0.0

        # 精准匹配
        hit = channel_data[channel_data["原始名"] == orig]
        if not hit.empty and hit.iloc[0]["是否已维护"] == "是":
            matched = hit.iloc[0]["标准名"]
            info = "精准匹配"
            score = 100.0

        # 模糊匹配
        if matched is None:
            res = process.extractOne(key, network_channels.keys(), scorer=fuzz.ratio)
            if res and res[1] > 90:
                matched = clean_network_std_name(network_channels[res[0]])
                info = "模糊匹配（>90%）"
                score = float(res[1])
                add_channel_data(orig, matched, "待确认分组")
            else:
                matched = orig
                add_channel_data(orig, orig, "待标准化")

        matched_names.append(matched)
        match_info.append(info)
        match_score.append(score)

    total["频道名"] = matched_names
    total["匹配信息"] = match_info
    total["匹配值"] = match_score

    # =========================
    # 分组 / 输出顺序映射
    # =========================

    std_to_group = dict(zip(channel_data["标准名"], channel_data["分组"]))
    std_to_order = dict(zip(channel_data["标准名"], channel_data["输出顺序"]))

    total["分组"] = total["频道名"].map(std_to_group).fillna("未分类")
    total["输出顺序"] = total["频道名"].map(std_to_order).fillna("未排序")

    channel_data = channel_data.drop_duplicates(subset=["原始名"], keep="first")

    # =========================
    # 保存输出
    # =========================

    total.to_csv(OUTPUT_TOTAL_FINAL, index=False, encoding="utf-8-sig")
    channel_data.to_csv(CHANNEL_DATA_PATH, index=False, encoding="utf-8-sig")

    print("🎉 标准化流程完成")

if __name__ == "__main__":
    main()
