#!/usr/bin/env python3
import csv
import os
import re
import sys
import asyncio
from collections import defaultdict
from urllib.parse import urlparse, urlunparse

# --- 配置 ---
CSV_INPUT = "output/total_final.csv"
LOCAL_PNG_DIR = "depend/png"
OUTPUT_DXL = "output/dxl.m3u"
OUTPUT_SJMZ = "output/sjmz.m3u"

# 分组优先级
GROUP_PRIORITY = [
    "央视频道",
    "4K频道",
    "卫视频道",
    "山东频道",
    "台湾频道",
    "香港频道",
    "澳门频道",
    "国际频道",
    "他省频道",
    "数字频道",
    "直播频道",
    "待确认分组",
    "待标准化"
]

# 来源优先级（仅此两行替换）
SOURCE_PRIORITY_DXL = [
    "济南联通组播",
    "济南电信组播",
    "上海移动单播",
    "济南电信单播",
    "青岛联通单播",
    "网络源"
]  # 东兴里没移动，去除山东移动单播

SOURCE_PRIORITY_SJMZ = [
    "山东移动单播",
    "上海移动单播",
    "济南电信组播",
    "济南电信单播",
    "青岛联通单播",
    "网络源"
]  # 山景明珠联通没调好，暂时去除

# 过滤来源
FILTER_SOURCE = []  # 取消过滤上海移动，保持全部来源

# 默认tv-logo模板链接
TVG_LOGO_TEMPLATE = "https://raw.githubusercontent.com/plsy1/iptv/main/logo/{channel}.png"

# 用于拼音排序
try:
    from pypinyin import lazy_pinyin
except ImportError:
    print("缺少 pypinyin 库，请运行: pip install pypinyin")
    sys.exit(1)


def get_pinyin_key(name: str):
    """获取拼音排序关键字"""
    if re.search(r'[\u4e00-\u9fff]', name):
        return ''.join(lazy_pinyin(name)).lower()
    else:
        return name.lower()


def split_alpha_num(text: str):
    """将文本分成字母部分和数字部分，方便排序"""
    match = re.match(r'^([a-zA-Z]+)(\d*)', text)
    if match:
        alpha = match.group(1).lower()
        num = int(match.group(2)) if match.group(2) else 0
        return (alpha, num)
    return (text.lower(), 0)


def load_local_png_set():
    """扫描本地png目录，返回所有文件名(无后缀小写)集合"""
    if not os.path.exists(LOCAL_PNG_DIR):
        return set()
    files = os.listdir(LOCAL_PNG_DIR)
    pngs = set()
    for f in files:
        if f.lower().endswith(".png"):
            pngs.add(os.path.splitext(f)[0].lower())
    return pngs


def find_local_logo(channel_name, local_png_set):
    """根据频道名找本地图标文件名，忽略大小写"""
    name_lower = channel_name.lower()
    if name_lower in local_png_set:
        return os.path.join(LOCAL_PNG_DIR, channel_name + ".png")
    # 模糊匹配：忽略非字母数字的比较
    pattern = re.compile(r'[^a-z0-9]')
    norm_name = pattern.sub('', name_lower)
    for png_name in local_png_set:
        norm_png = pattern.sub('', png_name)
        if norm_png == norm_name:
            return os.path.join(LOCAL_PNG_DIR, png_name + ".png")
    return None


def make_tvg_logo(channel_name, local_png_set):
    """生成 tvg-logo 字段，先尝试远程模板URL，若本地有则用本地"""
    url = TVG_LOGO_TEMPLATE.format(channel=channel_name)
    local_logo = find_local_logo(channel_name, local_png_set)
    if local_logo:
        return local_logo.replace("\\", "/")
    return url


def construct_catchup(url):
    """
    构造 catchup-source URL
    """
    try:
        parsed = urlparse(url)
        ip = parsed.hostname
        if not ip:
            return None
        if ':' in ip:  # IPv6 不处理
            return None

        path = parsed.path
        # 去除 /import 段
        if path.startswith("/iptv/import"):
            path = path.replace("/iptv/import", "/iptv", 1)

        # 截断路径到第一个 .rsc 结尾
        rsc_pos = path.find(".rsc")
        if rsc_pos != -1:
            path = path[:rsc_pos + 4]

        time_params = "?tvdr={utc:YmdHMS}GMT-{utcend:YmdHMS}GMT"
        new_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            path,
            '',
            time_params[1:],
            ''
        ))
        return new_url
    except Exception:
        return None


def build_extinf_line(csv_row, local_png_set):
    channel = csv_row.get("频道名", "").strip()
    url = csv_row.get("地址", "").strip()
    group = csv_row.get("分组", "").strip()
    source = csv_row.get("来源", "").strip()

    tvg_logo = make_tvg_logo(channel, local_png_set)
    extinf = f'#EXTINF:-1 tvg-name="{channel}" group-title="{group}" tvg-logo="{tvg_logo}"'

    catchup_str = ""
    if source == "济南联通组播":  # 仅此来源添加回看参数
        catchup_url = construct_catchup(url)
        if catchup_url:
            catchup_str = f' catchup="default" catchup-source="{catchup_url}"'

    extinf += catchup_str + f", {channel}"
    return extinf


def parse_csv():
    """读取CSV，返回列表(dict)，过滤来源 + 过滤停用频道"""
    rows = []
    with open(CSV_INPUT, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:

            # ① 过滤来源
            if row.get("来源", "") in FILTER_SOURCE:
                continue

            # ② 过滤分组为“停用”的频道（新增）
            if row.get("分组", "").strip() == "停用":
                continue

            rows.append(row)
    return rows


def channel_sort_key(channel_name):
    pinyin_key = get_pinyin_key(channel_name)
    alpha_num = split_alpha_num(pinyin_key)
    return alpha_num


def group_sort_key(group_name):
    try:
        priority_index = GROUP_PRIORITY.index(group_name)
        return (0, priority_index, "")
    except ValueError:
        pinyin = get_pinyin_key(group_name)
        return (1, len(GROUP_PRIORITY), pinyin)


def source_sort_key(source_name, dxl=True):
    if dxl:
        try:
            return SOURCE_PRIORITY_DXL.index(source_name)
        except ValueError:
            return len(SOURCE_PRIORITY_DXL)
    else:
        try:
            return SOURCE_PRIORITY_SJMZ.index(source_name)
        except ValueError:
            return len(SOURCE_PRIORITY_SJMZ)


def resolution_area(row):
    m = re.match(r'(\d+)[xX](\d+)', row.get("分辨率", ""))
    if m:
        return int(m.group(1)) * int(m.group(2))
    return 0


def safe_float(val, default=0):
    try:
        return float(val)
    except Exception:
        return default


def compute_network_source_score(rows, alpha=0.7, beta=0.3):
    areas = []
    detects = []
    for r in rows:
        area = resolution_area(r)
        detect = safe_float(r.get("检测时间", 0))
        areas.append(area)
        detects.append(detect)

    def normalize(lst):
        if not lst:
            return []
        min_v = min(lst)
        max_v = max(lst)
        if max_v == min_v:
            return [1.0] * len(lst)
        return [(v - min_v) / (max_v - min_v) for v in lst]

    norm_areas = normalize(areas)
    norm_detects = normalize(detects)

    scored_rows = []
    for i, row in enumerate(rows):
        score = alpha * norm_areas[i] - beta * norm_detects[i]
        scored_rows.append((row, score))

    scored_rows.sort(key=lambda x: x[1], reverse=True)
    return [r[0] for r in scored_rows]


def sort_rows(rows, dxl=True):
    group_dict = defaultdict(list)
    for r in rows:
        group_dict[r.get("分组", "待标准化")].append(r)

    sorted_groups = sorted(group_dict.items(), key=lambda x: group_sort_key(x[0]))

    final_sorted = []
    for group, group_rows in sorted_groups:
        channel_dict = defaultdict(list)
        for r in group_rows:
            channel_dict[r.get("频道名", "")].append(r)

        sorted_channels = sorted(channel_dict.items(), key=lambda x: channel_sort_key(x[0]))

        for channel, ch_rows in sorted_channels:
            source_dict = defaultdict(list)
            for rr in ch_rows:
                source_dict[rr.get("来源", "网络源")].append(rr)

            network_source_rows = source_dict.get("网络源", [])
            others_sources = [(s, lst) for s, lst in source_dict.items() if s != "网络源"]

            others_sources_sorted = sorted(
                others_sources,
                key=lambda x: source_sort_key(x[0], dxl=dxl)
            )

            sorted_rows = []
            for s, lst in others_sources_sorted:
                sorted_rows.extend(lst)
            if network_source_rows:
                network_source_rows_sorted = compute_network_source_score(network_source_rows, alpha=0.7, beta=0.3)
                sorted_rows.extend(network_source_rows_sorted)

            final_sorted.extend(sorted_rows)

    return final_sorted


def generate_m3u(rows, output_file, dxl=True, local_png_set=None):
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for row in rows:
            extinf_line = build_extinf_line(row, local_png_set)
            f.write(extinf_line + "\n")
            f.write(row.get("地址", "") + "\n")


async def main():
    print("读取CSV文件...")
    rows = parse_csv()
    print(f"CSV条目数(过滤来源+停用后)：{len(rows)}")

    print("加载本地图标文件...")
    local_png_set = load_local_png_set()

    print("开始排序...")
    rows_dxl = sort_rows(rows, dxl=True)
    rows_sjmz = sort_rows(rows, dxl=False)

    print(f"生成{OUTPUT_DXL}...")
    generate_m3u(rows_dxl, OUTPUT_DXL, dxl=True, local_png_set=local_png_set)
    print(f"生成{OUTPUT_SJMZ}...")
    generate_m3u(rows_sjmz, OUTPUT_SJMZ, dxl=False, local_png_set=local_png_set)

    print("完成。")


if __name__ == "__main__":
    asyncio.run(main())
