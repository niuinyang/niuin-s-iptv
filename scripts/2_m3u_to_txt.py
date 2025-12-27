#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os              # 导入操作系统接口模块
import re              # 导入正则表达式模块
import csv             # 导入CSV读写模块

INPUT_DIR = "input/download/net/original"   # 输入文件夹路径，存放原始M3U或TXT文件
OUTPUT_DIR = "input/download/net/txt"       # 输出文件夹路径，存放转换后的TXT文件

# 定义输出字段顺序，用于写入CSV文件时列的顺序
OUTPUT_FIELDS = [
    "display_name",    # 频道显示名称
    "url",             # 频道播放地址
    "tvg_logo",        # 频道logo地址
    "tvg_name",        # TVG频道名
    "tvg_country",     # TVG所属国家
    "tvg_language",    # TVG语言
    "group_title",     # 频道分组
    "tvg_id",          # TVG频道ID
    "resolution"       # 视频分辨率
]

# 输出文件的表头中文名称，对应OUTPUT_FIELDS顺序
OUTPUT_HEADER = [
    "频道名",
    "地址",
    "logo",
    "tvg频道名",
    "tvg国家",
    "tcg语言",
    "tvg分组",
    "tvg-id",
    "resolution"
]


def safe_open(file_path):
    """
    以安全方式打开文件，自动尝试多种编码解码，去除空字符，
    返回文本内容按行拆分的列表
    """
    with open(file_path, "rb") as f:
        raw = f.read()  # 读取文件原始二进制内容

    # 尝试多种编码解码，避免乱码
    for enc in ["utf-8", "utf-8-sig", "gb18030"]:
        try:
            text = raw.decode(enc)  # 解码尝试
            break
        except UnicodeDecodeError:
            continue
    else:
        # 所有解码失败时，用utf-8替换错误解码
        text = raw.decode("utf-8", errors="replace")

    text = text.replace("\x00", "")  # 去除文本中所有空字符
    return text.splitlines()         # 按行拆分返回列表


def parse_m3u(lines):
    """
    解析M3U格式文本行，提取频道信息并以字典形式保存到列表
    """
    channels = []       # 用于保存所有频道信息的列表
    i = 0               # 当前行索引
    while i < len(lines):
        line = lines[i].strip()     # 读取并去除行首尾空白
        if line.startswith("#EXTINF:"):   # 找到频道信息行
            info_line = line

            display_name = ""
            if ',' in info_line:
                # 频道名称在逗号后面
                display_name = info_line.split(',', 1)[1].strip()

            # 辅助函数，用正则表达式从info_line提取属性值
            def extract_attr(attr):
                m = re.search(r'%s="([^"]*)"' % attr, info_line)
                return m.group(1).strip() if m else ""

            # 依次提取各个属性
            tvg_name = extract_attr("tvg-name")
            tvg_country = extract_attr("tvg-country")
            tvg_language = extract_attr("tvg-language")
            tvg_logo = extract_attr("tvg-logo")
            group_title = extract_attr("group-title")
            tvg_id = extract_attr("tvg-id")          # 新增字段
            resolution = extract_attr("resolution")  # 新增字段

            url = ""
            # 频道URL通常在下一行，且非注释行
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and not next_line.startswith("#"):
                    url = next_line

            # 组装字典存储该频道信息
            channels.append({
                "display_name": display_name,
                "url": url,
                "tvg_logo": tvg_logo,
                "tvg_name": tvg_name,
                "tvg_country": tvg_country,
                "tvg_language": tvg_language,
                "group_title": group_title,
                "tvg_id": tvg_id,
                "resolution": resolution
            })
            i += 2   # 跳过已处理的URL行
        else:
            i += 1   # 非频道信息行，继续下一行
    return channels


def is_m3u_format(lines):
    """
    判断文件是否为M3U格式，只要有一行以#EXTINF:开头即判断为M3U
    """
    for line in lines:
        if line.strip().startswith("#EXTINF:"):
            return True
    return False


def parse_txt(lines):
    """
    解析TXT格式的CSV文件，提取频道信息并补齐缺失字段
    """
    datetime_pattern = re.compile(r"^\d{8} \d{2}:\d{2}$")  # 匹配形如 20231227 15:30 的时间格式

    channels = []
    sample = "\n".join(lines[:10])  # 取前10行做CSV格式嗅探
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])  # 自动判断分隔符
    except csv.Error:
        dialect = csv.excel  # 默认Excel风格逗号分隔

    reader = csv.reader(lines, dialect)  # 以确定分隔符解析CSV
    for row in reader:
        if not row:
            continue    # 跳过空行
        first_col = row[0].strip().lower()
        # 跳过包含特定关键词的行，如“更新时间”、包含“#genre#”等
        if ("更新时间" in first_col) or ("#genre#" in first_col):
            continue
        # 跳过符合时间格式的行
        if datetime_pattern.match(row[0].strip()):
            continue

        # 补足到9列，不足用空字符串补齐
        row += [""] * (9 - len(row))

        # 构造频道信息字典
        ch = {
            "display_name": row[0].strip(),
            "url": row[1].strip(),
            "tvg_logo": row[2].strip(),
            "tvg_name": row[3].strip(),
            "tvg_country": row[4].strip(),
            "tvg_language": row[5].strip(),
            "group_title": row[6].strip(),
            "tvg_id": row[7].strip(),
            "resolution": row[8].strip()
        }
        channels.append(ch)
    return channels


def process_file(file_path):
    """
    根据文件内容判断格式并解析文件，返回频道列表
    """
    lines = safe_open(file_path)  # 读取并解码文件内容
    if not lines:
        return []

    if is_m3u_format(lines):
        channels = parse_m3u(lines)   # M3U格式解析
        print(f"解析 {os.path.basename(file_path)} 作为 M3U 格式，提取 {len(channels)} 条频道")
    else:
        channels = parse_txt(lines)   # TXT格式解析
        print(f"解析 {os.path.basename(file_path)} 作为 TXT 格式，提取 {len(channels)} 条频道")
    return channels


def save_channels_to_txt(channels, output_file):
    """
    将频道列表写入TXT文件，使用CSV格式并带有表头
    """
    with open(output_file, "w", encoding="utf-8-sig", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_HEADER)   # 写入表头
        for ch in channels:
            # 按照字段顺序提取对应值构成一行
            row = [ch.get(field, "") for field in OUTPUT_FIELDS]
            writer.writerow(row)          # 写入一行频道信息


def main():
    """
    主函数，遍历输入目录中的文件，解析后输出为标准TXT格式文件
    """
    if not os.path.exists(INPUT_DIR):
        print(f"输入目录不存在: {INPUT_DIR}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)  # 创建输出目录（若不存在）

    for filename in os.listdir(INPUT_DIR):
        if not (filename.endswith(".m3u") or filename.endswith(".txt")):
            continue  # 只处理.m3u和.txt文件

        file_path = os.path.join(INPUT_DIR, filename)
        channels = process_file(file_path)   # 解析文件

        if not channels:
            print(f"文件 {filename} 无有效频道，跳过输出。")
            continue

        base_name = os.path.splitext(filename)[0]
        output_file = os.path.join(OUTPUT_DIR, f"{base_name}.txt")

        save_channels_to_txt(channels, output_file)  # 保存解析结果
        print(f"已输出文件 {output_file}，包含 {len(channels)} 条频道")


if __name__ == "__main__":
    main()
