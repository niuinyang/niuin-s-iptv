#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv

INPUT_DIR = "input/download/net/original"
OUTPUT_DIR = "input/download/net/txt"

OUTPUT_FIELDS = [
    "display_name",
    "url",
    "tvg_logo",
    "tvg_name",
    "tvg_country",
    "tvg_language",
    "group_title",
    "tvg_id",       # 新增第8列
    "resolution"    # 新增第9列
]

OUTPUT_HEADER = [
    "频道名",
    "地址",
    "logo",
    "tvg频道名",
    "tvg国家",
    "tcg语言",
    "tvg分组",
    "tvg-id",       # 新增列名
    "resolution"    # 新增列名
]


def safe_open(file_path):
    with open(file_path, "rb") as f:
        raw = f.read()

    for enc in ["utf-8", "utf-8-sig", "gb18030"]:
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("utf-8", errors="replace")

    text = text.replace("\x00", "")
    return text.splitlines()


def parse_m3u(lines):
    channels = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXTINF:"):
            info_line = line

            display_name = ""
            if ',' in info_line:
                display_name = info_line.split(',', 1)[1].strip()

            def extract_attr(attr):
                m = re.search(r'%s="([^"]*)"' % attr, info_line)
                return m.group(1).strip() if m else ""

            tvg_name = extract_attr("tvg-name")
            tvg_country = extract_attr("tvg-country")
            tvg_language = extract_attr("tvg-language")
            tvg_logo = extract_attr("tvg-logo")
            group_title = extract_attr("group-title")
            tvg_id = extract_attr("tvg-id")          # 新增
            resolution = extract_attr("resolution")  # 新增

            url = ""
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and not next_line.startswith("#"):
                    url = next_line

            channels.append({
                "display_name": display_name,
                "url": url,
                "tvg_logo": tvg_logo,
                "tvg_name": tvg_name,
                "tvg_country": tvg_country,
                "tvg_language": tvg_language,
                "group_title": group_title,
                "tvg_id": tvg_id,             # 新增
                "resolution": resolution      # 新增
            })
            i += 2
        else:
            i += 1
    return channels


def is_m3u_format(lines):
    for line in lines:
        if line.strip().startswith("#EXTINF:"):
            return True
    return False


def parse_txt(lines):
    datetime_pattern = re.compile(r"^\d{8} \d{2}:\d{2}$")
    channels = []
    sample = "\n".join(lines[:10])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
    except csv.Error:
        dialect = csv.excel

    reader = csv.reader(lines, dialect)
    for row in reader:
        if not row:
            continue
        first_col = row[0].strip().lower()
        if ("更新时间" in first_col) or ("#genre#" in first_col):
            continue
        if datetime_pattern.match(row[0].strip()):
            continue

        # 补足9列，不足用空字符串填充
        row += [""] * (9 - len(row))

        ch = {
            "display_name": row[0].strip(),
            "url": row[1].strip(),
            "tvg_logo": row[2].strip(),
            "tvg_name": row[3].strip(),
            "tvg_country": row[4].strip(),
            "tvg_language": row[5].strip(),
            "group_title": row[6].strip(),
            "tvg_id": row[7].strip(),        # 新增
            "resolution": row[8].strip()     # 新增
        }
        channels.append(ch)
    return channels


def process_file(file_path):
    lines = safe_open(file_path)
    if not lines:
        return []

    if is_m3u_format(lines):
        channels = parse_m3u(lines)
        print(f"解析 {os.path.basename(file_path)} 作为 M3U 格式，提取 {len(channels)} 条频道")
    else:
        channels = parse_txt(lines)
        print(f"解析 {os.path.basename(file_path)} 作为 TXT 格式，提取 {len(channels)} 条频道")
    return channels


def save_channels_to_txt(channels, output_file):
    with open(output_file, "w", encoding="utf-8-sig", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_HEADER)
        for ch in channels:
            row = [ch.get(field, "") for field in OUTPUT_FIELDS]
            writer.writerow(row)


def main():
    if not os.path.exists(INPUT_DIR):
        print(f"输入目录不存在: {INPUT_DIR}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for filename in os.listdir(INPUT_DIR):
        if not (filename.endswith(".m3u") or filename.endswith(".txt")):
            continue

        file_path = os.path.join(INPUT_DIR, filename)
        channels = process_file(file_path)

        if not channels:
            print(f"文件 {filename} 无有效频道，跳过输出。")
            continue

        base_name = os.path.splitext(filename)[0]
        output_file = os.path.join(OUTPUT_DIR, f"{base_name}.txt")

        save_channels_to_txt(channels, output_file)
        print(f"已输出文件 {output_file}，包含 {len(channels)} 条频道")


if __name__ == "__main__":
    main()
