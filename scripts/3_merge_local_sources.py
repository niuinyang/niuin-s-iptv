#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import chardet
import platform
import pandas as pd

networksource_dir = "input/download/net/txt"
mysource_dir = "input/download/my"

OUTPUT_DIR = "output"
LOG_DIR = os.path.join(OUTPUT_DIR, "log")
MERGE_DIR = os.path.join(OUTPUT_DIR, "middle/merge")
LOG_MERGE_DIR = os.path.join(LOG_DIR, "merge")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(MERGE_DIR, exist_ok=True)
os.makedirs(LOG_MERGE_DIR, exist_ok=True)

NETWORK_M3U = os.path.join(MERGE_DIR, "networksource_total.m3u")
NETWORK_CSV = os.path.join(MERGE_DIR, "networksource_total.csv")
NETWORK_LOG = os.path.join(LOG_MERGE_DIR, "networksource_skipped.log")

MYSOURCE_M3U = os.path.join(MERGE_DIR, "mysource_total.m3u")
MYSOURCE_CSV = os.path.join(MERGE_DIR, "mysource_total.csv")
MYSOURCE_LOG = os.path.join(LOG_MERGE_DIR, "mysource_skipped.log")

SOURCE_MAPPING = {
    "1sddxzb.m3u": "济南电信组播",
    "2sddxdb.m3u": "济南电信单播",
    "3jnltzb.m3u": "济南联通组播",
    "4sdqdlt.m3u": "青岛联通单播",
    "5sdyd_ipv6.m3u": "山东移动单播",
    "6shyd_ipv6.m3u": "上海移动单播",
}

def safe_open(file_path):
    with open(file_path, 'rb') as f:
        raw = f.read()
        enc = chardet.detect(raw)['encoding'] or 'utf-8'
    try:
        text = raw.decode(enc, errors='ignore')
    except:
        text = raw.decode('utf-8', errors='ignore')
    text = text.replace('\x00', '')
    return text.splitlines()

def read_m3u_file(file_path: str):
    channels = []
    try:
        lines = safe_open(file_path)
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("#EXTINF:"):
                info_line = line
                url_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

                content = info_line.replace("#EXTINF:-1", "").replace("#EXTINF:", "").strip()

                attributes = dict(re.findall(r'(\w+)=["]([^"]*)["]', content))

                resolution = attributes.get("resolution", "")

                temp = re.sub(r'\w+="[^"]*"', '', content).strip()
                if "," in temp:
                    display_name = temp.split(",", 1)[1].strip()
                else:
                    display_name = temp.strip()

                channels.append({
                    "频道名": display_name,
                    "地址": url_line,
                    "tvg-id": attributes.get("tvg-id", ""),
                    "tvg-name": attributes.get("tvg-name", ""),
                    "国家分组": attributes.get("tvg-country", ""),
                    "语言分组": attributes.get("tvg-language", ""),
                    "图标": attributes.get("tvg-logo", ""),
                    "原分组": attributes.get("group-title", ""),
                    "分辨率": resolution,
                })
                i += 2
            else:
                i += 1

        print(f"📡 M3U已加载 {os.path.basename(file_path)}: {len(channels)} 条频道")
        return channels

    except Exception as e:
        print(f"⚠️ 读取M3U失败: {file_path}: {e}")
        return []

def read_txt_9_column(file_path: str):
    channels = []
    try:
        lines = safe_open(file_path)
        for ln in lines:
            ln = ln.strip()
            if not ln or "#genre#" in ln:
                continue
            parts = ln.split(",")
            if len(parts) < 9:
                continue

            (
                display_name,
                url,
                logo,
                tvg_name,
                country_group,
                lang_group,
                group,
                tvg_id,
                resolution,
            ) = [x.strip() for x in parts[:9]]

            if not (url.startswith("http://") or url.startswith("https://") or url.startswith("rtsp://")):
                continue

            channels.append({
                "频道名": display_name,
                "地址": url,
                "图标": logo,
                "tvg-name": tvg_name,
                "国家分组": country_group,
                "语言分组": lang_group,
                "原分组": group,
                "tvg-id": tvg_id,
                "分辨率": resolution,
            })

        print(f"📡 TXT已加载 {os.path.basename(file_path)}: {len(channels)} 条频道")
        return channels

    except Exception as e:
        print(f"⚠️ 读取TXT失败: {file_path}: {e}")
        return []

def merge_all_sources(source_dir, is_m3u=False):
    all_channels = []

    if not os.path.exists(source_dir):
        print(f"⚠️ 源目录不存在: {source_dir}")
        return []

    print(f"📂 扫描目录: {source_dir}")

    for file in os.listdir(source_dir):
        file_path = os.path.join(source_dir, file)
        if is_m3u and file.endswith(".m3u"):
            chs = read_m3u_file(file_path)
        elif not is_m3u and file.endswith(".txt"):
            chs = read_txt_9_column(file_path)
        else:
            continue

        for ch in chs:
            ch["来源文件"] = file

        all_channels.extend(chs)

    print(f"\n📊 合并所有频道，共 {len(all_channels)} 条")
    return all_channels

def write_output_files(channels, output_m3u, output_csv, skipped_log):
    seen_urls = set()
    valid = []
    skipped = []

    for ch in channels:
        url = ch["地址"].strip()
        if url in seen_urls:
            skipped.append((ch["频道名"], url, "重复URL"))
            continue
        seen_urls.add(url)

        valid.append(ch)

    print(f"✅ 有效频道: {len(valid)} 条")
    print(f"🚫 跳过: {len(skipped)} 条")

    with open(output_m3u, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in valid:
            extinf_attrs = []
            if ch.get("tvg-id"):
                extinf_attrs.append(f'tvg-id="{ch["tvg-id"]}"')
            if ch.get("tvg-name"):
                extinf_attrs.append(f'tvg-name="{ch["tvg-name"]}"')
            if ch.get("图标"):
                extinf_attrs.append(f'tvg-logo="{ch["图标"]}"')
            if ch.get("原分组"):
                extinf_attrs.append(f'group-title="{ch["原分组"]}"')
            if ch.get("分辨率"):
                extinf_attrs.append(f'resolution="{ch["分辨率"]}"')

            extinf_str = " ".join(extinf_attrs)
            f.write(f'#EXTINF:-1 {extinf_str},{ch["频道名"]}\n{ch["地址"]}\n')

    df = pd.DataFrame(valid)
    df = df[["频道名", "地址", "tvg-id", "tvg-name", "国家分组", "语言分组",
             "图标", "原分组", "分辨率", "来源文件"]]
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    with open(skipped_log, "w", encoding="utf-8") as f:
        f.write("频道名,地址,跳过原因\n")
        for name, url, rsn in skipped:
            f.write(f"{name},{url},{rsn}\n")

    print(f"📁 输出：{output_m3u} / {output_csv}")
    print(f"📁 跳过日志：{skipped_log}")

if __name__ == "__main__":
    print(f"🔧 当前系统: {platform.system()}，输出统一为 UTF-8")

    channels = merge_all_sources(networksource_dir, is_m3u=False)
    if channels:
        write_output_files(channels, NETWORK_M3U, NETWORK_CSV, NETWORK_LOG)
    else:
        print("⚠️ 没有读取到任何网络源频道")

    channels_my = merge_all_sources(mysource_dir, is_m3u=True)
    if channels_my:
        write_output_files(channels_my, MYSOURCE_M3U, MYSOURCE_CSV, MYSOURCE_LOG)
    else:
        print(f"⚠️ 没有读取到任何 M3U 源频道：{mysource_dir}")
