#!/usr/bin/env python3
import os
import json
import argparse
from datetime import datetime, timezone, timedelta

CHUNK_DIR_DEFAULT = "output/hash/chunk"
MERGE_DIR_DEFAULT = "output/hash/merge"

def get_now_tag():
    beijing_tz = timezone(timedelta(hours=8))
    return datetime.now(beijing_tz).strftime("%y%m%d%H%M")

def load_json_safe(path):
    if not os.path.exists(path):
        print(f"⚠️ 文件不存在: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ JSON解析失败，跳过文件: {path}")
            return {}

def main(args):
    chunk_dir = args.chunk_dir
    merge_dir = args.merge_dir
    time_tag = args.time_tag or get_now_tag()

    print(f"🕒 本次检测时间点: {time_tag}")

    if not os.path.exists(chunk_dir):
        print(f"⚠️ chunk目录不存在: {chunk_dir}")
        return

    files = [f for f in os.listdir(chunk_dir) if f.endswith(".json")]
    if not files:
        print("⚠️ chunk 目录为空，未发现可合并文件")
        return

    print(f"ℹ️ 发现 {len(files)} 个 chunk 文件，开始合并...")

    merged_data = {}

    for fname in files:
        fpath = os.path.join(chunk_dir, fname)
        print(f"  ↳ 处理文件: {fname}")
        chunk_data = load_json_safe(fpath)
        if not chunk_data:
            print(f"    ⚠️ 文件为空或无效，跳过: {fname}")
            continue

        for url, result in chunk_data.items():
            merged_data[url] = result  # 仅合并本次，不与历史合并

    os.makedirs(merge_dir, exist_ok=True)

    merge_file = os.path.join(merge_dir, f"{time_tag}-hash-merge.json")

    with open(merge_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)

    print(f"✅ 合并完成，结果写入: {merge_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="合并 hash chunk 文件生成本次合并文件")
    parser.add_argument("--chunk-dir", default=CHUNK_DIR_DEFAULT, help="hash chunk 目录")
    parser.add_argument("--merge-dir", default=MERGE_DIR_DEFAULT, help="hash 合并输出目录")
    parser.add_argument("--time-tag", help="手动指定时间标签，格式 yyMMddHHmm")

    args = parser.parse_args()
    main(args)