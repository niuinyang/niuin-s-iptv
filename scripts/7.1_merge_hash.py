#!/usr/bin/env python3
import os
import json
import argparse
from datetime import datetime, timezone, timedelta

CHUNK_DIR_DEFAULT = "output/hash/chunk"
TOTAL_FILE_DEFAULT = "output/hash/hash_total.json"
MAX_HISTORY = 6


def get_now_tag():
    """生成时间点标签 YYYYMMDDHHMM（北京时间，UTC+8）"""
    beijing_tz = timezone(timedelta(hours=8))
    return datetime.now(beijing_tz).strftime("%Y%m%d%H%M")


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
    total_file = args.total_file
    time_tag = args.time_tag or get_now_tag()

    print(f"🕒 本次检测时间点: {time_tag}")

    # 1. 读取已有 hash_total.json
    total_data = load_json_safe(total_file)

    # 2. 遍历 chunk 目录
    if not os.path.exists(chunk_dir):
        print(f"⚠️ chunk目录不存在: {chunk_dir}")
        return

    files = [
        f for f in os.listdir(chunk_dir)
        if f.endswith(".json")
    ]

    if not files:
        print("⚠️ chunk 目录为空，未发现可合并文件")
        return

    print(f"ℹ️ 发现 {len(files)} 个 chunk 文件，开始合并...")

    for fname in files:
        fpath = os.path.join(chunk_dir, fname)
        print(f"  ↳ 处理文件: {fname}")
        chunk_data = load_json_safe(fpath)
        if not chunk_data:
            print(f"    ⚠️ 文件为空或无效，跳过: {fname}")
            continue

        for url, result in chunk_data.items():
            # 初始化 URL 节点
            if url not in total_data:
                total_data[url] = {}

            # 写入当前时间点
            total_data[url][time_tag] = result

            # 超过最大历史数量，删除最早的
            if len(total_data[url]) > MAX_HISTORY:
                sorted_keys = sorted(total_data[url].keys())
                for old_key in sorted_keys[:-MAX_HISTORY]:
                    del total_data[url][old_key]

    # 3. 确保输出目录存在
    os.makedirs(os.path.dirname(total_file), exist_ok=True)

    # 4. 写回 hash_total.json
    with open(total_file, "w", encoding="utf-8") as f:
        json.dump(total_data, f, indent=2, ensure_ascii=False)

    print(f"✅ 合并完成，结果写入: {total_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="合并 hash chunk 文件，生成多时间点 hash 历史"
    )
    parser.add_argument(
        "--chunk-dir",
        default=CHUNK_DIR_DEFAULT,
        help="hash chunk 目录"
    )
    parser.add_argument(
        "--total-file",
        default=TOTAL_FILE_DEFAULT,
        help="hash_total.json 输出路径"
    )
    parser.add_argument(
        "--time-tag",
        help="手动指定时间点（YYYYMMDDHHMM），不指定则使用当前时间"
    )

    args = parser.parse_args()
    main(args)
