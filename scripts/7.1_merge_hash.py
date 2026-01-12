#!/usr/bin/env python3
import os
import json
import argparse
from datetime import datetime, timezone, timedelta
import subprocess

CHUNK_DIR_DEFAULT = "output/hash/chunk"
MERGE_DIR_DEFAULT = "output/hash/merge"


def get_now_tag():
    """生成时间点标签 YYMMDDHHMM（北京时间，UTC+8）"""
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


def validate_and_fix_entry(data):
    """
    验证并规范单个 URL 对应的哈希字段结构。
    确保主要哈希字段存在且为列表类型，若缺失则初始化为空列表。
    """
    fields = ["phash", "ahash", "dhash", "whash"]
    for field in fields:
        if field not in data or not isinstance(data[field], list):
            data[field] = []
    return data


def main(args):
    chunk_dir = args.chunk_dir
    merge_dir = args.merge_dir
    time_tag = args.time_tag or get_now_tag()

    print(f"🕒 合并时间标签: {time_tag}")

    if not os.path.exists(chunk_dir):
        print(f"❌ chunk目录不存在: {chunk_dir}")
        return

    files = [f for f in os.listdir(chunk_dir) if f.endswith(".json")]
    if not files:
        print("❌ chunk目录没有发现任何 JSON 文件")
        return

    merged_data = {}

    for fname in files:
        fpath = os.path.join(chunk_dir, fname)
        print(f"  ↳ 处理文件: {fname}")
        chunk_data = load_json_safe(fpath)
        if not chunk_data:
            print(f"    ⚠️ 文件为空或无效，跳过: {fname}")
            continue

        for url, data in chunk_data.items():
            data = validate_and_fix_entry(data)  # 校验并补全字段
            # 直接合并，后面覆盖前面相同 URL 数据
            merged_data[url] = data

    # 输出目录和文件名
    os.makedirs(merge_dir, exist_ok=True)
    output_file = os.path.join(merge_dir, f"{time_tag}-hash-merge.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)

    print(f"✅ 合并完成，文件保存到: {output_file}")

    # git add 合并结果文件
    try:
        subprocess.run(["git", "add", output_file], check=True)
        print(f"✅ 已 git add 文件: {output_file}")
    except Exception as e:
        print(f"⚠️ git add 失败: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="合并 chunk 文件生成单个大 JSON 文件，并 git add"
    )
    parser.add_argument(
        "--chunk-dir",
        default=CHUNK_DIR_DEFAULT,
        help="chunk JSON 文件目录"
    )
    parser.add_argument(
        "--merge-dir",
        default=MERGE_DIR_DEFAULT,
        help="合并结果保存目录"
    )
    parser.add_argument(
        "--time-tag",
        help="时间标签（YYMMDDHHMM），默认取当前北京时间"
    )
    args = parser.parse_args()
    main(args)