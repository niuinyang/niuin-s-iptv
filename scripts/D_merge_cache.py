#!/usr/bin/env python3
import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # 用于处理时区（Python 3.9+）

# 缓存数据所在目录，存放按日期划分的子目录
CACHE_DIR = "output/cache/chunk"

# 合并后总缓存文件路径
TOTAL_CACHE_FILE = "output/cache/total_cache.json"

# 记录上次合并日期的文件路径（仅作记录，不再阻断合并）
MERGE_RECORD_FILE = "output/cache/merge_record.json"

# 预定义的时间点顺序，用于排序输出中的时间点
TIME_KEYS = ["0811", "1612", "2113"]


def load_merge_record():
    if os.path.exists(MERGE_RECORD_FILE):
        with open(MERGE_RECORD_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_merge_record(record):
    with open(MERGE_RECORD_FILE, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)


def get_beijing_today():
    tz = ZoneInfo("Asia/Shanghai")
    return datetime.now(tz)


def merge_caches():
    today = get_beijing_today()

    if not os.path.exists(CACHE_DIR):
        print("缓存目录不存在，退出。")
        return

    all_dates = sorted([d for d in os.listdir(CACHE_DIR) if d.isdigit()])

    # 最近三天（含今天）
    recent_3days = [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(3)]

    # 实际存在的近三天目录
    recent_exist_dates = [d for d in recent_3days if d in all_dates]

    if not recent_exist_dates:
        print("无近三天缓存目录，无需合并。")
        return

    # ✅ 核心修改：每次都合并近三天（不再判断是否“新增日期”）
    dates_to_merge = recent_exist_dates

    # 读取已有总缓存
    if os.path.exists(TOTAL_CACHE_FILE):
        with open(TOTAL_CACHE_FILE, "r", encoding="utf-8") as f:
            merged = json.load(f)
    else:
        merged = {}

    # 删除超过 3 天的历史数据
    for url in list(merged.keys()):
        date_dict = merged[url]
        for date_key in list(date_dict.keys()):
            if date_key not in recent_3days:
                del date_dict[date_key]
        if not date_dict:
            del merged[url]

    # 合并近三天 chunk
    for date_dir in dates_to_merge:
        cache_path = os.path.join(CACHE_DIR, date_dir)
        if not os.path.exists(cache_path):
            continue

        for fname in os.listdir(cache_path):
            if not fname.endswith("_cache.json"):
                continue

            full_path = os.path.join(cache_path, fname)

            try:
                with open(full_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"读取失败 {full_path}: {e}")
                continue

            for url, timepoint_data in data.items():
                merged.setdefault(url, {})
                merged[url].setdefault(date_dir, {})

                for timepoint, hashes in timepoint_data.items():
                    merged[url][date_dir][timepoint] = {
                        "phash": hashes.get("phash"),
                        "ahash": hashes.get("ahash"),
                        "dhash": hashes.get("dhash"),
                        "error": hashes.get("error"),
                    }

    # 排序输出
    sorted_merged = {}
    for url in sorted(merged.keys()):
        ordered_date_dict = {}
        for date_key in sorted(merged[url].keys()):
            ordered_timepoint = {}
            for tk in TIME_KEYS:
                if tk in merged[url][date_key]:
                    ordered_timepoint[tk] = merged[url][date_key][tk]
            ordered_date_dict[date_key] = ordered_timepoint
        sorted_merged[url] = ordered_date_dict

    os.makedirs(os.path.dirname(TOTAL_CACHE_FILE), exist_ok=True)

    with open(TOTAL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted_merged, f, ensure_ascii=False, indent=2)

    # 仅记录最近一次合并日期（不作为判断条件）
    merge_record = load_merge_record()
    merge_record["last_merged_date"] = recent_exist_dates[-1]
    save_merge_record(merge_record)

    print(f"合并完成 → {TOTAL_CACHE_FILE}，处理日期：{', '.join(dates_to_merge)}")


def main():
    merge_caches()


if __name__ == "__main__":
    main()
