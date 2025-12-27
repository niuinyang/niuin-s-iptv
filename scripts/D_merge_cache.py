#!/usr/bin/env python3
import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # 用于处理时区（Python 3.9+）

# 缓存数据所在目录，存放按日期划分的子目录
CACHE_DIR = "output/cache/chunk"

# 合并后总缓存文件路径
TOTAL_CACHE_FILE = "output/cache/total_cache.json"

# 记录上次合并日期的文件路径，防止重复合并
MERGE_RECORD_FILE = "output/cache/merge_record.json"

# 预定义的时间点顺序，用于排序输出中的时间点
TIME_KEYS = ["0811", "1612", "2113"]

# 读取合并记录文件，返回JSON字典（记录上次合并日期）
def load_merge_record():
    if os.path.exists(MERGE_RECORD_FILE):
        with open(MERGE_RECORD_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

# 将合并记录写回文件，传入字典对象
def save_merge_record(record):
    with open(MERGE_RECORD_FILE, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)

# 获取当前北京时间（东八区）
def get_beijing_today():
    tz = ZoneInfo("Asia/Shanghai")              # 创建上海时区对象
    return datetime.now(tz)                      # 返回当前东八区时间

# 核心函数，合并近三天的缓存数据，且只保留最近3天的哈希数据
def merge_caches():
    today = get_beijing_today()                  # 获取当前北京时间（用作“今天”基准）

    # 获取CACHE_DIR下所有目录名，筛选纯数字（日期格式）目录，排序
    all_dates = sorted([d for d in os.listdir(CACHE_DIR) if d.isdigit()])

    # 生成最近3天的日期字符串列表，格式为 YYYYMMDD，包含今天往前两天
    recent_3days = [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(3)]

    # 从实际存在的目录中筛选出属于近3天的日期目录，可能实际存在的少于3个
    recent_exist_dates = [d for d in recent_3days if d in all_dates]

    # 若无近三天的目录，则无须合并，打印提示并退出
    if not recent_exist_dates:
        print("无近三天缓存目录，无需合并。")
        return

    # 读取合并记录文件，获取上次合并的最新日期
    merge_record = load_merge_record()
    last_merged_date = merge_record.get("last_merged_date", "")

    # 筛选出未合并的日期（比上次合并日期晚且在近三天内）
    dates_to_merge = [d for d in recent_exist_dates if (not last_merged_date or d > last_merged_date)]

    # 如果没有新目录需要合并，打印提示并退出
    if not dates_to_merge:
        print("无新增近三天缓存目录，无需合并。")
        return

    # 如果total_cache.json存在，读取已有合并数据，否则初始化空字典
    if os.path.exists(TOTAL_CACHE_FILE):
        with open(TOTAL_CACHE_FILE, "r", encoding="utf-8") as f:
            merged = json.load(f)
    else:
        merged = {}

    # -------- 新增：删除merged中超过3天的日期数据 --------
    for url in list(merged.keys()):
        date_dict = merged[url]
        for date_key in list(date_dict.keys()):
            if date_key not in recent_3days:
                del date_dict[date_key]
        # 删除后如果该url没有任何日期数据，删除该url
        if not date_dict:
            del merged[url]

    # 遍历每个待合并的日期目录
    for date_dir in dates_to_merge:
        cache_path = os.path.join(CACHE_DIR, date_dir)
        if not os.path.exists(cache_path):
            continue  # 如果目录不存在，跳过

        # 遍历目录内所有以"_cache.json"结尾的文件
        for fname in os.listdir(cache_path):
            if not fname.endswith("_cache.json"):
                continue

            full_path = os.path.join(cache_path, fname)

            # 读取单个缓存文件的JSON数据
            with open(full_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # data的结构是：{url: {timepoint: {phash, ahash, dhash, error}}}
            # 遍历所有URL及其对应的时间点数据，逐条合并到merged结构中
            for url, timepoint_data in data.items():
                # 若url未出现过，先初始化字典
                if url not in merged:
                    merged[url] = {}

                # 若该日期未出现过，也先初始化字典
                if date_dir not in merged[url]:
                    merged[url][date_dir] = {}

                # 把当前时间点数据存入对应url、日期的字典中，覆盖已有数据
                for timepoint, hashes in timepoint_data.items():
                    merged[url][date_dir][timepoint] = {
                        "phash": hashes.get("phash"),
                        "ahash": hashes.get("ahash"),
                        "dhash": hashes.get("dhash"),
                        "error": hashes.get("error")
                    }

    # 对合并后的数据做排序处理
    # 第一层：URL 按字典序排序
    # 第二层：日期按升序排序
    # 第三层：时间点按固定顺序 TIME_KEYS 排序
    sorted_merged = {}
    for url in sorted(merged.keys()):
        date_dict = merged[url]
        ordered_date_dict = {}

        # 按日期排序
        for date_key in sorted(date_dict.keys()):
            timepoint_dict = date_dict[date_key]
            ordered_timepoint = {}

            # 按 TIME_KEYS 预定义顺序挑选时间点，保证输出顺序一致
            for tk in TIME_KEYS:
                if tk in timepoint_dict:
                    ordered_timepoint[tk] = {
                        "phash": timepoint_dict[tk].get("phash"),
                        "ahash": timepoint_dict[tk].get("ahash"),
                        "dhash": timepoint_dict[tk].get("dhash"),
                        "error": timepoint_dict[tk].get("error"),
                    }

            # 每个日期对应的时间点数据写入字典
            ordered_date_dict[date_key] = ordered_timepoint

        # 该url的完整排序数据写入结果
        sorted_merged[url] = ordered_date_dict

    # 确保输出文件夹存在
    os.makedirs(os.path.dirname(TOTAL_CACHE_FILE), exist_ok=True)

    # 将排序后的合并数据写入 total_cache.json，格式化缩进，中文不转义
    with open(TOTAL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted_merged, f, ensure_ascii=False, indent=2)

    # 更新合并记录，记录最后合并的最新日期
    merge_record["last_merged_date"] = dates_to_merge[-1]
    save_merge_record(merge_record)

    # 打印合并完成信息，显示最新合并日期
    print(f"合并完成 → {TOTAL_CACHE_FILE}，最新日期：{dates_to_merge[-1]}")

# 程序入口，调用合并函数
def main():
    merge_caches()

if __name__ == "__main__":
    main()
