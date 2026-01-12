#!/usr/bin/env python3
import os
import json
import csv
import argparse
import logging
from collections import defaultdict, Counter
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

HASH_TYPES = ['phash', 'ahash', 'dhash', 'whash']
MAX_HISTORY = 6
FAKE_THRESHOLD = 7
LOOP_THRESHOLD = 6
THREAD_WORKERS = 6

def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=level)

def parse_datetime_from_filename(filename):
    base = os.path.basename(filename)
    prefix = base.split('-')[0]
    if len(prefix) != 10:
        return None
    try:
        year = int('20' + prefix[0:2])
        month = int(prefix[2:4])
        day = int(prefix[4:6])
        hour = int(prefix[6:8])
        minute = int(prefix[8:10])
        return datetime(year, month, day, hour, minute)
    except Exception as e:
        logging.warning(f"Failed parse datetime from filename {filename}: {e}")
        return None

def load_json_file(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        logging.error(f"Failed to load json {path}: {e}")
        return {}

def load_recent_hash_files(hash_dir, max_history):
    # 找出所有json文件，按时间倒序排序，取最多max_history个
    files = [os.path.join(hash_dir, f) for f in os.listdir(hash_dir) if f.endswith('.json')]
    files = [(f, parse_datetime_from_filename(f)) for f in files]
    files = [f for f in files if f[1] is not None]
    files.sort(key=lambda x: x[1], reverse=True)
    recent_files = files[:max_history]

    logging.info(f"Found {len(recent_files)} recent hash json files.")

    hash_history = defaultdict(lambda: defaultdict(list))

    # 多线程读取文件
    with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as executor:
        future_to_file = {executor.submit(load_json_file, f[0]): f for f in recent_files}
        # 为保证顺序按时间升序合并，先收集结果再按时间顺序处理
        file_data_map = {}
        for future in as_completed(future_to_file):
            f, dt = future_to_file[future]
            data = future.result()
            file_data_map[(f, dt)] = data

    # 按时间升序写入hash_history（方便历史一致性判断）
    for f, dt in sorted(file_data_map.keys(), key=lambda x: x[1]):
        data = file_data_map[(f, dt)]
        for addr, hdict in data.items():
            for htype in HASH_TYPES:
                vals = hdict.get(htype, [None, None, None])
                hash_history[addr][htype].append(vals)

    logging.info(f"Loaded hash history for {len(hash_history)} addresses.")
    return hash_history

def parse_resolution(res_str):
    try:
        w,h = res_str.lower().split('x')
        return (int(w), int(h))
    except Exception:
        return (0, 0)

def load_deep_csv(deep_csv_path):
    deep_data = {}
    try:
        with open(deep_csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                addr = row['地址']
                deep_data[addr] = {
                    'ffprobe_success': row.get('ffprobe是否成功', '') == '成功',
                    'fps': float(row.get('帧率', '0')),
                    'resolution': parse_resolution(row.get('分辨率', '0x0')),
                    'has_audio': row.get('音频', '') != '',
                }
        logging.info(f"Loaded deep info for {len(deep_data)} addresses.")
    except Exception as e:
        logging.error(f"Failed to load deep CSV {deep_csv_path}: {e}")
    return deep_data

def load_fake_hash_db(fake_hash_path):
    if not os.path.exists(fake_hash_path):
        logging.info(f"Fake hash db not found at {fake_hash_path}, initializing new.")
        return {htype:{} for htype in HASH_TYPES}
    try:
        with open(fake_hash_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Ensure all hash types keys present
            for htype in HASH_TYPES:
                if htype not in data:
                    data[htype] = {}
            logging.info(f"Loaded fake hash db with {sum(len(data[ht]) for ht in HASH_TYPES)} entries.")
            return data
    except Exception as e:
        logging.error(f"Failed to load fake hash db: {e}")
        return {htype:{} for htype in HASH_TYPES}

def save_fake_hash_db(fake_hash_db, fake_hash_path):
    try:
        with open(fake_hash_path, 'w', encoding='utf-8') as f:
            json.dump(fake_hash_db, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved fake hash db with {sum(len(fake_hash_db[ht]) for ht in HASH_TYPES)} entries.")
    except Exception as e:
        logging.error(f"Failed to save fake hash db: {e}")

def update_fake_hash_db(hash_history, fake_hash_db, min_addr_count=3, min_time_count=3):
    """
    统计hash跨地址和跨时间点出现次数，满足阈值的hash加入典型假源库
    """
    # 结构 {htype: {hash: set(addresses)}}
    hash_addr_map = {htype:defaultdict(set) for htype in HASH_TYPES}
    # 结构 {htype: Counter(hash: count_times_appeared)}
    hash_time_count = {htype:Counter() for htype in HASH_TYPES}

    for addr, htypes in hash_history.items():
        for htype, history_list in htypes.items():
            for hvals in history_list:
                unique_hashes = set([h for h in hvals if h])
                for h in unique_hashes:
                    hash_addr_map[htype][h].add(addr)
                    hash_time_count[htype][h] += 1

    added = 0
    for htype in HASH_TYPES:
        for h, addrs in hash_addr_map[htype].items():
            time_cnt = hash_time_count[htype][h]
            if len(addrs) >= min_addr_count and time_cnt >= min_time_count:
                if h not in fake_hash_db[htype]:
                    fake_hash_db[htype][h] = {
                        'count': time_cnt,
                        'addresses': list(addrs)
                    }
                    added += 1
                else:
                    # 更新已有条目
                    fake_hash_db[htype][h]['count'] = max(fake_hash_db[htype][h]['count'], time_cnt)
                    fake_hash_db[htype][h]['addresses'] = list(set(fake_hash_db[htype][h]['addresses']) | addrs)

    logging.info(f"Added/updated {added} typical fake hashes to the fake hash db.")
    return fake_hash_db

def is_hash_typical_fake(fake_hash_db, htype, h):
    return h in fake_hash_db.get(htype, {})

def hash_all_equal(hash_list):
    return len(hash_list) == 3 and hash_list[0] == hash_list[1] == hash_list[2]

def score_address(addr, hash_history, deep_info, fake_hash_db):
    score_fake = 0
    score_loop = 0

    htypes = hash_history.get(addr, {})
    deep = deep_info.get(addr, {'ffprobe_success':False,'fps':0,'resolution':(0,0),'has_audio':False})

    if not htypes:
        return 0, 0

    # 最近一次检测hash（历史最后一个）
    latest_hashes = {}
    for htype in HASH_TYPES:
        hlist = htypes.get(htype, [])
        if hlist:
            latest_hashes[htype] = hlist[-1]
        else:
            latest_hashes[htype] = [None, None, None]

    # A. 单次检测判定
    # A1: 单次2/5/20s同hash
    for htype, hashes in latest_hashes.items():
        if hashes and None not in hashes and hash_all_equal(hashes):
            score_fake += 2
            score_loop += 2

    # A2: 4种hash同时全部相同
    if all(htype in latest_hashes and latest_hashes[htype] and None not in latest_hashes[htype] and hash_all_equal(latest_hashes[htype]) for htype in HASH_TYPES):
        score_fake += 4
        score_loop += 4

    # B. 多次检测历史一致性 (只看2s点hash)
    for htype, history_list in htypes.items():
        if len(history_list) < 2:
            continue
        first_hashes = [hvals[0] for hvals in history_list if hvals and hvals[0]]
        if len(first_hashes) < 2:
            continue
        base_hash = first_hashes[0]
        same_count = sum(1 for h in first_hashes[1:] if h == base_hash)
        if same_count >= 4:
            score_fake += 7
            score_loop += 6
        elif same_count >= 2:
            score_fake += 3
            score_loop += 3

    # C. 多地址共性判定
    for htype, hashes in latest_hashes.items():
        for h in hashes:
            if h and is_hash_typical_fake(fake_hash_db, htype, h):
                score_fake += 10
                score_loop += 8

    # D. deep_total_ok辅助扣分
    if deep.get('ffprobe_success'):
        score_fake -= 1
        score_loop -= 1
    if deep.get('fps', 0) >= 25:
        score_fake -= 1
        score_loop -= 1
    if deep.get('resolution', (0,0))[0] >= 720:
        score_fake -= 1
        score_loop -= 1
    if deep.get('has_audio', False):
        score_fake -= 1
        score_loop -= 1

    # 最小为0
    score_fake = max(0, score_fake)
    score_loop = max(0, score_loop)

    return score_fake, score_loop

def write_output_files(deep_csv_path, output_total, output_fake, output_loop, output_ok, score_map):
    try:
        with open(deep_csv_path, newline='', encoding='utf-8') as f_in, \
             open(output_total, 'w', newline='', encoding='utf-8') as f_total, \
             open(output_fake, 'w', newline='', encoding='utf-8') as f_fake, \
             open(output_loop, 'w', newline='', encoding='utf-8') as f_loop, \
             open(output_ok, 'w', newline='', encoding='utf-8') as f_ok:

            reader = csv.DictReader(f_in)
            fieldnames = reader.fieldnames + ['假源评分', '轮播评分']
            writer_total = csv.DictWriter(f_total, fieldnames=fieldnames)
            writer_fake = csv.DictWriter(f_fake, fieldnames=fieldnames)
            writer_loop = csv.DictWriter(f_loop, fieldnames=fieldnames)
            writer_ok = csv.DictWriter(f_ok, fieldnames=fieldnames)

            writer_total.writeheader()
            writer_fake.writeheader()
            writer_loop.writeheader()
            writer_ok.writeheader()

            for row in reader:
                addr = row['地址']
                fscore, lscore = score_map.get(addr, (0,0))
                row['假源评分'] = fscore
                row['轮播评分'] = lscore
                writer_total.writerow(row)

                if fscore >= FAKE_THRESHOLD:
                    writer_fake.writerow(row)
                elif lscore >= LOOP_THRESHOLD:
                    writer_loop.writerow(row)
                else:
                    writer_ok.writerow(row)

        logging.info(f"Output written: total({output_total}), fake({output_fake}), loop({output_loop}), ok({output_ok})")
    except Exception as e:
        logging.error(f"Error writing output files: {e}")

def parse_args():
    parser = argparse.ArgumentParser(description="假源和轮播判定脚本")
    parser.add_argument('--hash-dir', type=str, default='output/hash/merge', help='hash json文件目录')
    parser.add_argument('--deep-csv', type=str, default='output/middle/deep/deep_total_ok.csv', help='deep CSV路径')
    parser.add_argument('--fake-hash-json', type=str, default='output/hash/fake_hash.json', help='典型假源hash库路径')
    parser.add_argument('--output-total', type=str, default='output/working/working_total.csv', help='输出合并csv')
    parser.add_argument('--output-fake', type=str, default='output/working/working_fake.csv', help='输出假源csv')
    parser.add_argument('--output-loop', type=str, default='output/working/working_loop.csv', help='输出轮播csv')
    parser.add_argument('--output-ok', type=str, default='output/working/working_ok.csv', help='输出正常csv')
    parser.add_argument('--verbose', action='store_true', help='启用调试日志')
    return parser.parse_args()

def main():
    args = parse_args()
    setup_logging(args.verbose)

    logging.info("Start processing")

    hash_history = load_recent_hash_files(args.hash_dir, MAX_HISTORY)
    deep_info = load_deep_csv(args.deep_csv)
    fake_hash_db = load_fake_hash_db(args.fake_hash_json)
    fake_hash_db = update_fake_hash_db(hash_history, fake_hash_db)

    score_map = {}
    for addr in deep_info:
        fscore, lscore = score_address(addr, hash_history, deep_info, fake_hash_db)
        score_map[addr] = (fscore, lscore)

    write_output_files(args.deep_csv, args.output_total, args.output_fake, args.output_loop, args.output_ok, score_map)
    save_fake_hash_db(fake_hash_db, args.fake_hash_json)

    logging.info("Processing finished.")

if __name__ == '__main__':
    main()