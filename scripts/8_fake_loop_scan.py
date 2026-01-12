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

DEFAULT_MAX_HISTORY = 6
DEFAULT_FAKE_THRESHOLD = 7
DEFAULT_LOOP_THRESHOLD = 6
DEFAULT_THREADS = 6
DEFAULT_LOG_LEVEL = "INFO"


def setup_logging(verbose=False, log_level=None):
    if log_level:
        level = getattr(logging, log_level.upper(), logging.INFO)
    else:
        level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=level
    )


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
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load json {path}: {e}")
        return {}


def load_recent_hash_files(hash_dir, max_history, threads):
    files = [os.path.join(hash_dir, f) for f in os.listdir(hash_dir) if f.endswith('.json')]
    files = [(f, parse_datetime_from_filename(f)) for f in files]
    files = [f for f in files if f[1] is not None]
    files.sort(key=lambda x: x[1], reverse=True)

    recent_files = files[:max_history]
    logging.info(f"Using last {len(recent_files)} hash files")

    hash_history = defaultdict(lambda: defaultdict(list))

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(load_json_file, f): (f, dt)
            for f, dt in recent_files
        }

        file_data = {}
        for future in as_completed(futures):
            f, dt = futures[future]
            file_data[(f, dt)] = future.result()

    for f, dt in sorted(file_data.keys(), key=lambda x: x[1]):
        data = file_data[(f, dt)]
        for addr, hdict in data.items():
            for htype in HASH_TYPES:
                vals = hdict.get(htype, [None, None, None])
                hash_history[addr][htype].append(vals)

    logging.info(f"Loaded hash history for {len(hash_history)} addresses")
    return hash_history


def parse_resolution(res_str):
    try:
        w, h = res_str.lower().split('x')
        return int(w), int(h)
    except Exception:
        return 0, 0


def load_deep_csv(deep_csv_path):
    deep_data = {}
    with open(deep_csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            deep_data[row['地址']] = {
                'ffprobe_success': row.get('ffprobe是否成功') == '成功',
                'fps': float(row.get('帧率', '0') or 0),
                'resolution': parse_resolution(row.get('分辨率', '0x0')),
                'has_audio': bool(row.get('音频'))
            }
    logging.info(f"Loaded deep info for {len(deep_data)} addresses")
    return deep_data


def load_fake_hash_db(path):
    if not os.path.exists(path):
        return {h: {} for h in HASH_TYPES}
    with open(path, 'r', encoding='utf-8') as f:
        db = json.load(f)
    for h in HASH_TYPES:
        db.setdefault(h, {})
    return db


def save_fake_hash_db(db, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=2, ensure_ascii=False)


def update_fake_hash_db(hash_history, fake_hash_db, min_addr_count=3, min_time_count=3):
    addr_map = {h: defaultdict(set) for h in HASH_TYPES}
    time_count = {h: Counter() for h in HASH_TYPES}

    for addr, htypes in hash_history.items():
        for htype, history in htypes.items():
            for hvals in history:
                for h in set(filter(None, hvals)):
                    addr_map[htype][h].add(addr)
                    time_count[htype][h] += 1

    added = 0
    for htype in HASH_TYPES:
        for h, addrs in addr_map[htype].items():
            if len(addrs) >= min_addr_count and time_count[htype][h] >= min_time_count:
                if h not in fake_hash_db[htype]:
                    fake_hash_db[htype][h] = {
                        "count": time_count[htype][h],
                        "addresses": list(addrs)
                    }
                    added += 1

    logging.info(f"Updated fake hash db (+{added})")
    return fake_hash_db


def hash_all_equal(h):
    return len(h) == 3 and h[0] == h[1] == h[2]


def score_address(addr, hash_history, deep_info, fake_hash_db):
    fake, loop = 0, 0
    htypes = hash_history.get(addr)
    if not htypes:
        return 0, 0

    latest = {h: htypes[h][-1] for h in HASH_TYPES if htypes.get(h)}

    for hvals in latest.values():
        if hash_all_equal(hvals):
            fake += 2
            loop += 2

    if len(latest) == 4 and all(hash_all_equal(v) for v in latest.values()):
        fake += 4
        loop += 4

    for htype, history in htypes.items():
        base = history[0][0] if history else None
        if base and sum(1 for h in history if h[0] == base) >= 4:
            fake += 7
            loop += 6

    for htype, hvals in latest.items():
        for h in hvals:
            if h and h in fake_hash_db.get(htype, {}):
                fake += 10
                loop += 8

    deep = deep_info.get(addr, {})
    if deep.get('ffprobe_success'):
        fake -= 1
        loop -= 1
    if deep.get('fps', 0) >= 25:
        fake -= 1
        loop -= 1
    if deep.get('resolution', (0, 0))[0] >= 720:
        fake -= 1
        loop -= 1
    if deep.get('has_audio'):
        fake -= 1
        loop -= 1

    return max(0, fake), max(0, loop)


def parse_args():
    p = argparse.ArgumentParser("假源 / 轮播判定")
    p.add_argument('--hash-dir', default='output/hash/merge')
    p.add_argument('--deep-csv', default='output/middle/deep/deep_total_ok.csv')
    p.add_argument('--fake-hash-json', default='output/hash/fake_hash.json')
    p.add_argument('--output-total', default='output/working/working_total.csv')
    p.add_argument('--output-fake', default='output/working/working_fake.csv')
    p.add_argument('--output-loop', default='output/working/working_loop.csv')
    p.add_argument('--output-ok', default='output/working/working_ok.csv')

    p.add_argument('--max-history', type=int, default=DEFAULT_MAX_HISTORY)
    p.add_argument('--threads', type=int, default=DEFAULT_THREADS)
    p.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    p.add_argument('--verbose', action='store_true')

    return p.parse_args()


def main():
    args = parse_args()
    setup_logging(args.verbose, args.log_level)

    logging.info("Fake / loop scan started")

    hash_history = load_recent_hash_files(
        args.hash_dir,
        args.max_history,
        args.threads
    )

    deep_info = load_deep_csv(args.deep_csv)
    fake_hash_db = load_fake_hash_db(args.fake_hash_json)
    fake_hash_db = update_fake_hash_db(hash_history, fake_hash_db)

    scores = {
        addr: score_address(addr, hash_history, deep_info, fake_hash_db)
        for addr in deep_info
    }

    write_output_files(
        args.deep_csv,
        args.output_total,
        args.output_fake,
        args.output_loop,
        args.output_ok,
        scores
    )

    save_fake_hash_db(fake_hash_db, args.fake_hash_json)
    logging.info("Fake / loop scan finished")


if __name__ == '__main__':
    main()