import os
import csv
import json
from glob import glob
from collections import Counter
from datetime import datetime

# ===============================
# 配置区
# ===============================

# 新源保护
MIN_HISTORY_SAMPLES = 3

# 硬规则阈值
THRESHOLD_ANCHOR_CONSISTENCY = 0.50      # 2s 或 32s ≥50%
THRESHOLD_ANCHOR_100 = 1.00              # 起始画面 100%
THRESHOLD_TOP5_RATIO = 0.80              # top5 phash ≥80%
THRESHOLD_REPEAT_SEQ_RATIO = 0.50        # 重复序列历史比例 ≥50%

# 评分阈值
SCORE_THRESHOLD = 70

# 评分权重
WEIGHT_DYNAMIC_CURRENT = 0.4
WEIGHT_DYNAMIC_HISTORY = 0.3
WEIGHT_CONTENT_CONCENTRATION = 0.2
WEIGHT_ANCHOR_BONUS = 0.1

FRAMES_PER_SAMPLE = 6

TYPICAL_REPEAT_SEQS = [
    "aabbaa", "abcabc", "ababab", "abcdab", "cabcab"
]

# ===============================
# 基础工具函数
# ===============================

def load_csv(filepath):
    with open(filepath, encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))

def load_all_hash_jsons(folder_path):
    hash_data = {}
    json_files = sorted(glob(os.path.join(folder_path, '*.json')))
    for jf in json_files:
        basename = os.path.basename(jf)
        try:
            detect_time = datetime.strptime(basename.split('-')[0], '%y%m%d%H%M')
        except:
            continue
        with open(jf, encoding='utf-8') as f:
            jdata = json.load(f)
        for url, content in jdata.items():
            hash_data.setdefault(url, []).append({
                'detect_time': detect_time,
                'phash': content.get('phash', [None] * FRAMES_PER_SAMPLE)
            })
    for url in hash_data:
        hash_data[url].sort(key=lambda x: x['detect_time'])
    return hash_data

def clean_phash(p):
    if not p or str(p).lower() == 'null':
        return None
    return p

def calc_dynamic(phash_list):
    if len(phash_list) < 2:
        return 100.0
    total, diff = 0, 0
    for i in range(len(phash_list)):
        for j in range(i + 1, len(phash_list)):
            total += 1
            if phash_list[i] != phash_list[j]:
                diff += 1
    return diff / total * 100 if total else 100.0

def calc_anchor_consistency(phash_history, index):
    values = []
    for s in phash_history:
        if index < len(s['phash']):
            p = clean_phash(s['phash'][index])
            if p:
                values.append(p)
    if not values:
        return 0.0
    return Counter(values).most_common(1)[0][1] / len(values)

# ===============================
# 核心分析函数
# ===============================

def analyze_phash_data(phash_history):
    total_samples = len(phash_history)
    total_frames = total_samples * FRAMES_PER_SAMPLE

    all_phash = []
    for s in phash_history:
        for p in s['phash']:
            p = clean_phash(p)
            if p:
                all_phash.append(p)

    if not all_phash:
        return None

    counter = Counter(all_phash)
    top5_ratio = sum(c for _, c in counter.most_common(5)) / total_frames

    last_phash = [clean_phash(p) for p in phash_history[-1]['phash'] if clean_phash(p)]
    dynamic_current = calc_dynamic(last_phash)

    dynamic_hist = []
    for s in phash_history:
        valid = [clean_phash(p) for p in s['phash'] if clean_phash(p)]
        if len(valid) >= 2:
            dynamic_hist.append(calc_dynamic(valid))

    mean_dynamic = sum(dynamic_hist) / len(dynamic_hist) if dynamic_hist else 100.0

    anchor_2s = calc_anchor_consistency(phash_history, 0)
    anchor_32s = calc_anchor_consistency(phash_history, 4)

    repeat_hit = 0
    for s in phash_history:
        seq = ''.join([p[0] if p else 'x' for p in s['phash']])
        if any(t in seq for t in TYPICAL_REPEAT_SEQS):
            repeat_hit += 1
    repeat_ratio = repeat_hit / total_samples if total_samples else 0

    score = (
        WEIGHT_DYNAMIC_CURRENT * (100 - dynamic_current) +
        WEIGHT_DYNAMIC_HISTORY * (100 - mean_dynamic) +
        WEIGHT_CONTENT_CONCENTRATION * (top5_ratio * 100) +
        WEIGHT_ANCHOR_BONUS * ((anchor_2s + anchor_32s) / 2 * 100)
    )

    return {
        'total_samples': total_samples,
        'top5_ratio': top5_ratio,
        'anchor_2s': anchor_2s,
        'anchor_32s': anchor_32s,
        'repeat_ratio': repeat_ratio,
        'static_score': score
    }

# ===============================
# 判定逻辑
# ===============================

def hard_rule_loop_judge(stat):
    if stat['total_samples'] < MIN_HISTORY_SAMPLES:
        return False

    if stat['anchor_2s'] >= THRESHOLD_ANCHOR_100:
        return True

    if (max(stat['anchor_2s'], stat['anchor_32s']) >= THRESHOLD_ANCHOR_CONSISTENCY
        and stat['top5_ratio'] >= THRESHOLD_TOP5_RATIO):
        return True

    if stat['repeat_ratio'] >= THRESHOLD_REPEAT_SEQ_RATIO:
        return True

    return False

def score_loop_judge(stat):
    return stat['static_score'] >= SCORE_THRESHOLD

# ===============================
# 主流程
# ===============================

def main():
    input_csv = 'output/middle/fake/fake_scan_total.csv'
    hash_folder = 'output/hash/merge/'

    csv_data = load_csv(input_csv)
    hash_data = load_all_hash_jsons(hash_folder)

    total, loop_cnt = 0, 0
    rows_total, rows_loop, rows_not = [], [], []

    for row in csv_data:
        url = row.get('地址', '')
        phash_history = hash_data.get(url)

        stat = analyze_phash_data(phash_history) if phash_history else None

        if not stat:
            reason = '未筛除'
            is_loop = False
        elif hard_rule_loop_judge(stat):
            reason = '硬规则筛除'
            is_loop = True
        elif score_loop_judge(stat):
            reason = '评分阈值超限筛除'
            is_loop = True
        else:
            reason = '未筛除'
            is_loop = False

        new_row = dict(row)
        new_row['轮播_筛除原因'] = reason
        rows_total.append(new_row)

        if is_loop:
            rows_loop.append(new_row)
            loop_cnt += 1
        else:
            rows_not.append(new_row)

        total += 1

    os.makedirs('output/middle/loop', exist_ok=True)
    fields = list(rows_total[0].keys())

    def write(path, rows):
        with open(path, 'w', encoding='utf-8-sig', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    write('output/middle/loop/loop_scan_total.csv', rows_total)
    write('output/middle/loop/loop_scan_yes.csv', rows_loop)
    write('output/middle/loop/loop_scan_not.csv', rows_not)

    print(f'轮播筛除数：{loop_cnt}')
    print(f'总源数：{total}')
    print('轮播扫描完成')

if __name__ == '__main__':
    main()
