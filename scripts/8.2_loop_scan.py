import os
import csv
import json
from glob import glob
from collections import Counter
from datetime import datetime

# ===============================
# 【配置区】阈值与参数设置
# ===============================

# 新源保护：检测样本数不足时，不进行硬判定，走评分逻辑
MIN_HISTORY_SAMPLES = 3

# 硬规则阈值

# 1. 锚点一致率阈值（2秒和32秒）
THRESHOLD_ANCHOR_CONSISTENCY_2S = 0.50  # ≥50%即满足硬规则A部分
THRESHOLD_ANCHOR_CONSISTENCY_32S = 0.50  # 同上

# 2. top5 phash内容集中度阈值
THRESHOLD_TOP5_PHASH_RATIO = 0.80  # ≥80%

# 3. 最近一次检测动态值阈值（用于评分辅助）
THRESHOLD_DYNAMIC_1 = 10

# 4. 轮播重复序列历史比例阈值（硬规则B）
THRESHOLD_REPEAT_SEQ_RATIO = 0.50  # ≥50%

# 5. 起始画面完全一致阈值（硬规则D）
THRESHOLD_ANCHOR_CONSISTENCY_2S_100 = 1.0  # 100%

# 评分权重（软判定）
WEIGHT_DYNAMIC_CURRENT = 0.4
WEIGHT_DYNAMIC_HISTORY = 0.3
WEIGHT_CONTENT_CONCENTRATION = 0.2
WEIGHT_ANCHOR_BONUS = 0.1

# 评分阈值（≥即判为轮播评分阶段）
SCORE_THRESHOLD = 70

# --------------------------------
# 采样帧数量（每次检测6帧）
FRAMES_PER_SAMPLE = 6

# 典型轮播序列样本（简易版，后续可扩展）
TYPICAL_REPEAT_SEQS = [
    "aabbaa", "abcabc", "ababab", "abcdab", "cabcab"
]

# ===============================
# 基础函数
# ===============================

def load_csv(filepath):
    with open(filepath, encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))

def load_all_hash_jsons(folder_path):
    """
    载入所有json文件，合并成字典
    {
      url: [
        {
          'detect_time': datetime,
          'phash': [phash1, phash2, ... phash6]
        }, ...
      ]
    }
    """
    hash_data = {}
    json_files = sorted(glob(os.path.join(folder_path, '*.json')))
    for jf in json_files:
        basename = os.path.basename(jf)
        try:
            detect_time = datetime.strptime(basename.split('-')[0], '%y%m%d%H%M')
        except:
            detect_time = basename.split('-')[0]
        with open(jf, encoding='utf-8') as f:
            jdata = json.load(f)
        for url, content in jdata.items():
            hash_data.setdefault(url, []).append({
                'detect_time': detect_time,
                'phash': content.get('phash', [None]*FRAMES_PER_SAMPLE)
            })
    # 按时间排序
    for url in hash_data:
        hash_data[url].sort(key=lambda x: x['detect_time'])
    return hash_data

def clean_phash(p):
    if p is None or p == '' or (isinstance(p, str) and p.lower() == 'null'):
        return None
    return p

# --- 先到这里 --- 

# 你确认这部分代码没问题，我马上给你第二部分（核心判定和评分函数）

下面是你这段代码的第一部分，包括配置区和基础函数：


```python
import os
import csv
import json
from glob import glob
from collections import Counter
from datetime import datetime

# ===============================
# 【配置区】阈值与参数设置
# ===============================

# 新源保护：检测样本数不足时，不进行硬判定，走评分逻辑
MIN_HISTORY_SAMPLES = 3

# 硬规则阈值

# 1. 锚点一致率阈值（2秒和32秒）
THRESHOLD_ANCHOR_CONSISTENCY_2S = 0.50  # ≥50%即满足硬规则A部分
THRESHOLD_ANCHOR_CONSISTENCY_32S = 0.50  # 同上

# 2. top5 phash内容集中度阈值
THRESHOLD_TOP5_PHASH_RATIO = 0.80  # ≥80%

# 3. 最近一次检测动态值阈值（用于评分辅助）
THRESHOLD_DYNAMIC_1 = 10

# 4. 轮播重复序列历史比例阈值（硬规则B）
THRESHOLD_REPEAT_SEQ_RATIO = 0.50  # ≥50%

# 5. 起始画面完全一致阈值（硬规则D）
THRESHOLD_ANCHOR_CONSISTENCY_2S_100 = 1.0  # 100%

# 评分权重（软判定）
WEIGHT_DYNAMIC_CURRENT = 0.4
WEIGHT_DYNAMIC_HISTORY = 0.3
WEIGHT_CONTENT_CONCENTRATION = 0.2
WEIGHT_ANCHOR_BONUS = 0.1

# 评分阈值（≥即判为轮播评分阶段）
SCORE_THRESHOLD = 70

# --------------------------------
# 采样帧数量（每次检测6帧）
FRAMES_PER_SAMPLE = 6

# 典型轮播序列样本（简易版，后续可扩展）
TYPICAL_REPEAT_SEQS = [
    "aabbaa", "abcabc", "ababab", "abcdab", "cabcab"
]

# ===============================
# 基础函数
# ===============================

def load_csv(filepath):
    with open(filepath, encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))

def load_all_hash_jsons(folder_path):
    """
    载入所有json文件，合并成字典
    {
      url: [
        {
          'detect_time': datetime,
          'phash': [phash1, phash2, ... phash6]
        }, ...
      ]
    }
    """
    hash_data = {}
    json_files = sorted(glob(os.path.join(folder_path, '*.json')))
    for jf in json_files:
        basename = os.path.basename(jf)
        try:
            detect_time = datetime.strptime(basename.split('-')[0], '%y%m%d%H%M')
        except:
            detect_time = basename.split('-')[0]
        with open(jf, encoding='utf-8') as f:
            jdata = json.load(f)
        for url, content in jdata.items():
            hash_data.setdefault(url, []).append({
                'detect_time': detect_time,
                'phash': content.get('phash', [None]*FRAMES_PER_SAMPLE)
            })
    # 按时间排序
    for url in hash_data:
        hash_data[url].sort(key=lambda x: x['detect_time'])
    return hash_data

def clean_phash(p):
    if p is None or p == '' or (isinstance(p, str) and p.lower() == 'null'):
        return None
    return p
from collections import defaultdict

def calc_dynamic_1(phash_list):
    """
    计算动态值：phash列表中不同元素的比例，值越低越静态
    """
    if len(phash_list) < 2:
        return 100.0
    total = 0
    diff = 0
    for i in range(len(phash_list)):
        for j in range(i + 1, len(phash_list)):
            total += 1
            if phash_list[i] != phash_list[j]:
                diff += 1
    return (diff / total) * 100 if total else 100.0

def calc_anchor_consistency(phash_history, index):
    """
    计算指定采样帧索引的phash一致率
    """
    values = []
    for sample in phash_history:
        if index < len(sample['phash']):
            p = clean_phash(sample['phash'][index])
            if p:
                values.append(p)
    if not values:
        return 0.0
    counter = Counter(values)
    most_common_count = counter.most_common(1)[0][1]
    return most_common_count / len(values)

def analyze_phash_data(phash_history):
    """
    统计分析phash数据，计算硬规则及评分所需指标
    """
    total_samples = len(phash_history)
    total_frames = total_samples * FRAMES_PER_SAMPLE

    all_phash_list = []
    for sample in phash_history:
        valid_phash = [clean_phash(p) for p in sample['phash']]
        valid_phash = [p for p in valid_phash if p]
        all_phash_list.extend(valid_phash)

    if not all_phash_list:
        return None

    phash_counter = Counter(all_phash_list)
    max_count = max(phash_counter.values())
    main_phash_candidates = [p for p, c in phash_counter.items() if c == max_count]

    # 取最近一次检测的phash，优先选其中存在于最大计数候选的
    last_sample_phash = [clean_phash(p) for p in phash_history[-1]['phash']]
    last_sample_phash = [p for p in last_sample_phash if p]
    main_phash = next((p for p in reversed(last_sample_phash) if p in main_phash_candidates), main_phash_candidates[0])

    main_phash_total_count = phash_counter[main_phash]
    main_phash_ratio = main_phash_total_count / total_frames if total_frames else 0

    # 计算top5 phash占比
    top5_count = sum(c for _, c in phash_counter.most_common(5))
    top5_ratio = top5_count / total_frames if total_frames else 0

    # 计算动态值（最近一次检测）
    dynamic_1 = calc_dynamic_1(last_sample_phash)

    # 计算锚点一致率
    anchor_2s = calc_anchor_consistency(phash_history, 0)
    anchor_32s = calc_anchor_consistency(phash_history, 4)

    # 计算轮播重复序列历史比例（硬规则B）
    repeat_seq_hit_count = 0
    for sample in phash_history:
        phash_seq = ''.join([p[0] if p else 'x' for p in sample['phash']])
        if any(seq in phash_seq for seq in TYPICAL_REPEAT_SEQS):
            repeat_seq_hit_count += 1
    repeat_seq_ratio = repeat_seq_hit_count / total_samples if total_samples else 0

    # 评分计算
    score_dynamic_current = max(0, min(100, 100 - dynamic_1))

    # 历史动态均值和方差
    dynamic_history_values = []
    for sample in phash_history:
        valid_phash = [clean_phash(p) for p in sample['phash']]
        valid_phash = [p for p in valid_phash if p]
        if len(valid_phash) >= 2:
            dynamic_history_values.append(calc_dynamic_1(valid_phash))
    if dynamic_history_values:
        mean_dynamic = sum(dynamic_history_values) / len(dynamic_history_values)
        variance_dynamic = sum((x - mean_dynamic) ** 2 for x in dynamic_history_values) / len(dynamic_history_values)
    else:
        mean_dynamic = variance_dynamic = 0
    score_dynamic_history = max(0, min(100, 100 - mean_dynamic)) * 0.7 + max(0, min(100, 100 - variance_dynamic * 50)) * 0.3

    score_content_concentration = top5_ratio * 100
    score_anchor_bonus = ((anchor_2s + anchor_32s) / 2) * 100

    static_score = (WEIGHT_DYNAMIC_CURRENT * score_dynamic_current +
                    WEIGHT_DYNAMIC_HISTORY * score_dynamic_history +
                    WEIGHT_CONTENT_CONCENTRATION * score_content_concentration +
                    WEIGHT_ANCHOR_BONUS * score_anchor_bonus)

    # 首次出现时间
    first_appearance_time = None
    for sample in phash_history:
        valid_phash = [clean_phash(p) for p in sample['phash']]
        if main_phash in valid_phash:
            first_appearance_time = sample['detect_time']
            break

    return {
        'total_samples': total_samples,
        'total_frames': total_frames,
        'main_phash': main_phash,
        'main_phash_total_count': main_phash_total_count,
        'main_phash_ratio': main_phash_ratio,
        'top5_ratio': top5_ratio,
        'dynamic_1': dynamic_1,
        'anchor_consistency_2s': anchor_2s,
        'anchor_consistency_32s': anchor_32s,
        'repeat_seq_ratio': repeat_seq_ratio,
        'static_score': static_score,
        'score_dynamic_current': score_dynamic_current,
        'score_dynamic_history': score_dynamic_history,
        'score_content_concentration': score_content_concentration,
        'score_anchor_bonus': score_anchor_bonus,
        'first_appearance_time': first_appearance_time,
    }

def hard_rule_judge(stat):
    """
    硬规则判定，返回 (是否轮播, 是否灰区, 判定原因)
    """
    if stat is None:
        return False, False, '无数据'

    if stat['total_samples'] < MIN_HISTORY_SAMPLES:
        return False, False, '样本不足，视为新源，跳过硬判'

    # 条件A: 锚点一致率≥50%且top5 phash占比≥80%
    cond_a = (stat['anchor_consistency_2s'] >= THRESHOLD_ANCHOR_CONSISTENCY_2S and
              stat['top5_ratio'] >= THRESHOLD_TOP5_PHASH_RATIO)

    # 条件B: 典型轮播重复序列在历史样本中占比≥50%
    cond_b = stat['repeat_seq_ratio'] >= THRESHOLD_REPEAT_SEQ_RATIO

    # 条件D: 起始画面完全一致（100%）
    cond_d = stat['anchor_consistency_2s'] >= THRESHOLD_ANCHOR_CONSISTENCY_2S_100

    if cond_a or cond_b or cond_d:
        # 软灰区判断：内容集中度不足则为灰区
        is_gray = stat['top5_ratio'] < THRESHOLD_TOP5_PHASH_RATIO
        reason = '硬规则判定为轮播' + ('，内容集中度不足，灰区' if is_gray else '')
        return True, is_gray, reason

    return False, False, '未满足硬规则'

def score_rule_judge(stat):
    """
    评分判定是否轮播，返回 (是否轮播, 判定原因)
    """
    if stat is None:
        return False, '无数据'
    if stat['static_score'] >= SCORE_THRESHOLD:
        return True, f'评分高于阈值({stat["static_score"]:.2f}≥{SCORE_THRESHOLD})'
    return False, '评分未达阈值'
def hard_rule_loop_judge(stat):
    """
    硬规则判定轮播源，返回 (is_loop, reason_str)
    reason_str示例："硬规则1命中"
    """
    if stat is None:
        return False, '无有效数据'

    # 硬规则1：非新源（样本数≥MIN_HISTORY_SAMPLES）
    if stat['total_samples'] < MIN_HISTORY_SAMPLES:
        return False, '样本数不足'

    # 硬规则2：锚点一致率≥阈值 AND top5 phash占比≥阈值
    anchor_ok = (
        stat['anchor_consistency_2s'] >= THRESHOLD_ANCHOR_CONSISTENCY_2S or
        stat['anchor_consistency_32s'] >= THRESHOLD_ANCHOR_CONSISTENCY_32S
    )
    top5_phash_ratio = stat.get('top5_phash_ratio', 0)  # 需要在分析时计算
    if not (anchor_ok and top5_phash_ratio >= 0.80):
        return False, ''

    return True, '硬规则1命中'

def score_loop_judge(stat):
    """
    评分判定轮播源，返回 (is_loop, score, reason_str)
    """
    if stat is None:
        return False, 0.0, '无有效数据'

    score = stat.get('loop_score', 0)
    if score >= LOOP_SCORE_THRESHOLD:
        return True, score, f'评分阈值超限({score:.2f}≥{LOOP_SCORE_THRESHOLD})'
    return False, score, ''

def generate_loop_output_rows(csv_data, hash_data):
    """
    针对每条数据，结合hash结果计算轮播判定，输出带新增轮播字段的行
    """

    total_rows = []
    loop_rows = []
    not_loop_rows = []

    for row in csv_data:
        url = row.get('地址', '')
        phash_history = hash_data.get(url)

        stat = analyze_loop_phash_data(phash_history) if phash_history else None

        # 计算top5 phash占比，用于硬规则判定，补充字段
        if stat and stat.get('all_phash_list'):
            counter = Counter(stat['all_phash_list'])
            top5_count = sum(c for _, c in counter.most_common(5))
            total_count = stat['total_frames']
            stat['top5_phash_ratio'] = top5_count / total_count if total_count else 0
        else:
            stat = stat or {}
            stat['top5_phash_ratio'] = 0

        # 硬规则判定
        is_hard, hard_reason = hard_rule_loop_judge(stat)

        # 评分判定
        is_score, score_val, score_reason = score_loop_judge(stat)

        # 轮播判定优先硬规则
        if is_hard:
            is_loop = True
            filter_reason = '硬规则筛除'
            detail_reason = hard_reason
        elif is_score:
            is_loop = True
            filter_reason = '评分阈值超限筛除'
            detail_reason = score_reason
        else:
            is_loop = False
            filter_reason = '未筛除'
            detail_reason = ''

        # 组装新增轮播字段
        new_fields = {
            '轮播_筛除原因': filter_reason,
            '轮播_硬规则具体项': detail_reason,
            '轮播_评分值': f"{score_val:.2f}",
            '轮播_样本检测次数': stat['total_samples'] if stat and 'total_samples' in stat else 0,
        }

        combined_row = dict(row)
        combined_row.update(new_fields)

        total_rows.append(combined_row)
        if is_loop:
            loop_rows.append(combined_row)
        else:
            not_loop_rows.append(combined_row)

    return total_rows, not_loop_rows, loop_rows


def write_loop_csv(filepath, rows, fieldnames):
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main_loop():
    csv_input_path = 'output/middle/fake/fake_scan_total.csv'
    hash_json_folder = 'output/hash/merge/'

    print("加载输入CSV文件...")
    csv_data = load_csv(csv_input_path)

    print("加载hash检测JSON文件...")
    hash_data = load_all_hash_jsons(hash_json_folder)

    print("计算轮播判定结果...")
    total_rows, not_loop_rows, loop_rows = generate_loop_output_rows(csv_data, hash_data)

    base_fields = list(csv_data[0].keys()) if csv_data else []
    added_fields = [
        '轮播_筛除原因',
        '轮播_硬规则具体项',
        '轮播_评分值',
        '轮播_样本检测次数',
    ]
    fieldnames = base_fields + added_fields

    print("写入轮播检测输出文件...")
    os.makedirs('output/middle/loop', exist_ok=True)
    write_loop_csv('output/middle/loop/loop_scan_total.csv', total_rows, fieldnames)
    write_loop_csv('output/middle/loop/loop_scan_not.csv', not_loop_rows, fieldnames)
    write_loop_csv('output/middle/loop/loop_scan_yes.csv', loop_rows, fieldnames)

    print(f"轮播判定数（loop_scan_yes.csv）：{len(loop_rows)}")
    print(f"非轮播数（loop_scan_not.csv）：{len(not_loop_rows)}")
    print(f"总条目数（loop_scan_total.csv）：{len(total_rows)}")

    print("轮播检测完成！")


if __name__ == '__main__':
    main_loop()
