import os
import csv
import json
from glob import glob
from collections import Counter
from datetime import datetime

# ====== 配置区：阈值与参数设置 ======

# ================================
# 【修改 0️⃣】新源保护（样本量）
# ================================
MIN_HISTORY_SAMPLES = 3
# —— 历史检测次数 ≤ 2 视为新源 / 观察期
#    替代原“首次出现时间为当天”逻辑


# 1️⃣ 最近一次检测动态值阈值（原有）
THRESHOLD_DYNAMIC_1 = 10  


# ================================
# 【修改 2️⃣】主 phash 重复性（分样本量）
# ================================
PHASH_SAMPLE_SPLIT = 72
# —— phash 总数分界线（检测次数 × 6）

THRESHOLD_MAIN_PHASH_RATIO_SMALL = 0.30
# —— 样本数 < 72 时，主 phash 占比 ≥ 30%

THRESHOLD_MAIN_PHASH_COUNT_LARGE = 20
# —— 样本数 ≥ 72 时，主 phash 出现次数 ≥ 20


# 3️⃣ 锚点一致率阈值（原有）
THRESHOLD_ANCHOR_CONSISTENCY_2S = 0.95  
THRESHOLD_ANCHOR_CONSISTENCY_32S = 0.95  


# ================================
# 【评分系统】完全保留（不改）
# ================================
WEIGHT_CURRENT_DYNAMIC = 0.4  
WEIGHT_HISTORY_STABILITY = 0.3  
WEIGHT_CONTENT_CONCENTRATION = 0.2  
WEIGHT_ANCHOR_BONUS = 0.1  

SCORE_STATIC_THRESHOLD = 100  


GRAB_TIMES = [2, 5, 9, 16, 32, 50]


def load_csv(filepath):
    with open(filepath, encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def load_all_hash_jsons(folder_path):
    hash_data = {}
    json_files = sorted(glob(os.path.join(folder_path, '*.json')))

    for jf in json_files:
        basename = os.path.basename(jf)
        detect_time_str = basename.split('-')[0]
        try:
            detect_time = datetime.strptime(detect_time_str, '%y%m%d%H%M')
        except Exception:
            detect_time = detect_time_str

        with open(jf, encoding='utf-8') as f:
            jdata = json.load(f)

        for url, content in jdata.items():
            hash_data.setdefault(url, []).append({
                'detect_time': detect_time,
                'phash': content.get('phash', [None] * 6)
            })

    for url in hash_data:
        hash_data[url].sort(key=lambda x: x['detect_time'])

    return hash_data


def clean_phash(phash):
    if phash is None or phash == '' or (isinstance(phash, str) and phash.lower() == 'null'):
        return None
    return phash


def analyze_phash_data(phash_history):
    """
    【说明】
    - 评分系统逻辑：完全沿用原实现
    - 仅新增硬规则所需的统计字段
    """
    total_samples = len(phash_history)
    total_frames = total_samples * len(GRAB_TIMES)

    all_phash_list = []
    confidence_per_sample = []

    for sample in phash_history:
        phash_valid = [clean_phash(p) for p in sample['phash']]
        valid = [p for p in phash_valid if p is not None]
        confidence_per_sample.append(len(valid))
        all_phash_list.extend(valid)

    if not all_phash_list:
        return None

    counter = Counter(all_phash_list)
    max_count = max(counter.values())
    candidates = [p for p, c in counter.items() if c == max_count]
    
    last_sample_phash = [
        p for p in (clean_phash(x) for x in phash_history[-1]['phash']) if p
    ]

    main_phash = next(
        (p for p in reversed(last_sample_phash) if p in candidates),
        candidates[0]
    )

    main_phash_total_count = counter[main_phash]
    main_phash_ratio = main_phash_total_count / total_frames if total_frames else 0

    dynamic_1 = calc_dynamic_1(last_sample_phash)

    top3_repeat_ratio = (
        sum(cnt for _, cnt in counter.most_common(3)) / total_frames
        if total_frames else 0
    )

    anchor_2s = calc_anchor_consistency(phash_history, 0)
    anchor_32s = calc_anchor_consistency(phash_history, 4)


    # 首次出现时间
    first_appearance_time = None
    for sample in phash_history:
        phash_valid = [clean_phash(p) for p in sample['phash']]
        if main_phash in phash_valid:
            first_appearance_time = sample['detect_time']
            break
    # 当前置信度（最近一次检测有效phash数量）
    current_confidence = confidence_per_sample[-1]

    # 历史置信度 = 所有有效phash数量 / 总抓帧数量
    total_valid_phash = len(all_phash_list)
    history_confidence = total_valid_phash / total_frames if total_frames > 0 else 0


    # ================================
    # 【评分系统】原样保留
    # ================================
    dynamic_history_values = []
    for sample in phash_history:
        phash_valid = [clean_phash(p) for p in sample['phash']]
        valid = [p for p in phash_valid if p is not None]
        if len(valid) >= 2:
            dynamic_history_values.append(calc_dynamic_1(valid))

    if dynamic_history_values:
        mean_dynamic = sum(dynamic_history_values) / len(dynamic_history_values)
        variance_dynamic = sum(
            (x - mean_dynamic) ** 2 for x in dynamic_history_values
        ) / len(dynamic_history_values)
    else:
        mean_dynamic = variance_dynamic = 0

    score_current_dynamic = convert_dynamic_to_score(dynamic_1)
    score_history_stability = convert_history_dynamic_to_score(
        mean_dynamic, variance_dynamic
    )
    score_content_concentration = top3_repeat_ratio * 100
    score_anchor_bonus = (anchor_2s + anchor_32s) / 2 * 100

    static_score = (
        WEIGHT_CURRENT_DYNAMIC * score_current_dynamic +
        WEIGHT_HISTORY_STABILITY * score_history_stability +
        WEIGHT_CONTENT_CONCENTRATION * score_content_concentration +
        WEIGHT_ANCHOR_BONUS * score_anchor_bonus
    )

    return {
        # ===== 原有字段 =====
        'dynamic_1': dynamic_1,
        'main_phash': main_phash,
        'main_phash_total_count': main_phash_total_count,
        'top3_repeat_ratio': top3_repeat_ratio,
        'anchor_consistency_2s': anchor_2s,
        'anchor_consistency_32s': anchor_32s,
        'static_score': static_score,
        'score_current_dynamic': score_current_dynamic,
        'score_history_stability': score_history_stability,
        'score_content_concentration': score_content_concentration,
        'score_anchor_bonus': score_anchor_bonus,
        'current_confidence': current_confidence,
        'history_confidence': history_confidence,
        'first_appearance_time': first_appearance_time,
        # ===== 新增字段（仅供硬规则使用）=====
        'total_samples': total_samples,
        'total_frames': total_frames,
        'main_phash_ratio': main_phash_ratio
    }


def calc_dynamic_1(phash_list):
    if len(phash_list) < 2:
        return 100.0
    total = diff = 0
    for i in range(len(phash_list)):
        for j in range(i + 1, len(phash_list)):
            total += 1
            if phash_list[i] != phash_list[j]:
                diff += 1
    return diff / total * 100 if total else 100.0


def calc_anchor_consistency(phash_history, index):
    values = []
    for sample in phash_history:
        if index < len(sample['phash']):
            p = clean_phash(sample['phash'][index])
            if p:
                values.append(p)
    if not values:
        return 0.0
    return Counter(values).most_common(1)[0][1] / len(values)


def convert_dynamic_to_score(val):
    return max(0, min(100, 100 - val))


def convert_history_dynamic_to_score(mean_dyn, var_dyn):
    mean_score = max(0, min(100, 100 - mean_dyn))
    var_score = max(0, min(100, 100 - var_dyn * 50))
    return 0.7 * mean_score + 0.3 * var_score


# ==================================================
# 【修改 5️⃣ + 6️⃣】硬规则 + 灰区判定（仅此函数改）
# ==================================================
def hard_rule_judge(stat):
    """
    返回：
    is_static, is_gray, reason
    """
    if stat is None:
        return False, False, '无有效数据'

    # 0️⃣ 新源保护
    if stat['total_samples'] < MIN_HISTORY_SAMPLES:
        return False, False, '观察期：样本不足'

    # 1️⃣ dynamic 约束
    if stat['dynamic_1'] > THRESHOLD_DYNAMIC_1:
        return False, False, '动态值过高'

    # 2️⃣ 主 phash 重复性（分样本量）
    if stat['total_frames'] < PHASH_SAMPLE_SPLIT:
        cond_phash = stat['main_phash_ratio'] >= THRESHOLD_MAIN_PHASH_RATIO_SMALL
    else:
        cond_phash = stat['main_phash_total_count'] >= THRESHOLD_MAIN_PHASH_COUNT_LARGE

    if not cond_phash:
        return False, False, '主phash重复度不足'

    # 3️⃣ 锚点一致率
    anchor_ok = (
        stat['anchor_consistency_2s'] >= THRESHOLD_ANCHOR_CONSISTENCY_2S or
        stat['anchor_consistency_32s'] >= THRESHOLD_ANCHOR_CONSISTENCY_32S
    )

    if not anchor_ok:
        return False, False, '锚点一致率不足'

    # 6️⃣ 灰区：不再作为硬门槛
    if stat['top3_repeat_ratio'] < 0.7:
        return True, True, '灰区：内容集中度不足'

    # 5️⃣ 硬规则命中
    return True, False, '硬规则命中：高度静态'
def score_rule_judge(stat):
    """
    评分判定是否静态假源，返回布尔值和筛除原因字符串
    """
    if stat is None:
        return False, '无有效数据'

    if stat['static_score'] >= SCORE_STATIC_THRESHOLD:
        return True, f"评分高于阈值({stat['static_score']:.2f} ≥ {SCORE_STATIC_THRESHOLD})"
    return False, ''


def generate_output_rows(csv_data, hash_data):
    """
    遍历csv条目，对应hash检测结果，计算统计及判定，准备输出行字典
    【修改】新增灰区字段输出
    """
    total_rows = []
    ok_rows = []
    not_rows = []

    for row in csv_data:
        url = row.get('地址', '')
        phash_history = hash_data.get(url)

        stats = analyze_phash_data(phash_history) if phash_history else None

        # 新源保护判定，只有硬规则判定时考虑（评分不排除）
        if stats and stats['total_samples'] < MIN_HISTORY_SAMPLES:
            is_static = False
            is_gray = False
            filter_reason = '观察期 / 新源'
            gray_reason = ''
            gray_action = ''
        else:
            is_hard, is_gray, reason_hard = hard_rule_judge(stats)
            is_score, reason_score = score_rule_judge(stats)

            # 硬规则优先
            if is_hard:
                is_static = True
                filter_reason = reason_hard
            elif is_score:
                is_static = True
                filter_reason = reason_score
            else:
                is_static = False
                filter_reason = '未筛除'

            gray_reason = reason_hard if is_gray else ''
            gray_action = '建议延迟复核' if is_gray else ''

        # 构造新增列字典，新增灰区字段【修改】
        new_fields = {
            '筛除原因': filter_reason if is_static else '未筛除',
            '是否灰区': '是' if is_gray else '否',
            '灰区原因': gray_reason,
            '灰区建议动作': gray_action,
            '动态值（阈值 ≤ 10）': f"{stats['dynamic_1']:.2f}" if stats else '',
            '主phash总出现次数': stats['main_phash_total_count'] if stats else '',
            '主phash占比': f"{stats['main_phash_ratio']:.4f}" if stats else '',
            '主phash值': stats['main_phash'] if stats else '',
            '前三主phash重复比例（阈值 ≥ 70%）': f"{stats['top3_repeat_ratio']:.2%}" if stats else '',
            '锚点一致率-2秒（阈值 ≥ 95%）': f"{stats['anchor_consistency_2s']:.2%}" if stats else '',
            '锚点一致率-32秒（阈值 ≥ 95%）': f"{stats['anchor_consistency_32s']:.2%}" if stats else '',
            '静态评分': f"{stats['static_score']:.2f}" if stats else '',
            '最近一次动态分': f"{stats['score_current_dynamic']:.2f}" if stats else '',
            '历史动态稳定分': f"{stats['score_history_stability']:.2f}" if stats else '',
            '内容集中度分': f"{stats['score_content_concentration']:.2f}" if stats else '',
            '锚点软加分': f"{stats['score_anchor_bonus']:.2f}" if stats else '',
            '当前置信度': stats['current_confidence'] if stats and 'current_confidence' in stats else '',
            '历史置信度': f"{stats['history_confidence']:.4f}" if stats and 'history_confidence' in stats else '',
            '首次出现时间': stats['first_appearance_time'].strftime('%Y-%m-%d %H:%M:%S') if stats and 'first_appearance_time' in stats and stats['first_appearance_time'] else ''
        }

        combined_row = dict(row)
        combined_row.update(new_fields)

        total_rows.append(combined_row)
        if is_static:
            not_rows.append(combined_row)
        else:
            ok_rows.append(combined_row)

    return total_rows, ok_rows, not_rows


def write_csv(filepath, rows, fieldnames):
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    csv_input_path = 'output/middle/deep/deep_total_ok.csv'
    hash_json_folder = 'output/hash/merge/'

    print("加载输入CSV文件...")
    csv_data = load_csv(csv_input_path)

    print("加载hash检测JSON文件...")
    hash_data = load_all_hash_jsons(hash_json_folder)

    print("计算统计指标，进行判定...")
    total_rows, ok_rows, not_rows = generate_output_rows(csv_data, hash_data)

    # 输出字段调整，新增灰区相关字段【修改】
    base_fields = csv_data[0].keys() if csv_data else []
    added_fields = [
        '筛除原因',
        '是否灰区',
        '灰区原因',
        '灰区建议动作',
        '动态值（阈值 ≤ 10）',
        '主phash总出现次数',
        '主phash占比',
        '主phash值',
        '前三主phash重复比例（阈值 ≥ 70%）',
        '锚点一致率-2秒（阈值 ≥ 95%）',
        '锚点一致率-32秒（阈值 ≥ 95%）',
        '静态评分',
        '最近一次动态分',
        '历史动态稳定分',
        '内容集中度分',
        '锚点软加分',
        '当前置信度',
        '历史置信度',
        '首次出现时间'
    ]
    fieldnames = list(base_fields) + added_fields

    print("写入输出文件...")
    os.makedirs('output/middle/fake', exist_ok=True)
    write_csv('output/middle/fake/fake_scan_total.csv', total_rows, fieldnames)
    write_csv('output/middle/fake/fake_scan_ok.csv', ok_rows, fieldnames)
    write_csv('output/middle/fake/fake_scan_not.csv', not_rows, fieldnames)

    print(f"筛选出静态假源条数（fake_scan_not.csv）：{len(not_rows)}")
    print(f"非静态源条数（fake_scan_ok.csv）：{len(ok_rows)}")
    print(f"总条目数（fake_scan_total.csv）：{len(total_rows)}")

    print("全部完成！")


if __name__ == '__main__':
    main()
