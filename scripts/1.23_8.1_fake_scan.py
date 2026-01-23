import os
import csv
import json
from glob import glob
from collections import Counter
from datetime import datetime

# ====== 配置区：阈值与参数设置 ======

# 硬规则阈值设定：
THRESHOLD_DYNAMIC_1 = 10  
# —— 最近一次检测的“动态值”阈值（%）
#    动态值是指单次检测中6个采样帧的phash两两比较的不同比例
#    数值越小表示越静态，阈值10表示动态值小于等于10%视为静态源硬判定条件之一

THRESHOLD_MAIN_PHASH_TOTAL_COUNT = 40  
# —— 主phash在所有历史检测样本中出现的总次数阈值
#    主phash是出现频率最高的phash，用以衡量内容重复程度
#    出现次数≥40说明该内容高度重复，符合静态源特征

THRESHOLD_TOP3_REPEAT_RATIO = 0.7  
# —— 前三主phash出现次数之和占总采样次数比例阈值
#    反映内容集中度，值越高说明内容越集中，重复度越高
#    0.7表示70%以上采样被前三个phash占据，偏向静态源

THRESHOLD_ANCHOR_CONSISTENCY_2S = 0.95  
# —— 锚点一致率-2秒阈值（比例）
#    在所有有效检测样本中，2秒抓帧点的phash一致比例
#    ≥95%表明该采样点非常稳定，符合静态源特征

THRESHOLD_ANCHOR_CONSISTENCY_32S = 0.95  
# —— 锚点一致率-32秒阈值（比例）
#    类似2秒锚点一致率，针对32秒采样点

# 评分系统权重配置（用于综合评分计算）：
WEIGHT_CURRENT_DYNAMIC = 0.4  
# —— 当前动态分权重，反映最近一次检测的动态性
#    权重较大，重视最新动态变化

WEIGHT_HISTORY_STABILITY = 0.3  
# —— 历史动态稳定分权重，反映长期动态稳定性
#    较大权重，体现内容长期稳定性对静态判定的影响

WEIGHT_CONTENT_CONCENTRATION = 0.2  
# —— 内容集中度分权重（前三主phash重复比例）
#    体现内容重复密集度，权重中等

WEIGHT_ANCHOR_BONUS = 0.1  
# —— 锚点软加分权重（锚点一致率平均）
#    辅助判定，权重较小，作为额外参考

# 评分判定阈值：
SCORE_STATIC_THRESHOLD = 100  
# —— 评分系统静态源判定阈值（0-100）
#    评分大于等于80判定为静态假源，低于80则为动态或不确定

# 抓帧时间点（秒）：
GRAB_TIMES = [2, 5, 9, 16, 32, 50]
# —— 采样的时间点列表（秒）
#    对视频流在这些时间点抓取帧计算phash，用于动态和静态分析

def load_csv(filepath):
    """加载输入CSV，返回字典列表"""
    with open(filepath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        data = list(reader)
    return data


def load_all_hash_jsons(folder_path):
    """加载所有hash检测JSON文件，返回dict：{url: [{检测时间, phash列表}, ...]}"""
    hash_data = {}
    json_files = sorted(glob(os.path.join(folder_path, '*.json')))
    for jf in json_files:
        # 解析检测时间（文件名前缀）
        basename = os.path.basename(jf)
        detect_time_str = basename.split('-')[0]
        try:
            detect_time = datetime.strptime(detect_time_str, '%y%m%d%H%M')
        except Exception:
            detect_time = detect_time_str  # 如果格式异常，保留原字符串
        
        with open(jf, encoding='utf-8') as f:
            jdata = json.load(f)
        
        for url, content in jdata.items():
            if url not in hash_data:
                hash_data[url] = []
            # phash列表为一个长度6的list，注意null/空需后续处理
            phash_list = content.get('phash', [None]*6)
            hash_data[url].append({
                'detect_time': detect_time,
                'phash': phash_list
            })
    # 按检测时间排序（升序）
    for url in hash_data:
        hash_data[url].sort(key=lambda x: x['detect_time'])
    return hash_data


def clean_phash(phash):
    """清理单个phash值，空字符串或 None 视为无效，返回None"""
    if phash is None or phash == '' or (isinstance(phash, str) and phash.lower() == 'null'):
        return None
    return phash


def analyze_phash_data(phash_history):
    """
    输入：phash_history = [{'detect_time':..., 'phash':[6帧phash]}, ...]
    输出：结构化统计数据
    """
    total_samples = len(phash_history)
    total_frames = total_samples * len(GRAB_TIMES)  # 6帧采样点

    # 1. 主phash统计：统计所有有效phash出现频次
    all_phash_list = []
    # 记录每次检测有效phash数量（置信度）
    confidence_per_sample = []
    # 记录主phash首次出现时间点
    first_appearance_time = None

    # 用于统计每次检测主phash是否出现（后面用于最后检测次数统计）
    main_phash = None

    for sample in phash_history:
        phash_valid = [clean_phash(p) for p in sample['phash']]
        phash_valid_filtered = [p for p in phash_valid if p is not None]
        confidence_per_sample.append(len(phash_valid_filtered))
        all_phash_list.extend(phash_valid_filtered)
    
    if not all_phash_list:
        # 无有效数据
        return None
    
    # 统计主phash
    counter = Counter(all_phash_list)
    max_count = max(counter.values())
    # 候选主phash，可能不止一个
    candidates = [ph for ph, cnt in counter.items() if cnt == max_count]

    # 确定主phash：在最近一次检测样本中最后出现的候选主phash
    last_sample_phash = [clean_phash(p) for p in phash_history[-1]['phash']]
    last_sample_phash = [p for p in last_sample_phash if p is not None]
    main_phash_candidates_in_last = [p for p in reversed(last_sample_phash) if p in candidates]
    main_phash = main_phash_candidates_in_last[0] if main_phash_candidates_in_last else candidates[0]

    # 主phash出现总次数
    main_phash_total_count = counter[main_phash]

    # 主phash在最后一次检测出现次数
    main_phash_last_count = last_sample_phash.count(main_phash)

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

    # 计算动态值 (dynamic_1) : 最近一次检测所有6帧两两phash不同的比例
    dynamic_1 = calc_dynamic_1(last_sample_phash)

    # 计算前三主phash重复比例 (top3_repeat_ratio)
    top3_repeat_ratio = sum(cnt for _, cnt in counter.most_common(3)) / total_frames if total_frames > 0 else 0

    # 计算锚点一致率 2秒和32秒
    anchor_consistency_2s = calc_anchor_consistency(phash_history, 0)  # 2秒抓帧点索引0
    anchor_consistency_32s = calc_anchor_consistency(phash_history, 4)  # 32秒抓帧点索引4

    # 历史动态稳定分（均值和方差相关，简单用均值做示例）
    dynamic_history_values = []
    for sample in phash_history:
        phash_valid = [clean_phash(p) for p in sample['phash']]
        phash_valid_filtered = [p for p in phash_valid if p is not None]
        if len(phash_valid_filtered) >= 2:
            dynamic_val = calc_dynamic_1(phash_valid_filtered)
            dynamic_history_values.append(dynamic_val)
    if dynamic_history_values:
        mean_dynamic = sum(dynamic_history_values) / len(dynamic_history_values)
        # 简单用方差示例，也可以扩展为标准差等
        variance_dynamic = sum((x - mean_dynamic) ** 2 for x in dynamic_history_values) / len(dynamic_history_values)
    else:
        mean_dynamic = 0
        variance_dynamic = 0

    # 计算评分
    score_current_dynamic = convert_dynamic_to_score(dynamic_1)
    score_history_stability = convert_history_dynamic_to_score(mean_dynamic, variance_dynamic)
    score_content_concentration = top3_repeat_ratio * 100
    score_anchor_bonus = ((anchor_consistency_2s + anchor_consistency_32s) / 2) * 100

    static_score = (
        WEIGHT_CURRENT_DYNAMIC * score_current_dynamic +
        WEIGHT_HISTORY_STABILITY * score_history_stability +
        WEIGHT_CONTENT_CONCENTRATION * score_content_concentration +
        WEIGHT_ANCHOR_BONUS * score_anchor_bonus
    )

    return {
        'main_phash': main_phash,
        'main_phash_total_count': main_phash_total_count,
        'main_phash_last_count': main_phash_last_count,
        'first_appearance_time': first_appearance_time,
        'current_confidence': current_confidence,
        'history_confidence': history_confidence,
        'dynamic_1': dynamic_1,
        'top3_repeat_ratio': top3_repeat_ratio,
        'anchor_consistency_2s': anchor_consistency_2s,
        'anchor_consistency_32s': anchor_consistency_32s,
        'mean_dynamic': mean_dynamic,
        'variance_dynamic': variance_dynamic,
        'score_current_dynamic': score_current_dynamic,
        'score_history_stability': score_history_stability,
        'score_content_concentration': score_content_concentration,
        'score_anchor_bonus': score_anchor_bonus,
        'static_score': static_score
    }


def calc_dynamic_1(phash_list):
    """
    计算某次检测的dynamic_1指标：所有两两phash两两对比不同的比例(%)
    phash_list长度可小于6（清理无效后）
    """
    if len(phash_list) < 2:
        return 100.0  # 极大动态，数据太少无法判断静态
    total_pairs = 0
    diff_pairs = 0
    for i in range(len(phash_list)):
        for j in range(i + 1, len(phash_list)):
            total_pairs += 1
            if phash_list[i] != phash_list[j]:
                diff_pairs += 1
    if total_pairs == 0:
        return 100.0
    return diff_pairs / total_pairs * 100


def calc_anchor_consistency(phash_history, frame_index):
    """
    计算锚点一致率（指定抓帧点frame_index）
    统计所有有效检测样本该抓帧点phash值相同的比例
    """
    valid_samples = []
    values = []
    for sample in phash_history:
        phash_list = sample['phash']
        p = clean_phash(phash_list[frame_index]) if frame_index < len(phash_list) else None
        if p is not None:
            valid_samples.append(sample)
            values.append(p)
    total_valid = len(values)
    if total_valid == 0:
        return 0.0
    # 统计出现次数最多的phash
    counter = Counter(values)
    max_count = max(counter.values())
    return max_count / total_valid


def convert_dynamic_to_score(dynamic_val):
    """
    将dynamic_1转换成静态得分，dynamic越小分越高（0~100）
    采用线性映射，dynamic 0->100，dynamic 100->0
    """
    score = max(0, min(100, 100 - dynamic_val))
    return score


def convert_history_dynamic_to_score(mean_dyn, var_dyn):
    """
    将历史动态均值和方差转换成稳定得分（0~100）
    均值和方差越低得分越高
    这里示范简单线性缩放（可根据实际调整）
    """
    # 假设动态均值和方差最大可能为100
    mean_score = max(0, min(100, 100 - mean_dyn))
    var_score = max(0, min(100, 100 - var_dyn * 50))  # 方差权重放大
    # 加权平均
    return 0.7 * mean_score + 0.3 * var_score
def hard_rule_judge(statistics):
    """
    根据硬规则判定是否静态假源，返回布尔值和筛除原因字符串
    满足所有条件且锚点一致率2s或32s满足阈值即通过
    """
    if statistics is None:
        return False, '无有效数据'

    cond_dynamic = statistics['dynamic_1'] <= THRESHOLD_DYNAMIC_1
    cond_phash_total = statistics['main_phash_total_count'] >= THRESHOLD_MAIN_PHASH_TOTAL_COUNT
    cond_top3_repeat = statistics['top3_repeat_ratio'] >= THRESHOLD_TOP3_REPEAT_RATIO
    cond_anchor_2s = statistics['anchor_consistency_2s'] >= THRESHOLD_ANCHOR_CONSISTENCY_2S
    cond_anchor_32s = statistics['anchor_consistency_32s'] >= THRESHOLD_ANCHOR_CONSISTENCY_32S

    anchor_cond = cond_anchor_2s or cond_anchor_32s

    if cond_dynamic and cond_phash_total and cond_top3_repeat and anchor_cond:
        reasons = []
        if cond_dynamic:
            reasons.append(f"动态值({statistics['dynamic_1']:.2f} ≤ {THRESHOLD_DYNAMIC_1})")
        if cond_phash_total:
            reasons.append(f"主phash总出现次数({statistics['main_phash_total_count']} ≥ {THRESHOLD_MAIN_PHASH_TOTAL_COUNT})")
        if cond_top3_repeat:
            reasons.append(f"前三主phash重复比例({statistics['top3_repeat_ratio']:.2%} ≥ {THRESHOLD_TOP3_REPEAT_RATIO:.2%})")
        if cond_anchor_2s:
            reasons.append(f"锚点一致率-2秒({statistics['anchor_consistency_2s']:.2%} ≥ {THRESHOLD_ANCHOR_CONSISTENCY_2S:.2%})")
        if cond_anchor_32s:
            reasons.append(f"锚点一致率-32秒({statistics['anchor_consistency_32s']:.2%} ≥ {THRESHOLD_ANCHOR_CONSISTENCY_32S:.2%})")
        reason_str = "硬规则命中: " + "，".join(reasons)
        return True, reason_str

    return False, ''


def score_rule_judge(statistics):
    """
    根据评分判定是否静态假源，返回布尔值和筛除原因字符串
    """
    if statistics is None:
        return False, '无有效数据'

    if statistics['static_score'] >= SCORE_STATIC_THRESHOLD:
        return True, f"评分高于阈值({statistics['static_score']:.2f} ≥ {SCORE_STATIC_THRESHOLD})"
    return False, ''

def is_new_source_today(first_appearance_time):
    if first_appearance_time is None:
        return True  # 无首次出现时间当新源处理
    now = datetime.now()
    return first_appearance_time.date() == now.date()

def generate_output_rows(csv_data, hash_data):
    """
    遍历csv条目，对应hash检测结果，计算统计及判定，准备输出行字典
    """
    total_rows = []
    ok_rows = []
    not_rows = []

    for row in csv_data:
        url = row.get('地址', '')
        phash_history = hash_data.get(url)

        stats = analyze_phash_data(phash_history) if phash_history else None

        # 判断静态假源
        if stats and is_new_source_today(stats['first_appearance_time']):
            is_static = False
            filter_reason = '新源不筛除'
        else:
            is_hard, reason_hard = hard_rule_judge(stats)
            is_score, reason_score = score_rule_judge(stats)

            is_static = False
            filter_reason = ''
            if is_hard:
                is_static = True
                filter_reason = reason_hard
            elif is_score:
                is_static = True
                filter_reason = reason_score

        # 构造新增列字典，统一字段，未命中为默认空值
        new_fields = {
            '筛除原因': filter_reason if is_static else '未筛除',
            '动态值（阈值 ≤ 10）': f"{stats['dynamic_1']:.2f}" if stats else '',
            '主phash总出现次数（阈值 ≥ 40）': stats['main_phash_total_count'] if stats else '',
            '主phash值': stats['main_phash'] if stats else '',
            '主phash最后检测出现次数': stats['main_phash_last_count'] if stats else '',
            '前三主phash重复比例（阈值 ≥ 70%）': f"{stats['top3_repeat_ratio']:.2%}" if stats else '',
            '锚点一致率-2秒（阈值 ≥ 95%）': f"{stats['anchor_consistency_2s']:.2%}" if stats else '',
            '锚点一致率-32秒（阈值 ≥ 95%）': f"{stats['anchor_consistency_32s']:.2%}" if stats else '',
            '静态评分': f"{stats['static_score']:.2f}" if stats else '',
            '最近一次动态分': f"{stats['score_current_dynamic']:.2f}" if stats else '',
            '历史动态稳定分': f"{stats['score_history_stability']:.2f}" if stats else '',
            '内容集中度分': f"{stats['score_content_concentration']:.2f}" if stats else '',
            '锚点软加分': f"{stats['score_anchor_bonus']:.2f}" if stats else '',
            '当前置信度': stats['current_confidence'] if stats else '',
            '历史置信度': f"{stats['history_confidence']:.4f}" if stats else '',
            '首次出现时间': stats['first_appearance_time'].strftime('%Y-%m-%d %H:%M:%S') if stats and stats['first_appearance_time'] else ''
        }

        # 合并原始列和新增列
        combined_row = dict(row)
        combined_row.update(new_fields)

        total_rows.append(combined_row)

        if is_static:
            not_rows.append(combined_row)
        else:
            ok_rows.append(combined_row)

    return total_rows, ok_rows, not_rows


def write_csv(filepath, rows, fieldnames):
    """写CSV文件"""
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

    # 确定输出字段名（输入所有字段 + 新增字段）
    base_fields = csv_data[0].keys() if csv_data else []
    added_fields = [
        '筛除原因',
        '动态值（阈值 ≤ 10）',
        '主phash总出现次数（阈值 ≥ 40）',
        '主phash值',
        '主phash最后检测出现次数',
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

# 新增打印统计信息
    print(f"筛选出静态假源条数（fake_scan_not.csv）：{len(not_rows)}")
    print(f"非静态源条数（fake_scan_ok.csv）：{len(ok_rows)}")
    print(f"总条目数（fake_scan_total.csv）：{len(total_rows)}")

    print("全部完成！")


if __name__ == '__main__':
    main()
