#!/usr/bin/env python3
# coding: utf-8

import os
import glob
import json
import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# ================= 配置区（所有阈值参数统一配置） =================

WINDOW_DAYS = 14
SCAN_PER_DAY = 4
GRAB_TIMES = [2, 5, 9, 16, 32, 50]
EXPECTED_TOTAL_SCANS = WINDOW_DAYS * SCAN_PER_DAY
GRAB_POINTS = len(GRAB_TIMES)

DYNAMIC_STATIC_THRESHOLD = 70
NIGHT_OFF_AIR_MIN_STATIC_SLOTS = 1
NIGHT_OFF_AIR_MAX_STATIC_SLOTS = 2
NIGHT_OFF_AIR_MIN_DYNAMIC_SLOTS = 2
NIGHT_OFF_AIR_MAX_DYNAMIC_SLOTS = 3
NIGHT_OFF_AIR_MIN_DAYS = 9

WEIGHTS_STATIC = {
    "last_score": 0.33,
    "hist_score": 0.23,
    "p_repeat_index": 0.14,
    "master_max_run": 0.14,
    "daily_max_run": 0.07,
    "audio_presence": 0.05,
    "fps_stability": 0.04,
}

WEIGHTS_LOOP = {
    "p_repeat_index": 0.25,
    "master_max_run_length": 0.20,
    "anchor_AB_same_ratio": 0.20,
    "master_phash_span": 0.15,
    "top3_repeat_ratio": 0.15,
    "daily_max_run_length": 0.05,
}

OUTPUT_COLUMN_RENAME_MAP = {
    "scan_total_count": "实际检测次数",
    "scan_valid_ratio": "有效检测率",
    "first_seen_ts": "首次出现时间",
    "S_last": "当前动态评分",
    "C_last": "当前检测置信度",
    "long_gop_flag": "长 GOP 预判",
    "loop_flag": "轮播预判",
    "sample_total_count": "总采样次数",
    "sample_valid_count": "有效采样次数",
    "dynamic_sample_count": "动态样本次数",
    "S_hist": "历史动态评分",
    "C_hist": "历史置信度",
    "master_phash": "主 phash",
    "master_phash_count": "主 phash 出现次数",
    "p_repeat_index": "重复指数",
    "master_phash_span": "时间跨度覆盖度",
    "top3_repeat_ratio": "phash 集中度",
    "anchor_AB_same_ratio": "锚点一致率",
    "master_max_run_length": "最大连续重复长度",
    "daily_max_run_length": "单天最大连续重复",
    "loop_score": "轮播源评分",
    "loop_level": "轮播源等级",
    "fake_score": "静态源评分",
    "fake_level": "静态源等级",
    "night_off_air_flag": "是否夜间停播",
}

INPUT_CSV = "output/middle/deep/deep_total_ok.csv"
HASH_DIR = "output/hash/merge"
OUTPUT_CSV = "output/middle/fake_loop/fake_loop_scan.csv"

# ==================== 数据读取与预处理模块 ====================

def read_deep_csv(csv_path=INPUT_CSV):
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"基础CSV文件未找到: {csv_path}")
    df = pd.read_csv(csv_path)
    return df

def parse_hash_files(hash_dir=HASH_DIR):
    files = glob.glob(os.path.join(hash_dir, "*.json"))
    timestamps = []
    data_map = {}

    for file_path in sorted(files):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = json.load(f)

            base = os.path.basename(file_path)
            ts = base.split('.')[0]
            timestamps.append(ts)

            converted = {}
            for url, val in content.items():
                phash_list = val.get("phash", [])
                items = []
                for phash in phash_list:
                    items.append({"phash": phash if phash else None, "status": "OK"})
                converted[url] = items
            data_map[ts] = converted

        except Exception as e:
            print(f"警告：解析文件失败 {file_path}，原因: {e}")

    timestamps = sorted(timestamps)
    return timestamps, data_map

def build_source_matrix(df_sources, timestamps, data_map):
    source_matrix_map = {}
    # 不再使用统一scan_total_count，改为存每个url实际检测次数
    # 暂时不返回统一的scan_total_count

    urls = df_sources['地址'].tolist()

    for url in urls:
        matrix = []
        for ts in timestamps:
            row = [{"hash": None, "status": "NOT_APPEARED"} for _ in range(GRAB_POINTS)]
            if ts in data_map and url in data_map[ts]:
                try:
                    items = data_map[ts][url]
                    for i in range(min(GRAB_POINTS, len(items))):
                        phash_val = items[i].get("phash") if isinstance(items[i], dict) else None
                        status_val = items[i].get("status") if isinstance(items[i], dict) else "OK"
                        if not phash_val:
                            phash_val = None
                        row[i] = {"hash": phash_val, "status": status_val}
                except Exception as e:
                    print(f"警告：解析数据时异常，时间戳={ts}, url={url}, 错误：{e}")
            matrix.append(row)
        source_matrix_map[url] = matrix

    return source_matrix_map

# ==================== 单次检测动态评分模块 ====================

def calc_single_scan_dynamic_score(phash_list):
    valid_phash = [p for p in phash_list if p and len(p) > 0]
    confidence = len(valid_phash)

    null_flag = confidence <= 1

    partial_null_same_flag = False
    if 1 < confidence < 6:
        unique_phash = set(valid_phash)
        if len(unique_phash) == 1:
            partial_null_same_flag = True

    diff_pairs = 0
    total_pairs = 0
    for i in range(confidence):
        for j in range(i + 1, confidence):
            total_pairs += 1
            if valid_phash[i] != valid_phash[j]:
                diff_pairs += 1

    if total_pairs == 0:
        raw_score = 100.0
    else:
        diff_ratio = diff_pairs / total_pairs
        raw_score = 100 * (1 - diff_ratio)

    if null_flag or partial_null_same_flag:
        final_score = 100.0
    else:
        final_score = raw_score * (confidence / 6) + 50 * (1 - confidence / 6)

    long_gop_flag = detect_long_gop(phash_list)
    loop_flag = detect_loop_pattern(phash_list)

    return {
        "raw_score": raw_score,
        "confidence": confidence,
        "final_score": final_score,
        "long_gop_flag": long_gop_flag,
        "loop_flag": loop_flag,
        "null_flag": null_flag,
        "partial_null_same_flag": partial_null_same_flag,
    }

def detect_long_gop(phash_list):
    filtered = [p for p in phash_list if p]
    if len(filtered) <= 1:
        return False
    changes = 0
    for i in range(len(filtered) - 1):
        if filtered[i] != filtered[i+1]:
            changes += 1
    return changes <= 2

def detect_loop_pattern(phash_list):
    filtered = [p for p in phash_list if p]
    if len(filtered) < 4:
        return False
    pattern1 = filtered[0:2]
    repeated1 = True
    for i in range(0, len(filtered), 2):
        if filtered[i:i+2] != pattern1:
            repeated1 = False
            break
    if repeated1:
        return True
    half = len(filtered) // 2
    first_half = filtered[:half]
    second_half = filtered[-half:]
    if first_half == second_half[::-1]:
        return True
    return False

# ==================== 多次检测横向比较模块（含修改） ====================

def analyze_long_term_metrics(source_matrix, timestamps):
    """
    修改要点：
    - 实际检测次数：统计该URL在哪些时间戳至少有一个有效phash（非None）
    - 有效检测次数：该URL检测次数中有效phash≥2的次数
    - 主phash出现次数：统计所有抓帧点中主phash出现的总次数（非采样数）
    - p_repeat_index = 主phash出现总次数 / (实际检测次数 × 抓帧点数)
    - master_phash_span：主phash首次出现时间戳到最后出现时间戳跨度（天）
    """
    actual_detected_indices = []
    sample_valid_count = 0
    dynamic_sample_count = 0
    phash_counter = Counter()
    dynamic_scores = []

    anchor_A_idx = 0
    anchor_B_idx = 4
    sample_master_phash_list = []

    day_groups = defaultdict(list)
    for idx, ts in enumerate(timestamps):
        day_str = ts[:8]
        day_groups[day_str].append(idx)

    for i, row in enumerate(source_matrix):
        # 判断该行是否有效检测（至少一抓帧有hash）
        if any(cell["hash"] for cell in row):
            actual_detected_indices.append(i)

    for i in actual_detected_indices:
        row = source_matrix[i]
        phash_list = [cell["hash"] if cell["hash"] else None for cell in row]

        single_score_dict = calc_single_scan_dynamic_score(phash_list)
        dynamic_scores.append(single_score_dict["final_score"])

        valid_count = sum(1 for p in phash_list if p)
        if valid_count >= 2:
            sample_valid_count += 1
            if single_score_dict["final_score"] < 100:
                dynamic_sample_count += 1
            counter = Counter([p for p in phash_list if p])
            if counter:
                master_phash = counter.most_common(1)[0][0]
                phash_counter[master_phash] += 1
                sample_master_phash_list.append(master_phash)
            else:
                sample_master_phash_list.append(None)
        else:
            sample_master_phash_list.append(None)

    actual_scan_total = len(actual_detected_indices)

    # 重新计算主phash出现总次数（所有抓帧点）
    master_phash = None
    master_phash_count = 0
    if phash_counter:
        master_phash, _ = phash_counter.most_common(1)[0]
        # 统计所有检测中，所有抓帧点主phash出现的总次数
        total_master_count = 0
        for i in actual_detected_indices:
            row = source_matrix[i]
            for cell in row:
                if cell["hash"] == master_phash:
                    total_master_count += 1
        master_phash_count = total_master_count

    # p_repeat_index
    if actual_scan_total > 0:
        p_repeat_index = master_phash_count / (actual_scan_total * GRAB_POINTS)
    else:
        p_repeat_index = 0.0

    # history_dynamic_level
    if sample_valid_count > 0:
        history_dynamic_level = (dynamic_sample_count / sample_valid_count) * 100
    else:
        history_dynamic_level = 0.0

    # master_phash_span: 主phash首次和最后出现时间跨度，单位天
    first_ts = None
    last_ts = None
    if master_phash is not None:
        first_idx = None
        last_idx = None
        for idx, phash in enumerate(sample_master_phash_list):
            if phash == master_phash:
                if first_idx is None:
                    first_idx = idx
                last_idx = idx
        if first_idx is not None and last_idx is not None:
            first_ts_str = timestamps[actual_detected_indices[first_idx]]
            last_ts_str = timestamps[actual_detected_indices[last_idx]]
            try:
                dt_first = datetime.strptime(first_ts_str[:8], "%Y%m%d")
                dt_last = datetime.strptime(last_ts_str[:8], "%Y%m%d")
                span_days = (dt_last - dt_first).days
                master_phash_span = float(span_days)
            except Exception:
                master_phash_span = 0.0
        else:
            master_phash_span = 0.0
    else:
        master_phash_span = 0.0

    # top3_repeat_ratio
    top3 = phash_counter.most_common(3)
    top3_sum = sum([cnt for _, cnt in top3])
    top3_repeat_ratio = top3_sum / actual_scan_total if actual_scan_total > 0 else 0.0

    # anchor_AB_same_ratio
    anchor_same_count = 0
    anchor_total_count = 0
    for i in actual_detected_indices:
        row = source_matrix[i]
        a = row[anchor_A_idx]["hash"]
        b = row[anchor_B_idx]["hash"]
        if a and b:
            anchor_total_count += 1
            if a == b:
                anchor_same_count += 1
    anchor_AB_same_ratio = anchor_same_count / anchor_total_count if anchor_total_count > 0 else 0.0

    # master_max_run_length
    max_run_length = 0
    current_run = 0
    for phash in sample_master_phash_list:
        if phash == master_phash:
            current_run += 1
            max_run_length = max(max_run_length, current_run)
        else:
            current_run = 0
    master_max_run_length = max_run_length / actual_scan_total if actual_scan_total > 0 else 0.0

    # daily_max_run_length
    daily_max_runs = []
    for day, indices in day_groups.items():
        max_run = 0
        cur_run = 0
        for idx in indices:
            if idx >= len(sample_master_phash_list):
                continue
            if sample_master_phash_list[idx] == master_phash:
                cur_run += 1
                max_run = max(max_run, cur_run)
            else:
                cur_run = 0
        if len(indices) > 0:
            daily_max_runs.append(max_run / len(indices))
    daily_max_run_length = max(daily_max_runs) if daily_max_runs else 0.0

    return {
        "master_phash": master_phash,
        "master_phash_count": master_phash_count,
        "p_repeat_index": p_repeat_index,
        "history_dynamic_level": history_dynamic_level,
        "master_phash_span": master_phash_span,
        "top3_repeat_ratio": top3_repeat_ratio,
        "anchor_AB_same_ratio": anchor_AB_same_ratio,
        "master_max_run_length": master_max_run_length,
        "daily_max_run_length": daily_max_run_length,
        "sample_total_count": actual_scan_total,
        "sample_valid_count": sample_valid_count,
        "dynamic_sample_count": dynamic_sample_count,
        "dynamic_scores": dynamic_scores,
        "actual_detected_indices": actual_detected_indices,
    }
# ==================== 长 GOP 动态补偿模块 ====================

def apply_long_gop_dynamic_compensation(source_matrix_map, multi_metrics_map):
    """
    针对存在长 GOP 特征的源，动态补偿其评分，防止误判静态。
    规则示例：
      - 若某次检测标记为长 GOP，且动态评分高于某阈值，调整该检测动态评分为动态值
      - 更新 multi_metrics_map 中的 dynamic_scores 数据
    """
    COMPENSATION_THRESHOLD = 80  # 低于此值视为需要补偿
    COMPENSATION_SCORE = 50      # 补偿后的动态评分

    for url, matrix in source_matrix_map.items():
        if url not in multi_metrics_map:
            continue
        metrics = multi_metrics_map[url]
        dynamic_scores = metrics.get("dynamic_scores", [])
        if not dynamic_scores:
            continue

        # 获取该源所有检测的长GOP标记（bool列表）
        long_gop_flags = []
        for row in matrix:
            phash_list = [cell["hash"] for cell in row]
            long_gop_flags.append(detect_long_gop(phash_list))

        # 补偿逻辑
        adjusted_scores = []
        for i, score in enumerate(dynamic_scores):
            if i < len(long_gop_flags) and long_gop_flags[i]:
                # 如果长GOP且评分高于补偿阈值，则补偿为COMPENSATION_SCORE
                if score >= COMPENSATION_THRESHOLD:
                    adjusted_scores.append(COMPENSATION_SCORE)
                else:
                    adjusted_scores.append(score)
            else:
                adjusted_scores.append(score)

        # 更新补偿后的评分
        metrics["dynamic_scores"] = adjusted_scores
        multi_metrics_map[url] = metrics

# ==================== 主流程调用补偿函数 ====================

def main():
    print("开始读取基础CSV...")
    df_sources = read_deep_csv()

    print("解析hash文件...")
    timestamps, data_map = parse_hash_files()

    print(f"构建检测矩阵，源数量：{len(df_sources)}, 检测次数：{len(timestamps)}")
    source_matrix_map = build_source_matrix(df_sources, timestamps, data_map)

    # 准备存储各项指标
    last_scores_map = {}
    last_conf_map = {}
    long_gop_flags_map = {}
    loop_flags_map = {}
    multi_metrics_map = {}
    night_off_air_map = {}

    print("计算各源指标...")

    for url, matrix in source_matrix_map.items():
        # 最近一次检测行
        if len(matrix) == 0:
            continue
        last_row = matrix[-1]
        last_phash_list = [cell["hash"] for cell in last_row]
        single_score_dict = calc_single_scan_dynamic_score(last_phash_list)
        last_scores_map[url] = single_score_dict["final_score"]
        last_conf_map[url] = single_score_dict["confidence"]
        long_gop_flags_map[url] = single_score_dict["long_gop_flag"]
        loop_flags_map[url] = single_score_dict["loop_flag"]

        # 多次检测统计
        metrics = analyze_long_term_metrics(matrix, timestamps)
        multi_metrics_map[url] = metrics

        # 夜间停播判定
        is_night_off_air, _, _ = judge_night_off_air(metrics.get("dynamic_scores", []), timestamps)
        night_off_air_map[url] = is_night_off_air

    print("应用长 GOP 动态补偿...")
    apply_long_gop_dynamic_compensation(source_matrix_map, multi_metrics_map)

    print("整合输出CSV文件...")
    integrate_and_output(df_sources, timestamps, source_matrix_map, scan_total_count, 
                         last_scores_map, last_conf_map, long_gop_flags_map, loop_flags_map, 
                         multi_metrics_map, night_off_air_map)

    print("全部处理完成。")

if __name__ == "__main__":
    main()
