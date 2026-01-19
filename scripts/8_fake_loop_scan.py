#!/usr/bin/env python3
# coding: utf-8

import os
import glob
import json
import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# ================= 配置区（所有阈值参数统一配置） =================

# --- 时间与采样 ---
WINDOW_DAYS = 14                       # 期望历史天数窗口
SCAN_PER_DAY = 4                      # 每天检测次数
GRAB_TIMES = [2, 5, 9, 16, 32, 50]   # 抓帧秒数列表
EXPECTED_TOTAL_SCANS = WINDOW_DAYS * SCAN_PER_DAY
GRAB_POINTS = len(GRAB_TIMES)

# --- 动态评分阈值 ---
DYNAMIC_STATIC_THRESHOLD = 70  # 动态评分≥70视为静态（用于夜间停播判定）

# --- 夜间停播判定 ---
NIGHT_OFF_AIR_MIN_STATIC_SLOTS = 1    # 每天静态时段最少数量
NIGHT_OFF_AIR_MAX_STATIC_SLOTS = 2    # 每天静态时段最多数量
NIGHT_OFF_AIR_MIN_DYNAMIC_SLOTS = 2   # 每天动态时段最少数量
NIGHT_OFF_AIR_MAX_DYNAMIC_SLOTS = 3   # 每天动态时段最多数量
NIGHT_OFF_AIR_MIN_DAYS = 9             # 满足上述情况的天数阈值，达到则判定夜间停播

# --- 静态假源判定权重 ---
WEIGHTS_STATIC = {
    "last_score": 0.33,
    "hist_score": 0.23,
    "p_repeat_index": 0.14,
    "master_max_run": 0.14,
    "daily_max_run": 0.07,
    "audio_presence": 0.05,
    "fps_stability": 0.04,
}

# --- 轮播评分权重 ---
WEIGHTS_LOOP = {
    "p_repeat_index": 0.25,
    "master_max_run_length": 0.20,
    "anchor_AB_same_ratio": 0.20,
    "master_phash_span": 0.15,
    "top3_repeat_ratio": 0.15,
    "daily_max_run_length": 0.05,
}

# --- 其他配置 ---
INPUT_CSV = "output/middle/deep/deep_total_ok.csv"
HASH_DIR = "output/hash/merge"
OUTPUT_CSV = "output/middle/fake_loop/fake_loop_scan.csv"

# ==================== 输出列名中英映射 ====================

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
    "是否夜间停播": "是否夜间停播",
}

# ==================== 数据读取与预处理模块 ====================

def read_deep_csv(csv_path=INPUT_CSV):
    """
    读取基础源列表CSV，返回DataFrame。
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"基础CSV文件未找到: {csv_path}")
    df = pd.read_csv(csv_path)
    return df

def parse_hash_files(hash_dir=HASH_DIR):
    """
    解析hash目录所有json文件，提取检测时间戳和数据。
    返回：
      - timestamps: 按时间升序的检测时间戳列表（字符串）
      - data_map: dict{timestamp: dict{source_url: [{抓帧点数据字典}...]}}
    """
    files = glob.glob(os.path.join(hash_dir, "*.json"))
    time_file_map = {}
    for f in files:
        base = os.path.basename(f)
        # 例：文件名可能包含时间戳，需按实际文件名规则提取
        # 这里示范用文件名全名做key，稍后用文件创建时间或从内容取时间
        time_file_map[f] = None  # 暂留，后续读取文件确定时间戳

    timestamps = []
    data_map = {}

    for file_path in sorted(files):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = json.load(f)
            # 假设json结构为 {source_url: {timestamp: {...}}} 或其他
            # 这里提取文件时间戳，示范用文件名时间戳部分
            # 你根据文件名规则替换下面时间戳提取代码：
            base = os.path.basename(file_path)
            # 假设文件名格式如: 202601091736.json
            ts = base.split('.')[0]  
            timestamps.append(ts)
            data_map[ts] = content
        except Exception as e:
            print(f"警告：解析文件失败 {file_path}，原因: {e}")

    timestamps = sorted(timestamps)
    return timestamps, data_map

def build_source_matrix(df_sources, timestamps, data_map):
    """
    构建每个源的检测矩阵：
      - 行数：实际检测次数 N = len(timestamps)
      - 列数：抓帧点数量 6
      - 每个单元为 dict {"hash": phash或None, "status": 状态字符串}
    返回：
      - source_matrix_map: dict {source_url: List[List[dict]]} 结构为矩阵
      - scan_total_count: 实际检测次数 N
    """
    source_matrix_map = {}
    scan_total_count = len(timestamps)

    # 源列表唯一标识，假设用“地址”字段
    urls = df_sources['地址'].tolist()

    for url in urls:
        # 初始化空矩阵，行=scan次数，列=6抓帧点
        matrix = []
        for ts in timestamps:
            # 默认一行6个抓帧点，初始化空状态
            row = [{"hash": None, "status": "NOT_APPEARED"} for _ in range(GRAB_POINTS)]
            # 如果该时间点数据有该url，读取其hash列表和状态
            if ts in data_map and url in data_map[ts]:
                try:
                    # data_map[ts][url] 结构需和你实际json对应
                    # 这里示例为列表，每个元素含 "phash" 和 "status"
                    items = data_map[ts][url]
                    for i in range(min(GRAB_POINTS, len(items))):
                        phash_val = items[i].get("phash") if isinstance(items[i], dict) else None
                        status_val = items[i].get("status") if isinstance(items[i], dict) else "OK"
                        # None 或空字符串处理为 None
                        if not phash_val:
                            phash_val = None
                        row[i] = {"hash": phash_val, "status": status_val}
                except Exception as e:
                    print(f"警告：解析数据时异常，时间戳={ts}, url={url}, 错误：{e}")
            matrix.append(row)
        source_matrix_map[url] = matrix

    return source_matrix_map, scan_total_count
import math

# ==================== 单次检测动态评分模块 ====================

def calc_single_scan_dynamic_score(phash_list):
    """
    计算单次检测的动态评分和相关标记。
    输入：
      phash_list: 长度6的列表，元素为字符串phash或None
    输出：
      dict，包含以下字段：
        raw_score: 原始动态评分（无置信度加权，0完全动态，100完全静态）
        confidence: 有效phash数量（1~6）
        final_score: 置信度加权动态评分
        long_gop_flag: bool，长GOP预判
        loop_flag: bool，轮播预判
        null_flag: bool，全部无效标记
        partial_null_same_flag: bool，部分空但有效全部相同标记
    """
    valid_phash = [p for p in phash_list if p and len(p) > 0]
    confidence = len(valid_phash)

    # null_flag判断
    null_flag = confidence <= 1

    # partial_null_same_flag判断：部分空，但有效phash全部相同
    partial_null_same_flag = False
    if 1 < confidence < 6:
        unique_phash = set(valid_phash)
        if len(unique_phash) == 1:
            partial_null_same_flag = True

    # 差异统计
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

    # 置信度加权动态评分
    if null_flag or partial_null_same_flag:
        final_score = 100.0
    else:
        final_score = raw_score * (confidence / 6) + 50 * (1 - confidence / 6)

    # 长GOP预判（示例简单实现：检测连续重复块或少切换点）
    long_gop_flag = detect_long_gop(phash_list)

    # 轮播预判（示例简单实现：检测交替循环模式）
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
    """
    简单检测长GOP特征。
    规则示范：
      - 连续重复块（如 aaa bbb）
      - 少切换点（<=2次变化）
    返回bool
    """
    # 过滤None
    filtered = [p for p in phash_list if p]
    if len(filtered) <= 1:
        return False
    changes = 0
    for i in range(len(filtered) - 1):
        if filtered[i] != filtered[i+1]:
            changes += 1
    if changes <= 2:
        return True
    return False

def detect_loop_pattern(phash_list):
    """
    简单检测轮播交替循环模式。
    规则示范：
      - ababab 或 aabbaa 等模式的存在
    返回bool
    """
    filtered = [p for p in phash_list if p]
    if len(filtered) < 4:
        return False

    # 轮播模式检测示范：交替模式检测
    # 取2个元素循环或镜像
    pattern1 = filtered[0:2]
    repeated1 = True
    for i in range(0, len(filtered), 2):
        if filtered[i:i+2] != pattern1:
            repeated1 = False
            break
    if repeated1:
        return True

    # 镜像检测 aabbaa 类型
    half = len(filtered) // 2
    first_half = filtered[:half]
    second_half = filtered[-half:]
    if first_half == second_half[::-1]:
        return True

    return False

# ==================== 多次检测横向比较模块 ====================

def analyze_long_term_metrics(source_matrix, timestamps):
    """
    输入：
      source_matrix: List[List[dict]], 单源矩阵，行=检测次数，列=抓帧点数
      timestamps: 时间戳列表，按升序排列
    输出：
      dict，包含统计指标：
        master_phash
        master_phash_count
        p_repeat_index
        history_dynamic_level
        master_phash_span
        top3_repeat_ratio
        anchor_AB_same_ratio
        master_max_run_length
        daily_max_run_length
        sample_total_count
        sample_valid_count
        dynamic_sample_count
    """

    sample_total_count = len(source_matrix)
    sample_valid_count = 0
    dynamic_sample_count = 0

    # 主phash计数
    phash_counter = Counter()

    # 每次检测的动态评分（final_score）
    dynamic_scores = []

    # 用于锚点一致率计算的两点索引
    anchor_A_idx = 0   # 第一个抓帧点（2秒）
    anchor_B_idx = 4   # 第五个抓帧点（32秒）

    # 每个采样的主phash（出现最多的有效phash）
    sample_master_phash_list = []

    # 每天分组（key=日期字符串，value=list of sample indices）
    day_groups = defaultdict(list)
    for idx, ts in enumerate(timestamps):
        day_str = ts[:8]  # 假设时间戳格式 YYYYMMDDHHMM
        day_groups[day_str].append(idx)

    for i, row in enumerate(source_matrix):
        # 取该次检测的所有phash值（有效的）
        phash_list = []
        for cell in row:
            if cell["hash"]:
                phash_list.append(cell["hash"])
            else:
                phash_list.append(None)

        # 计算动态评分
        single_score_dict = calc_single_scan_dynamic_score(phash_list)
        dynamic_scores.append(single_score_dict["final_score"])

        # 统计有效检测，至少2个有效phash
        valid_count = sum([1 for p in phash_list if p])
        if valid_count >= 2:
            sample_valid_count += 1
            # 记录动态样本数，final_score < 100视为动态
            if single_score_dict["final_score"] < 100:
                dynamic_sample_count += 1

            # 统计该采样主phash（出现次数最多）
            counter = Counter([p for p in phash_list if p])
            if counter:
                master_phash = counter.most_common(1)[0][0]
                phash_counter[master_phash] += 1
                sample_master_phash_list.append(master_phash)
            else:
                sample_master_phash_list.append(None)
        else:
            sample_master_phash_list.append(None)

    # 计算主phash和相关指标
    if phash_counter:
        master_phash, master_phash_count = phash_counter.most_common(1)[0]
        p_repeat_index = master_phash_count / sample_total_count
    else:
        master_phash = None
        master_phash_count = 0
        p_repeat_index = 0.0

    # 历史动态级，动态样本次数占有效检测次数比例
    if sample_valid_count > 0:
        history_dynamic_level = (dynamic_sample_count / sample_valid_count) * 100
    else:
        history_dynamic_level = 0.0

    # 主phash时间跨度
    first_idx = None
    last_idx = None
    for idx, phash in enumerate(sample_master_phash_list):
        if phash == master_phash:
            if first_idx is None:
                first_idx = idx
            last_idx = idx
    if first_idx is not None and last_idx is not None:
        span_count = last_idx - first_idx + 1
        master_phash_span = span_count / sample_total_count
    else:
        master_phash_span = 0.0

    # phash分布离散度（前三高频占比）
    top3 = phash_counter.most_common(3)
    top3_sum = sum([cnt for _, cnt in top3])
    if sample_total_count > 0:
        top3_repeat_ratio = top3_sum / sample_total_count
    else:
        top3_repeat_ratio = 0.0

    # 锚点一致率计算（第1和第5抓帧点）
    anchor_same_count = 0
    anchor_total_count = 0
    for row in source_matrix:
        a = row[anchor_A_idx]["hash"]
        b = row[anchor_B_idx]["hash"]
        if a and b:
            anchor_total_count += 1
            if a == b:
                anchor_same_count += 1
    if anchor_total_count > 0:
        anchor_AB_same_ratio = anchor_same_count / anchor_total_count
    else:
        anchor_AB_same_ratio = 0.0

    # 最大连续重复段长度统计（主phash连续出现次数）
    max_run_length = 0
    current_run = 0
    for phash in sample_master_phash_list:
        if phash == master_phash:
            current_run += 1
            if current_run > max_run_length:
                max_run_length = current_run
        else:
            current_run = 0
    master_max_run_length = max_run_length / sample_total_count if sample_total_count > 0 else 0.0

    # 单天最长连续重复段长度（按天分段计算最大连续）
    daily_max_runs = []
    for day, indices in day_groups.items():
        max_run = 0
        cur_run = 0
        for idx in indices:
            if idx >= len(sample_master_phash_list):
                continue
            if sample_master_phash_list[idx] == master_phash:
                cur_run += 1
                if cur_run > max_run:
                    max_run = cur_run
            else:
                cur_run = 0
        if len(indices) > 0:
            daily_max_runs.append(max_run / len(indices))
    if daily_max_runs:
        daily_max_run_length = max(daily_max_runs)
    else:
        daily_max_run_length = 0.0

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
        "sample_total_count": sample_total_count,
        "sample_valid_count": sample_valid_count,
        "dynamic_sample_count": dynamic_sample_count,
        "dynamic_scores": dynamic_scores,
    }

# ==================== 夜间停播判定模块 ====================

def judge_night_off_air(dynamic_scores, timestamps):
    """
    判断是否存在夜间停播现象。
    规则：
      - 按天分组，每天4次检测
      - 判断当天是否有1~2次检测静态(>=DYNAMIC_STATIC_THRESHOLD)，其余检测动态(<阈值)
      - 若历史中满足上述条件天数≥NIGHT_OFF_AIR_MIN_DAYS，则判定夜间停播
    输入：
      dynamic_scores: List[float], 按时间顺序的每次检测动态评分
      timestamps: List[str], 时间戳列表
    输出：
      tuple(bool, list, float)
      是否夜间停播, 夜间停播天数列表, 夜间停播比例(0~100%)
    """
    # 按日期分组评分
    day_groups = defaultdict(list)
    for idx, ts in enumerate(timestamps):
        day_str = ts[:8]  # YYYYMMDD
        day_groups[day_str].append(dynamic_scores[idx])

    night_off_air_days = 0
    total_days = len(day_groups)
    night_off_air_days_list = []

    for day, scores in day_groups.items():
        static_count = sum(1 for s in scores if s >= DYNAMIC_STATIC_THRESHOLD)
        dynamic_count = sum(1 for s in scores if s < DYNAMIC_STATIC_THRESHOLD)
        # 判断条件
        if (NIGHT_OFF_AIR_MIN_STATIC_SLOTS <= static_count <= NIGHT_OFF_AIR_MAX_STATIC_SLOTS) and \
           (NIGHT_OFF_AIR_MIN_DYNAMIC_SLOTS <= dynamic_count <= NIGHT_OFF_AIR_MAX_DYNAMIC_SLOTS) and \
           (len(scores) == SCAN_PER_DAY):
            night_off_air_days += 1
            night_off_air_days_list.append(day)

    night_off_air_ratio = (night_off_air_days / total_days) * 100 if total_days > 0 else 0.0
    is_night_off_air = night_off_air_days >= NIGHT_OFF_AIR_MIN_DAYS

    return is_night_off_air, night_off_air_days_list, night_off_air_ratio
# ==================== 轮播评分计算模块 ====================

def normalize_score(value, max_val=1.0, min_val=0.0):
    """
    归一化分数到0~100区间，默认max_val=1，min_val=0。
    如果value超出范围，裁剪到边界。
    """
    if value is None:
        return 0.0
    v = max(min_val, min(max_val, value))
    norm = (v - min_val) / (max_val - min_val) * 100
    return norm

def calc_loop_score(metrics):
    """
    计算轮播综合评分，输入为多次检测统计指标dict。
    返回：
      - loop_score: 0~100浮点
      - loop_level: 轮播等级字符串
    """
    score = 0.0
    # 权重计算，指标归一化，数值范围根据指标本身特性
    score += WEIGHTS_LOOP["p_repeat_index"] * normalize_score(metrics.get("p_repeat_index", 0.0))
    score += WEIGHTS_LOOP["master_max_run_length"] * normalize_score(metrics.get("master_max_run_length", 0.0))
    score += WEIGHTS_LOOP["anchor_AB_same_ratio"] * normalize_score(metrics.get("anchor_AB_same_ratio", 0.0))
    score += WEIGHTS_LOOP["master_phash_span"] * normalize_score(metrics.get("master_phash_span", 0.0))
    score += WEIGHTS_LOOP["top3_repeat_ratio"] * normalize_score(metrics.get("top3_repeat_ratio", 0.0))
    score += WEIGHTS_LOOP["daily_max_run_length"] * normalize_score(metrics.get("daily_max_run_length", 0.0))

    # 限制在0~100之间
    loop_score = max(0, min(100, score))

    # 等级判定
    if loop_score < 25:
        loop_level = "无轮播特征"
    elif loop_score < 50:
        loop_level = "轻微轮播特征"
    elif loop_score < 75:
        loop_level = "明显轮播特征"
    else:
        loop_level = "强烈轮播特征"

    return loop_score, loop_level

# ==================== 静态假源评分模块 ====================

def calc_fake_score(metrics, last_score, hist_score, audio_presence=100, fps_stability=100):
    """
    计算静态假源综合评分，结合历史与当前动态评分及辅助指标。
    参数：
      - metrics: 多次检测统计指标dict
      - last_score: 最近一次动态评分（0-100，越大越静态）
      - hist_score: 历史动态评分（0-100，越大越静态）
      - audio_presence: 音频存在性评分（0-100）
      - fps_stability: 帧率稳定性评分（0-100）
    返回：
      - fake_score: 0~100浮点
      - fake_level: 等级字符串
    """
    p_repeat_index = metrics.get("p_repeat_index", 0.0)
    master_max_run = metrics.get("master_max_run_length", 0.0)
    daily_max_run = metrics.get("daily_max_run_length", 0.0)

    score = 0.0
    score += WEIGHTS_STATIC["last_score"] * last_score
    score += WEIGHTS_STATIC["hist_score"] * hist_score
    score += WEIGHTS_STATIC["p_repeat_index"] * p_repeat_index * 100
    score += WEIGHTS_STATIC["master_max_run"] * master_max_run * 100
    score += WEIGHTS_STATIC["daily_max_run"] * daily_max_run * 100
    score += WEIGHTS_STATIC["audio_presence"] * audio_presence
    score += WEIGHTS_STATIC["fps_stability"] * fps_stability

    fake_score = max(0, min(100, score))

    # 等级判定
    if fake_score < 25:
        fake_level = "动态源"
    elif fake_score < 50:
        fake_level = "疑似动态源"
    elif fake_score < 75:
        fake_level = "疑似静态源"
    else:
        fake_level = "静态源"

    return fake_score, fake_level

# ==================== 结果整合与CSV输出模块 ====================

def integrate_and_output(df_sources, timestamps, source_matrix_map, scan_total_count, 
                         last_scores_map, last_conf_map, long_gop_flags_map, loop_flags_map, 
                         multi_metrics_map, night_off_air_map, output_csv=OUTPUT_CSV):
    """
    整合所有指标，输出最终CSV。
    参数：
      df_sources: 原始DataFrame
      timestamps: 时间戳列表
      source_matrix_map: 源检测矩阵map
      scan_total_count: 实际检测次数
      last_scores_map: dict{url: float} 最近一次动态评分
      last_conf_map: dict{url: int} 最近一次置信度
      long_gop_flags_map: dict{url: bool}
      loop_flags_map: dict{url: bool}
      multi_metrics_map: dict{url: dict} 多次检测指标
      night_off_air_map: dict{url: bool} 是否夜间停播标记
    """
    output_rows = []

    for idx, row in df_sources.iterrows():
        url = row['地址']
        metrics = multi_metrics_map.get(url, {})
        last_score = last_scores_map.get(url, 100.0)
        last_conf = last_conf_map.get(url, 0)
        long_gop_flag = long_gop_flags_map.get(url, False)
        loop_flag = loop_flags_map.get(url, False)
        night_off_air = night_off_air_map.get(url, False)

        # 历史动态评分和置信度取自multi_metrics
        hist_score = metrics.get("history_dynamic_level", 0.0)
        hist_confidence = metrics.get("sample_valid_count", 0)

        # 计算轮播评分等级
        loop_score, loop_level = calc_loop_score(metrics)

        # 假设音频和帧率辅助指标暂时固定100（可扩展）
        audio_presence = 100
        fps_stability = 100

        # 计算静态假源评分等级
        fake_score, fake_level = calc_fake_score(metrics, last_score, hist_score, audio_presence, fps_stability)

        # 合并原始行数据转成dict
        output_row = row.to_dict()

        # 添加新字段
        output_row.update({
            "scan_total_count": scan_total_count,
            "scan_valid_ratio": (metrics.get("sample_valid_count",0) / scan_total_count) if scan_total_count > 0 else 0.0,
            "first_seen_ts": timestamps[0] if timestamps else "",
            "S_last": last_score,
            "C_last": last_conf,
            "long_gop_flag": int(long_gop_flag),
            "loop_flag": int(loop_flag),
            "sample_total_count": metrics.get("sample_total_count", 0),
            "sample_valid_count": metrics.get("sample_valid_count", 0),
            "dynamic_sample_count": metrics.get("dynamic_sample_count", 0),
            "S_hist": hist_score,
            "C_hist": hist_confidence,
            "master_phash": metrics.get("master_phash", ""),
            "master_phash_count": metrics.get("master_phash_count", 0),
            "p_repeat_index": round(metrics.get("p_repeat_index", 0.0), 4),
            "master_phash_span": round(metrics.get("master_phash_span", 0.0), 4),
            "top3_repeat_ratio": round(metrics.get("top3_repeat_ratio", 0.0), 4),
            "anchor_AB_same_ratio": round(metrics.get("anchor_AB_same_ratio", 0.0), 4),
            "master_max_run_length": round(metrics.get("master_max_run_length", 0.0), 4),
            "daily_max_run_length": round(metrics.get("daily_max_run_length", 0.0), 4),
            "loop_score": round(loop_score, 4),
            "loop_level": loop_level,
            "fake_score": round(fake_score, 4),
            "fake_level": fake_level,
            "night_off_air_flag": int(night_off_air),
        })

        output_rows.append(output_row)

    # 生成DataFrame并输出CSV
    df_out = pd.DataFrame(output_rows)
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df_out.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"已输出结果文件：{output_csv}")

# ==================== 主流程示例 ====================

def main():
    print("开始读取基础CSV...")
    df_sources = read_deep_csv()

    print("解析hash文件...")
    timestamps, data_map = parse_hash_files()

    print(f"构建检测矩阵，源数量：{len(df_sources)}, 检测次数：{len(timestamps)}")
    source_matrix_map, scan_total_count = build_source_matrix(df_sources, timestamps, data_map)

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

    print("整合输出CSV文件...")
    integrate_and_output(df_sources, timestamps, source_matrix_map, scan_total_count, 
                         last_scores_map, last_conf_map, long_gop_flags_map, loop_flags_map, 
                         multi_metrics_map, night_off_air_map)

    print("全部处理完成。")

if __name__ == "__main__":
    main()
