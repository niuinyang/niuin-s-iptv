#!/usr/bin/env python3
# scripts/5.3final_scan_advanced.py

import argparse
import csv
import asyncio
from asyncio.subprocess import create_subprocess_exec, PIPE
from PIL import Image
import io
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore
import json
import os
import chardet
import numpy as np

# 缓存目录及缓存文件路径
CACHE_DIR = "output/cache"
TOTAL_CACHE_FILE = os.path.join(CACHE_DIR, "total_cache.json")

# 图像哈希参数，8x8大小，64位哈希
HASH_SIZE = 8
HASH_BITS = HASH_SIZE * HASH_SIZE  # 64


# ----------- 图像哈希计算函数 -----------

def image_to_ahash_bytes(img_bytes, hash_size=HASH_SIZE):
    """
    计算图像的平均哈希（aHash）
    """
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize((hash_size, hash_size), Image.Resampling.LANCZOS)
    pixels = list(im.getdata())
    avg = sum(pixels) / len(pixels)
    bits = 0
    for p in pixels:
        bits = (bits << 1) | (1 if p > avg else 0)
    return bits


def image_to_phash_bytes(img_bytes, hash_size=HASH_SIZE):
    """
    计算图像的感知哈希（pHash）
    """
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize((32, 32), Image.Resampling.LANCZOS)
    pixels = np.array(im, dtype=np.float32)
    dct = dct2(pixels)
    dct_low_freq = dct[:hash_size, :hash_size]
    avg = dct_low_freq[1:, 1:].mean()
    bits = 0
    for v in dct_low_freq.flatten():
        bits = (bits << 1) | (1 if v > avg else 0)
    return bits


def dct2(a):
    """
    2D DCT变换，用FFT近似计算
    """
    return np.round(np.real(np.fft.fft2(a)))


def image_to_dhash_bytes(img_bytes, hash_size=HASH_SIZE):
    """
    计算图像的差值哈希（dHash）
    """
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
    pixels = list(im.getdata())
    bits = 0
    for row in range(hash_size):
        for col in range(hash_size):
            left_pixel = pixels[row * (hash_size + 1) + col]
            right_pixel = pixels[row * (hash_size + 1) + col + 1]
            bits = (bits << 1) | (1 if left_pixel > right_pixel else 0)
    return bits


def hamming(a, b):
    """
    计算两个哈希值的汉明距离
    """
    return (a ^ b).bit_count()


# ----------- 异步抓帧函数 -----------

async def grab_frame(url, at_time=1, timeout=15):
    """
    使用 ffmpeg 异步抓取视频 at_time 秒处的单帧图像
    返回图像字节数据，失败返回 None 和错误信息
    """
    cmd = ["ffmpeg", "-ss", str(at_time), "-i", url,
           "-frames:v", "1", "-f", "image2", "-vcodec", "mjpeg",
           "pipe:1", "-hide_banner", "-loglevel", "error"]
    try:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return None, "timeout"
        if stdout:
            return stdout, ""
        else:
            return None, stderr.decode('utf-8', errors='ignore') or "no_output"
    except FileNotFoundError:
        return None, "ffmpeg_not_installed"


# ----------- 读取缓存并解析 -----------

def load_cache_advanced():
    """
    读取并解析 total_cache.json 缓存文件，将16进制哈希转为整数
    返回两个字典 cache（整型哈希），raw_cache（原始json）
    """
    if not os.path.exists(TOTAL_CACHE_FILE):
        print(f"缓存文件不存在: {TOTAL_CACHE_FILE}")
        return {}, {}

    with open(TOTAL_CACHE_FILE, "r", encoding="utf-8") as f:
        raw_cache = json.load(f)

    cache = {}
    for url, date_dict in raw_cache.items():
        cache[url] = {}
        for date, timepoints in date_dict.items():
            cache[url][date] = {}
            for tp, entry in timepoints.items():
                try:
                    phash_int = int(entry["phash"], 16) if entry.get("phash") else None
                    ahash_int = int(entry["ahash"], 16) if entry.get("ahash") else None
                    dhash_int = int(entry["dhash"], 16) if entry.get("dhash") else None
                    cache[url][date][tp] = {
                        "phash": phash_int,
                        "ahash": ahash_int,
                        "dhash": dhash_int
                    }
                except Exception as e:
                    print(f"哈希转换错误 url={url} date={date} tp={tp} error={e}")
    return cache, raw_cache


# ----------- 计算哈希相似度 -----------

def similarity_hash(h1, h2):
    """
    计算两个哈希整数的相似度，范围0~1
    """
    if h1 is None or h2 is None:
        return 0.0
    dist = hamming(h1, h2)
    return 1.0 - dist / HASH_BITS


def average_similarity(hashes1, hashes2):
    """
    计算三种哈希的平均相似度，忽略缺失哈希
    """
    sims = []
    for key in ["phash", "ahash", "dhash"]:
        if hashes1.get(key) is not None and hashes2.get(key) is not None:
            sims.append(similarity_hash(hashes1[key], hashes2[key]))
    if not sims:
        return 0.0
    return sum(sims) / len(sims)


def compute_similarity_matrix(url, cache_for_url):
    """
    计算url对应所有日期+时间点之间哈希相似度矩阵
    只包含至少一个哈希非空的时间点
    返回相似度字典和时间点键列表
    """
    keys = []
    for date in sorted(cache_for_url.keys()):
        for tp in sorted(cache_for_url[date].keys()):
            h = cache_for_url[date][tp]
            if any(h.get(k) is not None for k in ["phash", "ahash", "dhash"]):
                keys.append((date, tp))

    sim_matrix = {}

    for i, (d1, t1) in enumerate(keys):
        h1 = cache_for_url[d1][t1]
        for j in range(i, len(keys)):
            d2, t2 = keys[j]
            h2 = cache_for_url[d2][t2]
            sim = average_similarity(h1, h2)
            sim_matrix[(d1, t1, d2, t2)] = sim
            sim_matrix[(d2, t2, d1, t1)] = sim

    return sim_matrix, keys


def analyze_similarity_time_series(sim_matrix, keys):
    """
    分析相邻时间点相似度，返回均值、标准差、最大、最小
    """
    sims = []
    for i in range(len(keys) - 1):
        k1 = keys[i]
        k2 = keys[i + 1]
        sim = sim_matrix.get((*k1, *k2), 0.0)
        sims.append(sim)

    if not sims:
        return 0.0, 0.0, 0.0, 0.0

    sims_arr = np.array(sims)
    return sims_arr.mean(), sims_arr.std(), sims_arr.max(), sims_arr.min()


def judge_fake_and_loop(sim_matrix, keys, threshold_fake=0.95, threshold_loop=0.98):
    """
    根据相似度矩阵判断假源和轮回：
    - 假源：超过半数时间点对相似度>=阈值
    - 轮回：相邻时间点平均相似度>=轮回阈值
    返回判断结果字典
    """
    high_sim_count = 0
    total_pairs = 0
    sim_values = []

    n = len(keys)
    for i in range(n):
        for j in range(i + 1, n):
            sim = sim_matrix.get((*keys[i], *keys[j]), 0.0)
            sim_values.append(sim)
            if sim >= threshold_fake:
                high_sim_count += 1
            total_pairs += 1

    max_sim = max(sim_values) if sim_values else 0.0

    avg_sim_adjacent, sim_std, _, _ = analyze_similarity_time_series(sim_matrix, keys)

    is_fake = high_sim_count > (total_pairs * 0.5)
    is_loop = avg_sim_adjacent >= threshold_loop

    return {
        "is_fake": is_fake,
        "is_loop": is_loop,
        "max_sim": max_sim,
        "avg_sim_adjacent": avg_sim_adjacent,
        "sim_std": sim_std
    }


# ----------- 异步处理单条url -----------

async def process_one(url, sem, cache, threshold=0.95, timeout=20):
    """
    异步处理单个url，判断假源和轮回，返回结果字典
    """
    async with sem:
        cache_for_url = cache.get(url)
        if not cache_for_url:
            # 无缓存，简单抓帧做判断
            img_bytes, err = await grab_frame(url, timeout=timeout)
            if not img_bytes:
                return {"url": url, "status": "error", "errors": [err], "is_fake": False, "is_loop": False,
                        "similarity": 0.0}
            try:
                phash_new = image_to_phash_bytes(img_bytes)
                ahash_new = image_to_ahash_bytes(img_bytes)
                dhash_new = image_to_dhash_bytes(img_bytes)
            except Exception as e:
                return {"url": url, "status": f"hash_error:{e}", "errors": [], "is_fake": False, "is_loop": False,
                        "similarity": 0.0}
            return {"url": url, "status": "ok", "errors": [], "is_fake": False, "is_loop": False,
                    "similarity": 1.0}

        # 计算缓存内相似度矩阵及判断
        sim_matrix, keys = compute_similarity_matrix(url, cache_for_url)
        judge_result = judge_fake_and_loop(sim_matrix, keys, threshold_fake=threshold, threshold_loop=0.98)

        # 实时抓帧和缓存哈希对比，找最大相似度
        img_bytes, err = await grab_frame(url, timeout=timeout)
        if not img_bytes:
            return {"url": url, "status": "error", "errors": [err], "is_fake": judge_result["is_fake"],
                    "is_loop": judge_result["is_loop"], "similarity": 0.0}
        try:
            phash_new = image_to_phash_bytes(img_bytes)
            ahash_new = image_to_ahash_bytes(img_bytes)
            dhash_new = image_to_dhash_bytes(img_bytes)
        except Exception as e:
            return {"url": url, "status": f"hash_error:{e}", "errors": [], "is_fake": judge_result["is_fake"],
                    "is_loop": judge_result["is_loop"], "similarity": 0.0}

        max_sim_real = 0.0
        for date in cache_for_url:
            for tp in cache_for_url[date]:
                cached_hashes = cache_for_url[date][tp]
                sim = average_similarity({"phash": phash_new, "ahash": ahash_new, "dhash": dhash_new}, cached_hashes)
                if sim > max_sim_real:
                    max_sim_real = sim

        return {
            "url": url,
            "status": "ok",
            "errors": [],
            "is_fake": judge_result["is_fake"],
            "is_loop": judge_result["is_loop"],
            "similarity": max_sim_real,
            "max_sim_cached": judge_result["max_sim"],
            "avg_sim_adjacent": judge_result["avg_sim_adjacent"],
            "sim_std_adjacent": judge_result["sim_std"]
        }


# ----------- 批量异步执行 -----------

async def run_all(urls, concurrency, cache, threshold=0.95, timeout=20):
    """
    并发异步执行所有url检测任务
    使用信号量控制最大并发量，显示进度
    返回所有结果列表
    """
    sem = Semaphore(concurrency)
    tasks = [process_one(url, sem, cache, threshold, timeout) for url in urls]
    results = []
    for fut in tqdm_asyncio.as_completed(tasks, desc="final-scan", total=len(tasks)):
        r = await fut
        results.append(r)
    return results


# ----------- 读取输入CSV -----------

def read_deep_input(path):
    """
    读取csv输入，返回url列表
    支持列名“地址”或“url”
    """
    urls = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            url = r.get("地址") or r.get("url")
            if url:
                urls.append(url)
    return urls


# ----------- 写入结果 -----------

def write_final(results, input_path, final_out, final_invalid_out):
    """
    根据检测结果，将有效（非假源）和无效（假源或错误）分开写入两个csv文件
    自动检测输入文件编码，保证输出兼容
    """
    final_map = {r["url"]: r for r in results}

    with open(input_path, "rb") as fb:
        raw = fb.read(20000)
        detected_enc = chardet.detect(raw)["encoding"] or "utf-8"

    with open(input_path, newline='', encoding=detected_enc, errors='ignore') as fin, \
         open(final_out, "w", newline='', encoding='utf-8') as fvalid, \
         open(final_invalid_out, "w", newline='', encoding='utf-8') as finvalid:

        reader = csv.DictReader(fin)

        # 基础字段
        working_fields = [
            "频道名","地址","来源","图标","检测时间","分组",
            "视频编码","分辨率","帧率","音频"
        ]
        # 有效输出字段（带检测结果）
        valid_fields = working_fields + ["相似度", "是否假源", "是否轮回", "轮回相似度", "轮回波动"]
        # 无效输出字段（带失败原因）
        invalid_fields = working_fields + ["未通过信息", "相似度", "是否假源", "是否轮回", "轮回相似度", "轮回波动"]

        w_valid = csv.DictWriter(fvalid, fieldnames=valid_fields)
        w_invalid = csv.DictWriter(finvalid, fieldnames=invalid_fields)

        w_valid.writeheader()
        w_invalid.writeheader()

        for row in reader:
            url = (row.get("地址") or row.get("url") or "").strip()
            if not url:
                continue
            r = final_map.get(url)

            # 没有检测结果，视为未检测，写入无效文件
            if not r:
                row_invalid = dict(row)
                row_invalid["未通过信息"] = "未检测"
                w_invalid.writerow(row_invalid)
                continue

            similarity = round(r.get("similarity", 0), 4)
            is_fake = r.get("is_fake", False)
            is_loop = r.get("is_loop", False)
            avg_sim_adjacent = round(r.get("avg_sim_adjacent", 0), 4)
            sim_std_adjacent = round(r.get("sim_std_adjacent", 0), 4)
            status = r.get("status", "")

            # 判定为假源或状态异常的写入无效文件
            if is_fake or status != "ok":
                row_invalid = dict(row)
                row_invalid["未通过信息"] = status if status != "ok" else "疑似假源"
                row_invalid["相似度"] = similarity
                row_invalid["是否假源"] = is_fake
                row_invalid["是否轮回"] = is_loop
                row_invalid["轮回相似度"] = avg_sim_adjacent
                row_invalid["轮回波动"] = sim_std_adjacent
                w_invalid.writerow(row_invalid)
            else:
                # 通过检测，写入有效文件
                row_valid = dict(row)
                row_valid["相似度"] = similarity
                row_valid["是否假源"] = is_fake
                row_valid["是否轮回"] = is_loop
                row_valid["轮回相似度"] = avg_sim_adjacent
                row_valid["轮回波动"] = sim_std_adjacent
                w_valid.writerow(row_valid)


# ----------- 主函数 -----------

def main():
    global CACHE_DIR, TOTAL_CACHE_FILE
    parser = argparse.ArgumentParser(description="IPTV 假源和轮回检测脚本")
    parser.add_argument("--input", required=True, help="输入CSV文件路径（带有地址字段）")
    parser.add_argument("--output", required=True, help="输出有效结果CSV路径")
    parser.add_argument("--invalid", required=True, help="输出无效结果CSV路径")
    parser.add_argument("--cache_dir", default=CACHE_DIR, help="缓存目录，默认 output/cache")
    parser.add_argument("--threshold", type=float, default=0.95, help="假源判定相似度阈值，默认0.95")
    parser.add_argument("--concurrency", type=int, default=6, help="并发数量，默认6")
    parser.add_argument("--timeout", type=int, default=20, help="抓帧超时时间，默认20秒")

    args = parser.parse_args()

    CACHE_DIR = args.cache_dir
    TOTAL_CACHE_FILE = os.path.join(CACHE_DIR, "total_cache.json")

    # 下面是示例调用流程（你可以根据实际需要调整）
    os.makedirs(CACHE_DIR, exist_ok=True)

    cache, raw_cache = load_cache_advanced()

    urls = read_deep_input(args.input)

    results = asyncio.run(run_all(urls, args.concurrency, cache, threshold=args.threshold, timeout=args.timeout))

    write_final(results, args.input, args.output, args.invalid)


if __name__ == "__main__":
    main()
