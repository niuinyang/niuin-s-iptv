#!/usr/bin/env python3
# scripts/6.3final_scan_advanced.py

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

# ================= 新增配置 =================
GRAB_TIMEPOINTS = [1, 5, 10]   # 多时间点抓帧
GRAB_RETRY = 2                # 每个时间点重试次数
# ==========================================

HASH_SIZE = 8
HASH_BITS = HASH_SIZE * HASH_SIZE


# ----------- 图像哈希计算函数 -----------

def image_to_ahash_bytes(img_bytes, hash_size=HASH_SIZE):
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize(
        (hash_size, hash_size), Image.Resampling.LANCZOS)
    pixels = list(im.getdata())
    avg = sum(pixels) / len(pixels)
    bits = 0
    for p in pixels:
        bits = (bits << 1) | (1 if p > avg else 0)
    return bits


def image_to_phash_bytes(img_bytes, hash_size=HASH_SIZE):
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize(
        (32, 32), Image.Resampling.LANCZOS)
    pixels = np.array(im, dtype=np.float32)
    dct = np.round(np.real(np.fft.fft2(pixels)))
    dct_low = dct[:hash_size, :hash_size]
    avg = dct_low[1:, 1:].mean()
    bits = 0
    for v in dct_low.flatten():
        bits = (bits << 1) | (1 if v > avg else 0)
    return bits


def image_to_dhash_bytes(img_bytes, hash_size=HASH_SIZE):
    im = Image.open(io.BytesIO(img_bytes)).convert('L').resize(
        (hash_size + 1, hash_size), Image.Resampling.LANCZOS)
    pixels = list(im.getdata())
    bits = 0
    for row in range(hash_size):
        for col in range(hash_size):
            l = pixels[row * (hash_size + 1) + col]
            r = pixels[row * (hash_size + 1) + col + 1]
            bits = (bits << 1) | (1 if l > r else 0)
    return bits


def hamming(a, b):
    return (a ^ b).bit_count()


# ----------- 抓帧 -----------

async def grab_frame(url, at_time=1, timeout=15):
    cmd = [
        "ffmpeg", "-ss", str(at_time), "-i", url,
        "-frames:v", "1", "-f", "image2", "-vcodec", "mjpeg",
        "pipe:1", "-hide_banner", "-loglevel", "error"
    ]
    try:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return None, "timeout"
        return (out, "") if out else (None, err.decode(errors="ignore"))
    except FileNotFoundError:
        return None, "ffmpeg_not_found"


async def grab_frame_multi(url, timepoints, retry=2, timeout=15):
    results = []
    for tp in timepoints:
        for _ in range(retry):
            img, err = await grab_frame(url, tp, timeout)
            if img:
                results.append((tp, img))
                break
    return results


# ----------- 缓存读取 -----------

def load_cache_advanced(total_cache_file):
    if not os.path.exists(total_cache_file):
        return {}, {}
    with open(total_cache_file, "r", encoding="utf-8") as f:
        raw = json.load(f)

    cache = {}
    for url, dates in raw.items():
        cache[url] = {}
        for d, tps in dates.items():
            cache[url][d] = {}
            for tp, v in tps.items():
                cache[url][d][tp] = {
                    "phash": int(v["phash"], 16) if v.get("phash") else None,
                    "ahash": int(v["ahash"], 16) if v.get("ahash") else None,
                    "dhash": int(v["dhash"], 16) if v.get("dhash") else None
                }
    return cache, raw


# ----------- 相似度 -----------

def similarity_hash(h1, h2):
    if h1 is None or h2 is None:
        return 0.0
    return 1.0 - hamming(h1, h2) / HASH_BITS


def average_similarity(h1, h2):
    sims = []
    for k in ["phash", "ahash", "dhash"]:
        if h1.get(k) is not None and h2.get(k) is not None:
            sims.append(similarity_hash(h1[k], h2[k]))
    return sum(sims) / len(sims) if sims else 0.0


# ----------- 核心处理 -----------

async def process_one(url, sem, cache, threshold=0.95, timeout=20):
    async with sem:

        cache_for_url = cache.get(url)

        frames = await grab_frame_multi(
            url, GRAB_TIMEPOINTS, GRAB_RETRY, timeout
        )
        if not frames:
            return {
                "url": url,
                "status": "error",
                "errors": ["all_grab_failed"],
                "is_fake": False,
                "is_loop": False,
                "similarity": 0.0
            }

        real_hashes = []
        for _, img in frames:
            real_hashes.append({
                "phash": image_to_phash_bytes(img),
                "ahash": image_to_ahash_bytes(img),
                "dhash": image_to_dhash_bytes(img)
            })

        if not cache_for_url:
            return {
                "url": url,
                "status": "ok",
                "errors": [],
                "is_fake": False,
                "is_loop": False,
                "similarity": 1.0
            }

        max_sim = 0.0
        for d in cache_for_url:
            for tp in cache_for_url[d]:
                cached = cache_for_url[d][tp]
                for rh in real_hashes:
                    max_sim = max(max_sim, average_similarity(rh, cached))

        return {
            "url": url,
            "status": "ok",
            "errors": [],
            "is_fake": max_sim >= threshold,
            "is_loop": False,
            "similarity": max_sim
        }


# ----------- 并发执行 -----------

async def run_all(urls, concurrency, cache, threshold=0.95, timeout=20):
    sem = Semaphore(concurrency)
    tasks = [process_one(u, sem, cache, threshold, timeout) for u in urls]
    results = []
    for fut in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="final-scan"):
        results.append(await fut)
    return results


# ----------- 入口 -----------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--invalid", required=True)
    parser.add_argument("--cache_dir", default="output/cache",
                        help="缓存目录，内含 total_cache.json")
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    total_cache_file = os.path.join(args.cache_dir, "total_cache.json")
    os.makedirs(args.cache_dir, exist_ok=True)

    cache, _ = load_cache_advanced(total_cache_file)

    with open(args.input, newline='', encoding="utf-8") as f:
        urls = [r["地址"] for r in csv.DictReader(f) if r.get("地址")]

    results = asyncio.run(
        run_all(urls, args.concurrency, cache, timeout=args.timeout)
    )

    # 输出逻辑保持不变（略）


if __name__ == "__main__":
    main()
