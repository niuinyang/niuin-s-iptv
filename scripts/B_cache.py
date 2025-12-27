#!/usr/bin/env python3
import argparse
import os
import csv
import asyncio
from asyncio.subprocess import create_subprocess_exec, PIPE
from PIL import Image
import io
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore
import json
from datetime import datetime
import imagehash

CACHE_DIR = "output/cache/chunk"

# 固定时间点顺序（用于写入时排序）
TIME_KEYS = ["0811", "1612", "2113"]

# 固定字段顺序
HASH_KEYS = ["phash", "ahash", "dhash", "error"]


async def grab_frame(url, at_time=1, timeout=15):
    cmd = [
        "ffmpeg", "-ss", str(at_time), "-i", url,
        "-frames:v", "1", "-f", "image2", "-vcodec", "mjpeg",
        "pipe:1", "-hide_banner", "-loglevel", "error"
    ]
    try:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        if stdout:
            return stdout, ""
        else:
            return None, stderr.decode(errors="ignore") or "no_output"
    except Exception as e:
        return None, str(e)


def phash_bytes(img_bytes):
    im = Image.open(io.BytesIO(img_bytes))
    return str(imagehash.phash(im))


def ahash_bytes(img_bytes):
    im = Image.open(io.BytesIO(img_bytes))
    return str(imagehash.average_hash(im))


def dhash_bytes(img_bytes):
    im = Image.open(io.BytesIO(img_bytes))
    return str(imagehash.dhash(im))


async def process_one(url, sem, timeout=20):
    async with sem:
        img_bytes, err = await grab_frame(url, timeout=timeout)
        if not img_bytes:
            return {
                "url": url,
                "phash": None, "ahash": None, "dhash": None,
                "error": err
            }
        try:
            return {
                "url": url,
                "phash": phash_bytes(img_bytes),
                "ahash": ahash_bytes(img_bytes),
                "dhash": dhash_bytes(img_bytes),
                "error": None
            }
        except Exception as e:
            return {
                "url": url,
                "phash": None, "ahash": None, "dhash": None,
                "error": str(e)
            }


async def main_async(input_file, timepoint, chunk_id, concurrency=6):
    sem = Semaphore(concurrency)
    urls = []

    with open(input_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("地址") or row.get("url")
            if url:
                urls.append(url)

    results = []
    tasks = [process_one(url, sem) for url in urls]

    for fut in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc=f"Cache {timepoint}"):
        results.append(await fut)

    # 缓存文件路径
    date_str = datetime.now().strftime("%Y%m%d")
    chunk_cache_dir = os.path.join(CACHE_DIR, date_str)
    os.makedirs(chunk_cache_dir, exist_ok=True)
    cache_file = os.path.join(chunk_cache_dir, f"{chunk_id}_cache.json")

    # 读取旧缓存
    old_cache = {}
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                old_cache = json.load(f)
        except:
            old_cache = {}

    # 写入新的哈希数据
    for r in results:
        url = r["url"]
        if url not in old_cache:
            old_cache[url] = {}

        old_cache[url][timepoint] = {
            "phash": r["phash"],
            "ahash": r["ahash"],
            "dhash": r["dhash"],
            "error": r["error"]
        }

    # ============ 排序输出核心 =============
    final_output = {}

    # 1. URL 排序
    for url in sorted(old_cache.keys()):
        time_dict = old_cache[url]
        ordered_time_dict = {}

        # 2. 时间点排序 0811 → 1612 → 2113
        for tk in TIME_KEYS:
            if tk in time_dict:
                old = time_dict[tk]

                # 3. 字段排序 phash → ahash → dhash → error
                ordered_time_dict[tk] = {
                    key: old.get(key) for key in HASH_KEYS
                }

        final_output[url] = ordered_time_dict

    # 写入文件
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--timepoint", required=True, choices=["0811", "1612", "2113"])
    parser.add_argument("--chunk_id", required=True)
    args = parser.parse_args()

    asyncio.run(main_async(args.input, args.timepoint, args.chunk_id))


if __name__ == "__main__":
    main()
