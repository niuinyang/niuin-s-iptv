#!/usr/bin/env python3
import asyncio
import csv
import json
import argparse
import os
import time
from PIL import Image
import imagehash
from asyncio.subprocess import create_subprocess_exec, PIPE
from io import BytesIO
from tqdm import tqdm

GRAB_TIMES = [2, 5, 20]  # 秒

def classify_error(err: str):
    """基于错误字符串简单分类"""
    if not err:
        return ""
    err_lower = err.lower()
    if "timeout" in err_lower:
        return "timeout"
    if any(x in err_lower for x in [
        "connection refused",
        "connection timed out",
        "connection reset",
        "connection failed",
        "network is unreachable"
    ]):
        return "network_error"
    if "ffmpeg error" in err_lower:
        return "ffmpeg_error"
    return "other_error"

async def fetch_frame(url, timestamp, timeout):
    """
    用 ffmpeg 抓取指定时间点的单帧图像，返回 PIL Image 或抛异常
    """
    cmd = [
        "ffmpeg",
        "-ss", str(timestamp),
        "-i", url,
        "-frames:v", "1",
        "-f", "image2pipe",
        "-vcodec", "png",
        "-"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=PIPE,
            stderr=PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            raise RuntimeError(f"Timeout after {timeout}s")

        if proc.returncode != 0 or not stdout:
            raise RuntimeError(
                stderr.decode("utf-8", errors="ignore").strip()
                or f"ffmpeg failed with code {proc.returncode}"
            )

        img = Image.open(BytesIO(stdout))
        return img

    except Exception as e:
        raise RuntimeError(f"ffmpeg error: {e}")

def calc_hashes(image):
    """
    计算 pHash / aHash / dHash / wHash
    返回 16 进制字符串
    """
    phash = str(imagehash.phash(image))
    ahash = str(imagehash.average_hash(image))
    dhash = str(imagehash.dhash(image))
    whash = str(imagehash.whash(image))
    return phash, ahash, dhash, whash

async def process_url(semaphore, url, timeout, retries):
    """抓取一个 url 的多个时间点帧并计算哈希"""
    async with semaphore:
        phashes, ahashes, dhashes, whashes = [], [], [], []

        fail_count = 0
        timeout_count = 0
        network_error_count = 0
        other_error_count = 0
        total_fetch_time = 0.0
        success_count = 0
        final_error = None

        for attempt in range(retries + 1):
            try:
                phashes.clear()
                ahashes.clear()
                dhashes.clear()
                whashes.clear()

                for t in GRAB_TIMES:
                    start = time.perf_counter()
                    img = await fetch_frame(url, t, timeout)
                    elapsed = time.perf_counter() - start
                    total_fetch_time += elapsed

                    p, a, d, w = calc_hashes(img)
                    phashes.append(p)
                    ahashes.append(a)
                    dhashes.append(d)
                    whashes.append(w)

                    success_count += 1

                final_error = None
                break

            except Exception as e:
                fail_count += 1
                err_str = str(e)
                final_error = err_str

                err_type = classify_error(err_str)
                if err_type == "timeout":
                    timeout_count += 1
                elif err_type == "network_error":
                    network_error_count += 1
                else:
                    other_error_count += 1

                if attempt < retries:
                    await asyncio.sleep(1)
                else:
                    phashes = [None] * len(GRAB_TIMES)
                    ahashes = [None] * len(GRAB_TIMES)
                    dhashes = [None] * len(GRAB_TIMES)
                    whashes = [None] * len(GRAB_TIMES)

        avg_fetch_time = (
            total_fetch_time / success_count
            if success_count > 0 else None
        )

        return url, {
            "phash": phashes,
            "ahash": ahashes,
            "dhash": dhashes,
            "whash": whashes,
            "error": {
                "fail_count": fail_count,
                "timeout_count": timeout_count,
                "network_error_count": network_error_count,
                "other_error_count": other_error_count,
                "final_error": final_error
            },
            "stats": {
                "total_fetch_time": total_fetch_time,
                "avg_fetch_time": avg_fetch_time,
                "success_count": success_count
            }
        }

async def main(args):
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    urls = []
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("地址", "").strip()
            if url.startswith("http"):
                urls.append(url)

    print(f"共读取 {len(urls)} 条有效地址")

    semaphore = asyncio.Semaphore(args.concurrency)
    tasks = [
        process_url(semaphore, url, args.timeout, args.retry)
        for url in urls
    ]

    results = {}
    for f in tqdm(
        asyncio.as_completed(tasks),
        total=len(tasks),
        desc="抓帧计算哈希"
    ):
        url, data = await f
        results[url] = data

    with open(args.output, "w", encoding="utf-8") as f_out:
        json.dump(results, f_out, indent=2, ensure_ascii=False)

    print(f"完成，结果保存到 {args.output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="IPTV 多时间点抓帧计算哈希（p/a/d/w）"
    )
    parser.add_argument("--input", required=True, help="输入 deep scan CSV 文件路径")
    parser.add_argument("--output", required=True, help="输出 JSON 文件路径")
    parser.add_argument("--concurrency", type=int, default=15)
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--retry", type=int, default=2)
    args = parser.parse_args()

    asyncio.run(main(args))
