#!/usr/bin/env python3
import asyncio
import csv
import json
import argparse
import os
from PIL import Image
import imagehash
from asyncio.subprocess import create_subprocess_exec, PIPE
from io import BytesIO
from tqdm import tqdm

GRAB_TIMES = [2, 5, 20]  # 秒

async def fetch_frame(url, timestamp, timeout):
    """
    用 ffmpeg 抓取指定时间点的单帧图像，返回 PIL Image 或抛异常
    """
    # ffmpeg 参数解释：
    # -ss <timestamp> ：指定时间点
    # -i <url> ：输入流
    # -frames:v 1 ：只抓1帧
    # -f image2pipe -vcodec png - ：输出png格式图片到管道
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
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            raise RuntimeError(f"Timeout after {timeout}s")
        if proc.returncode != 0 or not stdout:
            raise RuntimeError(stderr.decode('utf-8', errors='ignore').strip() or f"ffmpeg failed with code {proc.returncode}")
        img = Image.open(BytesIO(stdout))
        return img
    except Exception as e:
        raise RuntimeError(f"ffmpeg error: {e}")

def calc_hashes(image):
    """计算pHash, aHash, dHash，返回16进制字符串"""
    phash = str(imagehash.phash(image))
    ahash = str(imagehash.average_hash(image))
    dhash = str(imagehash.dhash(image))
    return phash, ahash, dhash

async def process_url(semaphore, url, timeout, retries):
    """抓取一个url的3个时间点帧，计算哈希，失败重试"""
    async with semaphore:
        phashes, ahashes, dhashes = [], [], []
        error = None
        for attempt in range(retries+1):
            try:
                phashes.clear()
                ahashes.clear()
                dhashes.clear()
                for t in GRAB_TIMES:
                    img = await fetch_frame(url, t, timeout)
                    p, a, d = calc_hashes(img)
                    phashes.append(p)
                    ahashes.append(a)
                    dhashes.append(d)
                error = None
                break  # 成功就跳出重试
            except Exception as e:
                error = str(e)
                if attempt < retries:
                    await asyncio.sleep(1)  # 等待重试
                else:
                    phashes = [None]*len(GRAB_TIMES)
                    ahashes = [None]*len(GRAB_TIMES)
                    dhashes = [None]*len(GRAB_TIMES)
        return url, {
            "phash": phashes,
            "ahash": ahashes,
            "dhash": dhashes,
            "error": error
        }

async def main(args):
    if not os.path.exists(os.path.dirname(args.output)):
        os.makedirs(os.path.dirname(args.output))

    # 读取输入CSV地址
    urls = []
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("地址", "").strip()
            if url.startswith("http"):
                urls.append(url)
    print(f"共读取 {len(urls)} 条有效地址")

    semaphore = asyncio.Semaphore(args.concurrency)
    tasks = [process_url(semaphore, url, args.timeout, args.retry) for url in urls]

    results = {}
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="抓帧计算哈希"):
        url, data = await f
        results[url] = data

    # 写出JSON
    with open(args.output, "w", encoding="utf-8") as f_out:
        json.dump(results, f_out, indent=2, ensure_ascii=False)

    print(f"完成，结果保存到 {args.output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IPTV多时间点抓帧计算哈希")
    parser.add_argument("--input", required=True, help="输入 deep scan CSV 文件路径")
    parser.add_argument("--output", required=True, help="输出 JSON 文件路径")
    parser.add_argument("--concurrency", type=int, default=15, help="并发数")
    parser.add_argument("--timeout", type=int, default=15, help="单次抓帧超时时间（秒）")
    parser.add_argument("--retry", type=int, default=2, help="失败重试次数")
    args = parser.parse_args()
    asyncio.run(main(args))
