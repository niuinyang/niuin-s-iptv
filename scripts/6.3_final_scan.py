#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import os
from io import BytesIO
from PIL import Image
import imagehash
from asyncio.subprocess import create_subprocess_exec, PIPE
from tqdm.asyncio import tqdm_asyncio

# 多时间点截帧 (秒)
GRAB_TIMES = [1, 5, 10]

# 汉明距离阈值
HAMMING_THRESHOLD = 5

def load_fake_hashes(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("typical_fake_hashes", [])

def save_fake_hashes(path, fake_hashes):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"typical_fake_hashes": fake_hashes}, f, indent=2, ensure_ascii=False)

def hamming_dist(s1, s2):
    """计算两个16进制字符串的汉明距离"""
    b1 = int(s1, 16)
    b2 = int(s2, 16)
    x = b1 ^ b2
    return bin(x).count("1")

async def grab_frame(url, timepoint):
    # 用ffmpeg抓帧命令，输出png到stdout
    cmd = [
        "ffmpeg", "-ss", str(timepoint), "-i", url,
        "-frames:v", "1", "-f", "image2pipe", "-vcodec", "png", "-"
    ]
    proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0 or not stdout:
        return None
    return stdout

def calc_hashes(image_bytes):
    image = Image.open(BytesIO(image_bytes))
    phash = str(imagehash.phash(image))
    ahash = str(imagehash.average_hash(image))
    dhash = str(imagehash.dhash(image))
    return {"phash": phash, "ahash": ahash, "dhash": dhash}

def is_fake_hash(hashes, fake_hashes):
    for fh in fake_hashes:
        for key in ["phash", "ahash", "dhash"]:
            dist = hamming_dist(hashes[key], fh[key])
            if dist <= HAMMING_THRESHOLD:
                return True, fh
    return False, None

async def process_url(row, fake_hashes, semaphore):
    url = row["地址"]
    async with semaphore:
        hashes_list = []
        for t in GRAB_TIMES:
            frame = await grab_frame(url, t)
            if not frame:
                continue
            hashes = calc_hashes(frame)
            hashes_list.append(hashes)
        if not hashes_list:
            row["假源检测"] = "抓帧失败"
            row["假源命中"] = ""
            row["是否假源"] = False
            return row

        # 多帧哈希合并策略：简单取第一个帧哈希（也可改进）
        combined = hashes_list[0]

        # 假源库匹配
        fake, matched_hash = is_fake_hash(combined, fake_hashes)
        row["假源检测"] = "假源" if fake else "正常"
        row["假源命中"] = json.dumps(matched_hash, ensure_ascii=False) if fake else ""
        row["是否假源"] = fake

        return row, combined if fake else None

async def main(args):
    fake_hashes = load_fake_hashes(os.path.join(args.cache_dir, "typical_fake_hashes.json"))

    semaphore = asyncio.Semaphore(args.concurrency)
    rows = []
    with open(args.input, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    results = []
    new_fake_hashes = []
    hash_count = {}

    async def handle_row(row):
        result = await process_url(row, fake_hashes, semaphore)
        if isinstance(result, tuple):
            row_res, fake_hash = result
            if fake_hash:
                hkey = fake_hash["phash"]
                hash_count[hkey] = hash_count.get(hkey, 0) + 1
                if hash_count[hkey] > 1 and fake_hash not in new_fake_hashes:
                    new_fake_hashes.append(fake_hash)
            results.append(row_res)
        else:
            results.append(result)

    await asyncio.gather(*(handle_row(r) for r in rows))

    # 更新假源库
    if new_fake_hashes:
        print(f"新增 {len(new_fake_hashes)} 条假源哈希，写入假源库")
        fake_hashes.extend(new_fake_hashes)
        save_fake_hashes(os.path.join(args.cache_dir, "typical_fake_hashes.json"), fake_hashes)

    # 写结果CSV
    fieldnames = list(results[0].keys())
    with open(args.output, "w", encoding="utf-8", newline="") as f_ok, \
         open(args.invalid, "w", encoding="utf-8", newline="") as f_not:
        writer_ok = csv.DictWriter(f_ok, fieldnames=fieldnames)
        writer_not = csv.DictWriter(f_not, fieldnames=fieldnames)

        writer_ok.writeheader()
        writer_not.writeheader()

        for r in results:
            if r.get("是否假源"):
                writer_not.writerow(r)
            else:
                writer_ok.writerow(r)

    print(f"完成假源检测，输出正常: {len([r for r in results if not r.get('是否假源')])}，假源: {len([r for r in results if r.get('是否假源')])}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--invalid", required=True)
    parser.add_argument("--cache_dir", required=True)
    parser.add_argument("--concurrency", type=int, default=10)
    args = parser.parse_args()

    asyncio.run(main(args))
