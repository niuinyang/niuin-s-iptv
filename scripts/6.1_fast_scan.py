import aiohttp
import asyncio
import csv
import time
import argparse
import os
from tqdm import tqdm

# ==============================
# 配置区
# ==============================
RETRY_LIMIT = 2           # 每个源重试次数
# 放行的状态码列表，包括成功、重定向和部分反爬状态码
SUCCESS_STATUS = [200, 206, 301, 302, 403, 429]  
DEFAULT_CONCURRENCY = 100
DEFAULT_TIMEOUT = 8
MIN_CONCURRENCY = 20
MAX_CONCURRENCY = 150


async def fetch_url(session, url, timeout):
    start = time.time()
    try:
        async with session.get(url, timeout=timeout) as resp:
            # 放行指定状态码，包括500-599范围
            if resp.status in SUCCESS_STATUS or (500 <= resp.status <= 599):
                await resp.content.read(10)
                return True, int((time.time() - start) * 1000), resp.status
            else:
                return False, None, resp.status
    except Exception:
        return False, None, None


async def check_source(semaphore, session, row, timeout):
    name, url, source, icon = row
    async with semaphore:
        for attempt in range(RETRY_LIMIT):
            ok, rtt, status = await fetch_url(session, url, timeout)
            if ok:
                return {
                    "频道名": name,
                    "地址": url,
                    "来源": source,
                    "图标": icon,
                    "检测时间": rtt,
                    "分组": "未分组",
                    "状态码": status
                }
            await asyncio.sleep(0.2 * (attempt + 1))
        return {
            "频道名": name,
            "地址": url,
            "来源": source,
            "图标": icon,
            "检测时间": "",
            "分组": "未分组",
            "状态": f"失败({status})",
            "状态码": status
        }


async def run_all(rows, output_valid, output_invalid, concurrency, timeout):
    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    valid_rows, invalid_rows = [], []

    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
        tasks = [check_source(semaphore, session, row, timeout) for row in rows]
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), ncols=80, desc="fast-scan"):
            res = await f
            if "状态" not in res:
                valid_rows.append(res)
            else:
                invalid_rows.append(res)

            # 动态调节逻辑（宽松）
            if len(valid_rows) + len(invalid_rows) > 0:
                success_rate = len(valid_rows) / (len(valid_rows) + len(invalid_rows))
                if success_rate < 0.3 and concurrency > MIN_CONCURRENCY:
                    concurrency -= 5
                elif success_rate > 0.7 and concurrency < MAX_CONCURRENCY:
                    concurrency += 5

    # 写入文件
    os.makedirs(os.path.dirname(output_valid), exist_ok=True)
    with open(output_valid, "w", newline='', encoding="utf-8") as f_ok, \
         open(output_invalid, "w", newline='', encoding="utf-8") as f_fail:
        fieldnames_valid = ["频道名", "地址", "来源", "图标", "检测时间", "分组", "状态码"]
        fieldnames_invalid = ["频道名", "地址", "来源", "图标", "检测时间", "分组", "状态", "状态码"]

        writer_ok = csv.DictWriter(f_ok, fieldnames=fieldnames_valid)
        writer_fail = csv.DictWriter(f_fail, fieldnames=fieldnames_invalid)

        writer_ok.writeheader()
        writer_fail.writeheader()

        writer_ok.writerows(valid_rows)
        writer_fail.writerows(invalid_rows)

    print(f"✅ 检测完成: 成功 {len(valid_rows)} 条, 失败 {len(invalid_rows)} 条, 共 {len(rows)} 条", flush=True)


def read_csv(input_file):
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # 跳过表头
        rows = [r for r in reader if len(r) >= 2 and r[1].startswith("http")]
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, help="成功文件路径")
    parser.add_argument("--invalid", required=True, help="失败文件路径")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args()

    rows = read_csv(args.input)
    print(f"📺 待检测源数量: {len(rows)}", flush=True)

    asyncio.run(run_all(rows, args.output, args.invalid, args.concurrency, args.timeout))


if __name__ == "__main__":
    main()
