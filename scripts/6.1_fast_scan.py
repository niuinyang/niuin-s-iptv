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

RETRY_LIMIT = 2

SUCCESS_STATUS = [200, 206, 301, 302, 403, 429]

DEFAULT_CONCURRENCY = 100
DEFAULT_TIMEOUT = 8


# ==============================
# HTTP 请求函数
# ==============================

async def fetch_url(session, url, timeout):
    start = time.time()

    try:
        async with session.get(url, timeout=timeout) as resp:
            if resp.status in SUCCESS_STATUS or (500 <= resp.status <= 599):
                await resp.content.read(10)
                rtt = int((time.time() - start) * 1000)
                return True, rtt, resp.status

            return False, None, resp.status

    except Exception:
        return False, None, None


# ==============================
# 单条源检测
# ==============================

async def check_source(semaphore, session, row, timeout):
    url = row.get("地址", "")

    async with semaphore:
        status = None

        for attempt in range(RETRY_LIMIT):
            ok, rtt, status = await fetch_url(session, url, timeout)
            if ok:
                result = row.copy()
                result["检测时间"] = rtt
                result["状态码"] = status
                return result

            await asyncio.sleep(0.2 * (attempt + 1))

        result = row.copy()
        result["检测时间"] = ""
        result["状态码"] = status
        return result


# ==============================
# 并发调度与结果拆分写出
# ==============================

async def run_all(
    rows,
    fieldnames,
    output_ok,
    output_invalid,
    concurrency,
    timeout
):
    semaphore = asyncio.Semaphore(concurrency)

    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)

    results = []

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout_cfg
    ) as session:

        tasks = [
            check_source(semaphore, session, row, timeout)
            for row in rows
        ]

        for f in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            ncols=80,
            desc="fast-scan"
        ):
            res = await f
            results.append(res)

    # ==============================
    # 按检测结果拆分
    # ==============================

    ok_rows = []
    not_rows = []

    for r in results:
        if r.get("检测时间") != "":
            ok_rows.append(r)
        else:
            not_rows.append(r)

    output_fields = fieldnames + ["检测时间", "状态码"]

    # 确保目录存在
    os.makedirs(os.path.dirname(output_ok), exist_ok=True)
    os.makedirs(os.path.dirname(output_invalid), exist_ok=True)

    # 写 OK 文件
    with open(output_ok, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=output_fields,
            extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(ok_rows)

    # 写 NOT 文件
    with open(output_invalid, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=output_fields,
            extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(not_rows)

    print(
        f"✅ fast-scan 完成 | OK: {len(ok_rows)} 条 | NOT: {len(not_rows)} 条",
        flush=True
    )


# ==============================
# 读取输入 CSV
# ==============================

def read_csv(input_file):
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = []

        for row in reader:
            url = row.get("地址", "")
            if url.startswith("http"):
                rows.append(row)

    return rows, fieldnames


# ==============================
# 主入口
# ==============================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, help="fast OK 输出 CSV")
    parser.add_argument("--invalid", required=True, help="fast NOT 输出 CSV")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args()

    rows, fieldnames = read_csv(args.input)
    print(f"📺 待检测源数量: {len(rows)}", flush=True)

    asyncio.run(
        run_all(
            rows,
            fieldnames,
            args.output,
            args.invalid,
            args.concurrency,
            args.timeout
        )
    )


if __name__ == "__main__":
    main()
