import aiohttp          # 异步 HTTP 客户端，用于并发访问 IPTV 地址
import asyncio          # Python 异步 IO 框架，管理协程和事件循环
import csv              # 读写 CSV 文件
import time             # 用于计算 HTTP 请求耗时（RTT）
import argparse         # 解析命令行参数
import os               # 操作文件系统（创建目录等）
from tqdm import tqdm   # 控制台进度条显示


# ==============================
# 配置区
# ==============================

RETRY_LIMIT = 2
# 单个地址最大重试次数（失败后重新请求的次数）

SUCCESS_STATUS = [200, 206, 301, 302, 403, 429]
# 认为“可用”的 HTTP 状态码：
# 200/206：正常返回
# 301/302：重定向（常见于 IPTV）
# 403：防盗链但源存在
# 429：限速但源通常有效

DEFAULT_CONCURRENCY = 100
# 默认并发检测数量（同时请求的 URL 数）

DEFAULT_TIMEOUT = 8
# 单个 HTTP 请求最大超时时间（秒）

MIN_CONCURRENCY = 20
# 并发下限（预留参数，目前未动态使用）

MAX_CONCURRENCY = 150
# 并发上限（预留参数，目前未动态使用）


# ==============================
# HTTP 请求函数
# ==============================

async def fetch_url(session, url, timeout):
    """
    对单个 URL 发起一次 HTTP GET 请求
    返回：是否成功、响应时间(ms)、HTTP 状态码
    """
    start = time.time()  # 记录请求开始时间

    try:
        async with session.get(url, timeout=timeout) as resp:

            # 如果状态码在可接受范围内，或为 5xx（服务端错误但源存在）
            if resp.status in SUCCESS_STATUS or (500 <= resp.status <= 599):

                # 读取少量数据，确保连接真实建立（避免假 200）
                await resp.content.read(10)

                # 计算 RTT（毫秒）
                rtt = int((time.time() - start) * 1000)

                return True, rtt, resp.status

            return False, None, resp.status

    except Exception:
        # 捕获连接失败、DNS 错误、超时等异常
        return False, None, None


# ==============================
# 单条源检测
# ==============================

async def check_source(semaphore, session, row, timeout):
    """
    对 CSV 中的一行进行检测
    保留原始字段，并追加检测结果
    """
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

        # 全部失败
        result = row.copy()
        result["检测时间"] = ""
        result["状态码"] = status
        return result


# ==============================
# 并发调度与结果写出
# ==============================

async def run_all(rows, fieldnames, output_file, concurrency, timeout):
    """
    创建所有检测任务，控制并发并写出最终结果
    """
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

    # 输出字段 = 输入字段 + 新增字段
    output_fields = fieldnames + ["检测时间", "状态码"]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # 写入 CSV 输出文件（逗号分隔）
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=output_fields,
            extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ 检测完成: 共 {len(results)} 条", flush=True)


# ==============================
# 读取输入文件
# ==============================

def read_csv(input_file):
    """
    读取 CSV 输入文件
    保留所有原始列
    """
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
    parser.add_argument("--output", required=True)
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
            args.concurrency,
            args.timeout
        )
    )


if __name__ == "__main__":
    main()
