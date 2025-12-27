import aiohttp          # 异步 HTTP 客户端，用于并发访问 IPTV 地址
import asyncio          # Python 异步 IO 框架，管理协程和事件循环
import csv              # 读写 CSV / TSV 文件
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
        # 使用 aiohttp 发起异步 GET 请求
        async with session.get(url, timeout=timeout) as resp:

            # 如果状态码在可接受范围内，或为 5xx（服务端错误但源存在）
            if resp.status in SUCCESS_STATUS or (500 <= resp.status <= 599):

                # 读取少量数据，确保连接真实建立（避免假 200）
                await resp.content.read(10)

                # 计算 RTT（毫秒）
                rtt = int((time.time() - start) * 1000)

                return True, rtt, resp.status

            # 状态码不在允许范围内，视为失败
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
    url = row.get("地址", "")  # 从输入行中获取播放地址

    async with semaphore:  # 使用信号量限制并发
        status = None      # 记录最后一次请求的状态码

        # 按配置的重试次数进行检测
        for attempt in range(RETRY_LIMIT):

            ok, rtt, status = await fetch_url(session, url, timeout)

            if ok:
                # 成功：复制原始行，追加检测字段
                result = row.copy()
                result["检测时间"] = rtt
                result["状态码"] = status
                return result

            # 失败后稍作等待（递增退避）
            await asyncio.sleep(0.2 * (attempt + 1))

        # 全部重试失败：仍然返回原始数据
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
    semaphore = asyncio.Semaphore(concurrency)  # 控制并发数量

    # 创建 TCP 连接池，限制最大连接数，关闭 SSL 校验
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)

    # aiohttp 客户端超时配置
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)

    results = []  # 保存所有检测结果

    # 创建 HTTP 会话，所有请求共享
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout_cfg
    ) as session:

        # 为每一行创建一个异步检测任务
        tasks = [
            check_source(semaphore, session, row, timeout)
            for row in rows
        ]

        # 使用 as_completed 按完成顺序处理任务，并显示进度条
        for f in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            ncols=80,
            desc="fast-scan"
        ):
            res = await f
            results.append(res)

    # 输出字段 = 输入字段 + 新增检测字段
    output_fields = fieldnames + ["检测时间", "状态码"]

    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # 写入 TSV 输出文件
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=output_fields,
            delimiter="\t",
            extrasaction="ignore"  # 忽略多余字段
        )
        writer.writeheader()      # 写表头
        writer.writerows(results) # 写所有结果

    print(f"✅ 检测完成: 共 {len(results)} 条", flush=True)


# ==============================
# 读取输入文件
# ==============================

def read_csv(input_file):
    """
    读取 Tab 分隔的输入文件
    保留所有原始列
    """
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")  # 按列名读取
        fieldnames = reader.fieldnames               # 保存字段顺序
        rows = []

        for row in reader:
            url = row.get("地址", "")
            # 只处理包含 http 地址的行
            if url.startswith("http"):
                rows.append(row)

    return rows, fieldnames


# ==============================
# 主入口
# ==============================

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)   # 输入文件路径
    parser.add_argument("--output", required=True)  # 输出文件路径
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args()

    # 读取输入文件
    rows, fieldnames = read_csv(args.input)
    print(f"📺 待检测源数量: {len(rows)}", flush=True)

    # 运行异步检测
    asyncio.run(
        run_all(
            rows,
            fieldnames,
            args.output,
            args.concurrency,
            args.timeout
        )
    )


# 程序入口
if __name__ == "__main__":
    main()
