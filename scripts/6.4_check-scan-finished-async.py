#!/usr/bin/env python3
import os
import re
import asyncio
import aiohttp
import time

# ---------------- 环境变量读取 ----------------
TOKEN = os.getenv("GITHUB_TOKEN")      # GitHub 访问令牌
OWNER = os.getenv("REPO_OWNER")        # 仓库拥有者
REPO = os.getenv("REPO_NAME")          # 仓库名称

# 检查环境变量是否齐全，不齐全则退出
if not TOKEN or not OWNER or not REPO:
    print("❌ Missing environment variables.")
    print("TOKEN:", TOKEN)
    print("OWNER:", OWNER)
    print("REPO:", REPO)
    exit(1)

# ---------------- 常量定义 ----------------
WORKFLOW_DIR = ".github/workflows"    # workflow 文件夹路径

# 只匹配以 "Scan_" 开头的 workflow 文件，忽略大小写
PATTERN = re.compile(r"^Scan_", re.IGNORECASE)

CHECK_INTERVAL = 20        # 每轮检查间隔（秒）
MAX_ROUNDS = 240           # 最多检查次数（240 次 * 20 秒 = 80 分钟）

# ---------------- 异步函数：获取指定 workflow 的最新运行状态 ----------------
async def fetch_latest_run(session, workflow_file):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/workflows/{workflow_file}/runs?per_page=1"
    headers = {"Authorization": f"token {TOKEN}"}

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                # 请求失败，返回状态空
                return workflow_file, None, None

            data = await resp.json()
            runs = data.get("workflow_runs", [])
            if not runs:
                # 没有运行记录
                return workflow_file, None, None

            latest = runs[0]
            # 返回 workflow 文件名、状态、结论
            return workflow_file, latest["status"], latest["conclusion"]

    except Exception as e:
        # 网络或其他异常时也返回空状态
        return workflow_file, None, None

# ---------------- 异步函数：检查所有符合条件的 workflow 是否全部完成 ----------------
async def check_all_finished():
    """执行一次检查，返回 True 表示全部完成，否则 False"""

    # 筛选出 workflow 目录下，文件名以 "Scan_" 开头的文件列表
    workflows = [
        f for f in os.listdir(WORKFLOW_DIR)
        if PATTERN.search(f)
    ]

    total = len(workflows)
    if total == 0:
        print("❌ No chunk workflow files found.")
        return False

    # 建立 aiohttp 会话，异步批量获取每个 workflow 最新运行状态
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_latest_run(session, wf) for wf in workflows]
        results = await asyncio.gather(*tasks)

    all_done = True

    # 统计各状态数量
    count_completed = 0
    count_in_progress = 0
    count_queued = 0
    count_no_run = 0

    for workflow_file, status, conclusion in results:
        if status is None:
            print(f"⚠️ {workflow_file}: No runs found")
            count_no_run += 1
            all_done = False
        else:
            print(f"📌 {workflow_file}: status={status}, conclusion={conclusion}")
            if status == "completed":
                count_completed += 1
            elif status == "queued":
                count_queued += 1
                all_done = False
            else:
                # 其他状态，基本是 in_progress 或者其他
                count_in_progress += 1
                all_done = False

    print(f"ℹ️ Total workflows: {total}, Completed: {count_completed}, In Progress: {count_in_progress}, Queued: {count_queued}, No Runs: {count_no_run}")

    return all_done

# ---------------- 主异步函数：循环检测 workflow 状态，直到全部完成或超时 ----------------
async def main():
    print("🚀 Starting async chunk workflow monitor (auto-loop mode)...")
    start_time = time.time()

    for round_idx in range(1, MAX_ROUNDS + 1):
        print(f"\n🔎 Round {round_idx}/{MAX_ROUNDS} checking...")

        finished = await check_all_finished()

        if finished:
            print("\n🎉 All chunk workflows completed!")
            return

        print(f"⏳ Not done yet. Waiting {CHECK_INTERVAL} sec...\n")
        await asyncio.sleep(CHECK_INTERVAL)

    print("❌ Timeout: Some workflows did not finish in time.")
    exit(1)

# ---------------- 入口 ----------------
if __name__ == "__main__":
    asyncio.run(main())
