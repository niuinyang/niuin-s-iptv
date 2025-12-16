#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import aiohttp
from urllib.parse import urlparse
from tqdm.asyncio import tqdm_asyncio

MY_URLS_FILE = "input/source/my.txt"
NET_URLS_FILE = "input/source/net.txt"

MY_SAVE_DIR = "input/download/my"
NET_SAVE_DIR = "input/download/net/原始"

CONCURRENCY = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/"  # 伪造Referer，部分网站需要
}

def parse_url_line(line):
    line = line.strip()
    if not line or line.startswith("#"):
        return None, None
    parts = line.split("#", 1)
    url = parts[0].strip()
    filename_base = parts[1].strip() if len(parts) > 1 else None

    parsed = urlparse(url)
    path = parsed.path
    ext = os.path.splitext(path)[1]
    if not ext:
        ext = ".m3u"

    if filename_base:
        filename = filename_base + ext
    else:
        filename = os.path.basename(path) or ("downloaded_file" + ext)

    return url, filename

async def download_file(session, url, save_path):
    try:
        async with session.get(url, headers=HEADERS, timeout=30) as resp:
            if resp.status == 200:
                data = await resp.read()
                with open(save_path, "wb") as f:
                    f.write(data)
                return True, None
            else:
                return False, f"HTTP status {resp.status}"
    except Exception as e:
        return False, str(e)

async def download_list(url_file, save_dir):
    if not os.path.exists(url_file):
        print(f"⚠️ URL文件不存在：{url_file}")
        return
    os.makedirs(save_dir, exist_ok=True)

    url_list = []
    with open(url_file, "r", encoding="utf-8") as f:
        for line in f:
            url, filename = parse_url_line(line)
            if url:
                url_list.append((url, filename))

    print(f"准备下载 {len(url_list)} 个文件到 {save_dir}")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []

        for url, filename in url_list:
            save_path = os.path.join(save_dir, filename)

            async def sem_task(url=url, save_path=save_path):
                async with semaphore:
                    success, err = await download_file(session, url, save_path)
                    if success:
                        return (url, save_path, "成功")
                    else:
                        return (url, save_path, f"失败: {err}")

            tasks.append(sem_task())

        for future in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc=f"下载 {os.path.basename(save_dir)}"):
            url, save_path, status = await future
            print(f"{status}: {url} -> {save_path}")

async def main():
    await asyncio.gather(
        download_list(MY_URLS_FILE, MY_SAVE_DIR),
        download_list(NET_URLS_FILE, NET_SAVE_DIR),
    )

if __name__ == "__main__":
    asyncio.run(main())
