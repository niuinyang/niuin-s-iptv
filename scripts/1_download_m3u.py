#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import aiohttp
import aiofiles
import aiofiles.os
from urllib.parse import urlparse
from tqdm.asyncio import tqdm_asyncio

MY_URLS_FILE = "input/source/my.txt"
NET_URLS_FILE = "input/source/net.txt"

MY_SAVE_DIR = "input/download/my"
NET_SAVE_DIR = "input/download/net/original"

CONCURRENCY = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/"
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
                async with aiofiles.open(save_path, "wb") as f:
                    await f.write(data)
                return True, None
            else:
                return False, f"HTTP status {resp.status}"
    except Exception as e:
        return False, str(e)

async def remove_old_files_async(directory, keep_files):
    if not os.path.exists(directory):
        return
    for f in await aiofiles.os.listdir(directory):
        if f not in keep_files:
            try:
                path = os.path.join(directory, f)
                await aiofiles.os.remove(path)
                print(f"删除旧文件: {path}")
            except Exception as e:
                print(f"删除文件失败: {path}，原因: {e}")

async def download_list(url_file, save_dir, clean_old_files=True):
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

    downloaded_files = []

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []

        for url, filename in url_list:
            save_path = os.path.join(save_dir, filename)

            async def sem_task(url=url, save_path=save_path, filename=filename):
                async with semaphore:
                    success, err = await download_file(session, url, save_path)
                    if success:
                        print(f"成功: {url} -> {save_path}")
                        return filename
                    else:
                        print(f"失败: {url} -> {save_path}，错误：{err}")
                        return None

            tasks.append(sem_task())

        results = []
        for future in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc=f"下载 {os.path.basename(save_dir)}"):
            res = await future
            results.append(res)

        downloaded_files = [f for f in results if f]

    if clean_old_files:
        await remove_old_files_async(save_dir, downloaded_files)

    return downloaded_files

async def main():
    task_net = download_list(
        NET_URLS_FILE,
        NET_SAVE_DIR,
        clean_old_files=True
    )

    task_my = download_list(
        MY_URLS_FILE,
        MY_SAVE_DIR,
        clean_old_files=False
    )

    await asyncio.gather(task_net, task_my)

if __name__ == "__main__":
    asyncio.run(main())
