#!/usr/bin/env python3
# download_m3u.py
# 双源下载：network + mysource
# 不写任何错误日志文件

import os
import sys
import re
import time
import random
import logging
from urllib.parse import urlparse, unquote
import requests

# ==============================
# 配置
# ==============================
NETWORK_SOURCE_LIST = "input/source/networksource.txt"
NETWORK_OUTPUT_DIR = "input/download/network"

MYSOURCE_LIST = "input/source/mysource.txt"
MYSOURCE_OUTPUT_DIR = "input/download/my"

LOG_FILE = "download_m3u.log"

RETRIES = 3
BACKOFF_BASE = 2
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
MIN_SIZE_BYTES = 200
M3U_KEYWORDS = ["#EXTM3U", ".m3u", ".m3u8"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

# ==============================
# 日志设置
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# ==============================
# 工具函数
# ==============================
def sanitize_filename(s: str) -> str:
    s = unquote(s)
    s = re.sub(r"[:/?#\[\]@!$&'()*+,;=\"<>\\|]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:200]

def guess_filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    if not name:
        name = parsed.netloc
    name = sanitize_filename(name)
    if not os.path.splitext(name)[1]:
        name += ".m3u"
    return name

def looks_like_m3u(content_bytes: bytes) -> bool:
    try:
        txt = content_bytes[:1024].decode("utf-8", errors="ignore").lower()
    except Exception:
        return False
    return any(kw.lower() in txt for kw in M3U_KEYWORDS)

def download_url(url: str, out_path: str) -> (bool, str):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": f"https://{urlparse(url).netloc}/",
        "Connection": "keep-alive",
    }

    temp_path = out_path + ".tmp"

    for attempt in range(1, RETRIES + 1):
        try:
            logging.info(f"Downloading ({attempt}/{RETRIES}): {url}")

            with requests.get(url, headers=headers, stream=True,
                              timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True) as r:
                if r.status_code != 200:
                    raise Exception(f"HTTP {r.status_code}")

                with open(temp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                size = os.path.getsize(temp_path)
                if size < MIN_SIZE_BYTES:
                    raise Exception(f"File too small ({size} bytes)")

                with open(temp_path, "rb") as f:
                    head = f.read(2048)

                if not looks_like_m3u(head):
                    logging.warning("Content does not look like M3U")

                os.replace(temp_path, out_path)
                return True, "OK"

        except Exception as e:
            wait = BACKOFF_BASE ** (attempt - 1)
            logging.warning(f"Attempt {attempt} failed for {url}: {e}. Backoff {wait}s")
            time.sleep(wait + random.random())

    if os.path.exists(temp_path):
        os.remove(temp_path)

    return False, f"Failed after {RETRIES} attempts"

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def read_url_list(path: str):
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line.split()[0])
    return urls

# 重命名mysource目录下载的前三个文件
def rename_mysource_files(output_dir, downloaded_files):
    rename_map = {
        0: "1sddxzb.m3u",
        1: "2sddxdb.m3u",
        2: "3jnltzb.m3u",
    }

    for idx, orig_name in enumerate(downloaded_files):
        if idx in rename_map:
            new_name = rename_map[idx]
            orig_path = os.path.join(output_dir, orig_name)
            new_path = os.path.join(output_dir, new_name)
            try:
                # 如果目标文件存在，先删除，避免rename失败
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.rename(orig_path, new_path)
                logging.info(f"Renamed {orig_name} -> {new_name}")
            except Exception as e:
                logging.warning(f"Failed to rename {orig_name} to {new_name}: {e}")

# ==============================
# 下载流程（复用）
# ==============================
def process_source(source_list, output_dir, clean_old_files=True, is_mysource=False):
    ensure_dir(output_dir)

    if not os.path.exists(source_list):
        logging.error(f"Source list not found: {source_list}")
        return

    urls = read_url_list(source_list)
    total = len(urls)
    logging.info(f"Processing {source_list}, total URLs: {total}")

    downloaded_files = []

    for url in urls:
        fname = guess_filename_from_url(url)
        out_path = os.path.join(output_dir, fname)
        success, msg = download_url(url, out_path)

        if success:
            logging.info(f"Saved: {out_path}")
            downloaded_files.append(fname)
        else:
            logging.error(f"Failed: {url} -> {msg}")

    if clean_old_files:
        # 清理旧文件
        for f in os.listdir(output_dir):
            if f not in downloaded_files:
                path = os.path.join(output_dir, f)
                try:
                    os.remove(path)
                    logging.info(f"Removed old file: {path}")
                except Exception as e:
                    logging.warning(f"Failed to remove {path}: {e}")

    # 如果是mysource，执行重命名
    if is_mysource:
        rename_mysource_files(output_dir, downloaded_files)

    logging.info(f"Completed {source_list}: {total} URLs")

# ==============================
# 主程序
# ==============================
def main():
    # network 目录，保留清理旧文件行为
    process_source(NETWORK_SOURCE_LIST, NETWORK_OUTPUT_DIR, clean_old_files=True)
    # mysource 目录，不删除旧文件，只下载覆盖，并重命名前三个
    process_source(MYSOURCE_LIST, MYSOURCE_OUTPUT_DIR, clean_old_files=False, is_mysource=True)
    logging.info("All tasks done.")

if __name__ == "__main__":
    main()
