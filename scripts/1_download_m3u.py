#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os                                         # 导入操作系统接口模块
import asyncio                                    # 导入异步IO模块
import aiohttp                                    # 导入异步HTTP客户端模块
import aiofiles.os                                # 导入异步文件操作模块（os部分）
from urllib.parse import urlparse                 # 导入URL解析函数
from tqdm.asyncio import tqdm_asyncio             # 导入异步任务进度条工具

MY_URLS_FILE = "input/source/my.txt"              # 定义 my URL 文件路径
NET_URLS_FILE = "input/source/net.txt"            # 定义 net URL 文件路径

MY_SAVE_DIR = "input/download/my"                  # 定义 my 文件保存目录
NET_SAVE_DIR = "input/download/net/original"      # 定义 net 文件保存目录

CONCURRENCY = 10                                   # 设置最大并发数为10

HEADERS = {                                        # 定义请求头，伪装浏览器请求
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/"          # 伪造Referer，部分网站需要
}

def parse_url_line(line):                           # 解析URL文件中一行，返回URL和文件名
    line = line.strip()                             # 去除行首尾空白符
    if not line or line.startswith("#"):            # 跳过空行和注释行
        return None, None
    parts = line.split("#", 1)                      # 用#分割，最多分1次
    url = parts[0].strip()                          # 取URL部分
    filename_base = parts[1].strip() if len(parts) > 1 else None  # 取自定义文件名部分（无扩展名）

    parsed = urlparse(url)                          # 解析URL结构
    path = parsed.path                              # 取URL路径部分
    ext = os.path.splitext(path)[1]                 # 取路径中的文件扩展名
    if not ext:                                     # 若无扩展名
        ext = ".m3u"                               # 默认扩展名设为 .m3u

    if filename_base:                               # 如果有自定义文件名
        filename = filename_base + ext             # 用自定义文件名 + 扩展名
    else:
        filename = os.path.basename(path) or ("downloaded_file" + ext)  # 否则用路径最后部分，若无则默认命名

    return url, filename                            # 返回URL和文件名

async def download_file(session, url, save_path):  # 异步下载单个文件
    try:
        async with session.get(url, headers=HEADERS, timeout=30) as resp:  # 发起GET请求，30秒超时
            if resp.status == 200:                   # 如果响应成功
                data = await resp.read()             # 读取响应内容
                async with aiofiles.open(save_path, "wb") as f:  # 异步写文件
                    await f.write(data)
                return True, None                     # 返回成功标记和无错误
            else:
                return False, f"HTTP status {resp.status}"  # 返回失败和状态码
    except Exception as e:
        return False, str(e)                          # 返回异常信息

async def remove_old_files_async(directory, keep_files):  # 异步删除目录中不需要保留的旧文件
    if not os.path.exists(directory):                  # 若目录不存在，直接返回
        return
    for f in await aiofiles.os.listdir(directory):     # 异步列出目录中的文件
        if f not in keep_files:                         # 如果文件不在保留列表中
            try:
                path = os.path.join(directory, f)      # 构造完整路径
                await aiofiles.os.remove(path)          # 异步删除文件
                print(f"删除旧文件: {path}")            # 打印删除成功信息
            except Exception as e:
                print(f"删除文件失败: {path}，原因: {e}")  # 删除失败时打印异常

async def rename_mysource_files_async(output_dir, downloaded_files):  # 异步重命名 my 目录前三个文件
    rename_map = {                                     # 定义重命名映射索引 -> 目标文件名
        0: "1sddxzb.m3u",
        1: "2sddxdb.m3u",
        2: "3jnltzb.m3u",
    }

    for idx, orig_name in enumerate(downloaded_files):  # 遍历已下载文件及其索引
        if idx in rename_map:                          # 如果索引在重命名映射中
            new_name = rename_map[idx]                 # 获取目标重命名
            orig_path = os.path.join(output_dir, orig_name)  # 原文件路径
            new_path = os.path.join(output_dir, new_name)    # 新文件路径
            try:
                if await aiofiles.os.path.exists(new_path):  # 若新文件已存在
                    await aiofiles.os.remove(new_path)       # 先删除，避免重命名失败
                await aiofiles.os.rename(orig_path, new_path) # 异步重命名
                print(f"重命名成功: {orig_name} -> {new_name}") # 打印成功信息
            except Exception as e:
                print(f"重命名失败: {orig_name} -> {new_name}: {e}") # 打印失败信息

async def download_list(url_file, save_dir, clean_old_files=True, do_rename=False):  # 异步批量下载任务
    if not os.path.exists(url_file):                   # 若URL文件不存在
        print(f"⚠️ URL文件不存在：{url_file}")          # 打印警告
        return
    os.makedirs(save_dir, exist_ok=True)               # 确保下载目录存在

    url_list = []                                      # 用于保存解析后的URL和文件名对
    with open(url_file, "r", encoding="utf-8") as f:  # 打开URL文件读取
        for line in f:
            url, filename = parse_url_line(line)       # 解析一行得到URL和文件名
            if url:
                url_list.append((url, filename))       # 加入列表

    print(f"准备下载 {len(url_list)} 个文件到 {save_dir}")  # 打印准备下载的文件数量

    semaphore = asyncio.Semaphore(CONCURRENCY)         # 创建信号量控制并发数量
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)  # aiohttp连接器限制并发且关闭SSL验证

    downloaded_files = []                               # 记录下载成功的文件名

    async with aiohttp.ClientSession(connector=connector) as session:  # 创建aiohttp会话
        tasks = []                                      # 任务列表

        for url, filename in url_list:                  # 遍历所有待下载的URL
            save_path = os.path.join(save_dir, filename)  # 构造保存路径

            async def sem_task(url=url, save_path=save_path, filename=filename):  # 定义单任务（带信号量）
                async with semaphore:                  # 获取信号量，限制并发
                    success, err = await download_file(session, url, save_path)  # 执行下载
                    if success:
                        print(f"成功: {url} -> {save_path}")    # 成功打印
                        return filename
                    else:
                        print(f"失败: {url} -> {save_path}，错误：{err}")  # 失败打印
                        return None

            tasks.append(sem_task())                     # 加入任务列表

        results = []                                    # 保存任务结果
        for future in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc=f"下载 {os.path.basename(save_dir)}"):
            res = await future                          # 异步等待任务完成
            results.append(res)                          # 记录结果
        downloaded_files = [f for f in results if f]   # 过滤成功的文件名

    if clean_old_files:                                 # 如果需要清理旧文件
        await remove_old_files_async(save_dir, downloaded_files)  # 执行清理操作

    if do_rename:                                       # 如果需要重命名
        await rename_mysource_files_async(save_dir, downloaded_files)  # 执行重命名操作

    return downloaded_files                             # 返回成功下载的文件名列表

async def main():                                      # 主异步函数
    task_net = download_list(NET_URLS_FILE, NET_SAVE_DIR, clean_old_files=True, do_rename=False)  # network任务，下载并清理不重命名
    task_my = download_list(MY_URLS_FILE, MY_SAVE_DIR, clean_old_files=False, do_rename=True)     # my任务，下载不清理，重命名
    await asyncio.gather(task_net, task_my)            # 并发执行两个任务

if __name__ == "__main__":                            # 脚本入口
    asyncio.run(main())                                # 运行主异步函数
