#!/usr/bin/env python3
# scripts/4.2deep_scan.py
import asyncio
import csv
import json
import argparse
import time    # 新增导入time
from asyncio.subprocess import create_subprocess_exec, PIPE
from tqdm.asyncio import tqdm_asyncio
from asyncio import Semaphore

async def ffprobe_json(url, timeout=20):
    cmd = ["ffprobe","-v","quiet","-print_format","json","-show_streams","-show_format", url]
    try:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return {"url": url, "error": "timeout"}
        if stdout:
            try:
                data = json.loads(stdout.decode('utf-8', errors='ignore'))
                return {"url": url, "probe": data}
            except Exception as e:
                return {"url": url, "error": f"json_parse_error: {e}"}
        else:
            return {"url": url, "error": stderr.decode('utf-8', errors='ignore') or "no_output"}
    except FileNotFoundError:
        return {"url": url, "error": "ffprobe_not_installed"}

def parse_probe(probe):
    info = {
        "has_video": False, "has_audio": False, "video_codec": None,
        "width": None, "height": None, "frame_rate": None,
        "duration": None, "bit_rate": None
    }
    if not probe:
        return info
    streams = probe.get("streams", [])
    for s in streams:
        if s.get("codec_type") == "video":
            info["has_video"] = True
            info["video_codec"] = s.get("codec_name")
            info["width"] = s.get("width")
            info["height"] = s.get("height")
            r = s.get("avg_frame_rate") or s.get("r_frame_rate")
            if r and "/" in str(r):
                num, den = r.split("/")
                try:
                    info["frame_rate"] = float(num) / float(den) if float(den) != 0 else None
                except Exception:
                    info["frame_rate"] = None
    fmt = probe.get("format", {})
    info["duration"] = float(fmt.get("duration")) if fmt.get("duration") else None
    info["bit_rate"] = int(fmt.get("bit_rate")) if fmt.get("bit_rate") else None
    if any(s.get("codec_type") == "audio" for s in streams):
        info["has_audio"] = True
    return info

async def probe_one(row, sem, timeout):
    async with sem:
        url = row["地址"]
        start = time.perf_counter()  # 计时开始
        res = await ffprobe_json(url, timeout=timeout)
        elapsed = time.perf_counter() - start  # 计时结束，计算耗时
        if "probe" in res:
            parsed = parse_probe(res["probe"])
            result = dict(row)
            result.update({
                "has_video": parsed["has_video"],
                "has_audio": parsed["has_audio"],
                "video_codec": parsed["video_codec"] or "",
                "width": parsed["width"] or "",
                "height": parsed["height"] or "",
                "frame_rate": parsed["frame_rate"] or "",
                "duration": parsed["duration"] or "",
                "bit_rate": parsed["bit_rate"] or "",
                "error": "",
                "elapsed": elapsed  # 新增字段
            })
            return result, True
        else:
            result = dict(row)
            result.update({
                "has_video": False,
                "has_audio": False,
                "video_codec": "",
                "width": "",
                "height": "",
                "frame_rate": "",
                "duration": "",
                "bit_rate": "",
                "error": res.get("error", "unknown"),
                "elapsed": elapsed  # 新增字段
            })
            return result, False

async def deep_scan(input_file, output_ok, output_fail, concurrency, timeout):
    sem = Semaphore(concurrency)
    rows = []
    with open(input_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 这里不再判断检测时间，所有行都检测
            rows.append(row)

    print(f"Probing {len(rows)} urls with concurrency={concurrency}")

    tasks = [probe_one(row, sem, timeout) for row in rows]
    results_ok = []
    results_fail = []

    # 这里改为普通for循环，await每个future
    for fut in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="deep-scan"):
        result, ok = await fut
        if ok:
            results_ok.append(result)
        else:
            results_fail.append(result)

    fieldnames_ok = ["频道名","地址","来源","图标","检测时间","分组","视频编码","分辨率","帧率","音频"]
    fieldnames_fail = fieldnames_ok + ["失败原因"]

    def format_resolution(r):
        w = r.get("width")
        h = r.get("height")
        if w and h:
            return f"{w}x{h}"
        return ""

    def format_audio(r):
        return "有音频" if r.get("has_audio") else "无音频"

    with open(output_ok, "w", newline='', encoding='utf-8') as f_ok, \
         open(output_fail, "w", newline='', encoding='utf-8') as f_fail:
        writer_ok = csv.DictWriter(f_ok, fieldnames=fieldnames_ok)
        writer_fail = csv.DictWriter(f_fail, fieldnames=fieldnames_fail)
        writer_ok.writeheader()
        writer_fail.writeheader()

        for r in results_ok:
            row = {
                "频道名": r.get("频道名",""),
                "地址": r.get("地址",""),
                "来源": r.get("来源",""),
                "图标": r.get("图标",""),
                # 这里检测时间赋值为耗时，保留三位小数
                "检测时间": f"{r.get('elapsed', 0):.3f}",
                "分组": r.get("分组",""),
                "视频编码": r.get("video_codec",""),
                "分辨率": format_resolution(r),
                "帧率": r.get("frame_rate",""),
                "音频": format_audio(r),
            }
            writer_ok.writerow(row)

        for r in results_fail:
            row = {
                "频道名": r.get("频道名",""),
                "地址": r.get("地址",""),
                "来源": r.get("来源",""),
                "图标": r.get("图标",""),
                "检测时间": f"{r.get('elapsed', 0):.3f}",
                "分组": r.get("分组",""),
                "视频编码": r.get("video_codec",""),
                "分辨率": format_resolution(r),
                "帧率": r.get("frame_rate",""),
                "音频": format_audio(r),
                "失败原因": r.get("error",""),
            }
            writer_fail.writerow(row)

    print(f"Deep scan finished: {len(results_ok)} success, {len(results_fail)} failed. Wrote {output_ok} and {output_fail}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--invalid", required=True)
    parser.add_argument("--concurrency", type=int, default=30)
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    asyncio.run(deep_scan(args.input, args.output, args.invalid, args.concurrency, args.timeout))

if __name__ == "__main__":
    main()
