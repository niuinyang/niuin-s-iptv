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
            if not parsed["has_video"]:
                # 标记为无视频流错误
                result = dict(row)
                result.update({
                    "has_video": False,
                    "has_audio": parsed["has_audio"],
                    "video_codec": "",
                    "width": "",
                    "height": "",
                    "frame_rate": "",
                    "duration": "",
                    "bit_rate": "",
                    "error": "no_video_stream",
                    "elapsed": elapsed
                })
                return result, False
            else:
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
        reader = csv.DictReader(f)  # 默认逗号分隔
        fieldnames_in = reader.fieldnames  # 记录输入所有列名
        for row in reader:
            rows.append(row)

    print(f"Probing {len(rows)} urls with concurrency={concurrency}")

    tasks = [probe_one(row, sem, timeout) for row in rows]
    results_ok = []
    results_fail = []

    for fut in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="deep-scan"):
        result, ok = await fut
        if ok:
            results_ok.append(result)
        else:
            results_fail.append(result)

    # 新增列
    new_fields = [
        "ffprobe是否成功", "视频编码", "分辨率", "帧率", "音频", "ffprobe探测时间"
    ]
    # 输出字段：输入所有列 + 新增列
    fieldnames_out = fieldnames_in + new_fields

    def format_resolution(r):
        w = r.get("width")
        h = r.get("height")
        if w and h:
            return f"{w}x{h}"
        return ""

    def format_audio(r):
        return "有音频" if r.get("has_audio") else "无音频"

    def classify_error(r):
        err = r.get("error", "")
        if err == "":
            return "成功"
        if "timeout" in err:
            return "超时"
        if "no_output" in err:
            return "无输出"
        if "json_parse_error" in err:
            return "解析错误"
        if "ffprobe_not_installed" in err:
            return "ffprobe未安装"
        if err == "no_video_stream":
            return "无视频流"
        return err  # 其他错误原样返回

    with open(output_ok, "w", newline='', encoding='utf-8') as f_ok, \
         open(output_fail, "w", newline='', encoding='utf-8') as f_fail:
        writer_ok = csv.DictWriter(f_ok, fieldnames=fieldnames_out, delimiter='\t')
        writer_fail = csv.DictWriter(f_fail, fieldnames=fieldnames_out, delimiter='\t')

        writer_ok.writeheader()
        writer_fail.writeheader()

        for r in results_ok:
            out_row = dict(r)
            out_row["ffprobe是否成功"] = classify_error(r)
            out_row["视频编码"] = r.get("video_codec", "")
            out_row["分辨率"] = format_resolution(r)
            out_row["帧率"] = r.get("frame_rate", "")
            out_row["音频"] = format_audio(r)
            out_row["ffprobe探测时间"] = f"{r.get('elapsed', 0):.3f}"
            writer_ok.writerow(out_row)

        for r in results_fail:
            out_row = dict(r)
            out_row["ffprobe是否成功"] = classify_error(r)
            out_row["视频编码"] = r.get("video_codec", "")
            out_row["分辨率"] = format_resolution(r)
            out_row["帧率"] = r.get("frame_rate", "")
            out_row["音频"] = format_audio(r)
            out_row["ffprobe探测时间"] = f"{r.get('elapsed', 0):.3f}"
            writer_fail.writerow(out_row)

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
