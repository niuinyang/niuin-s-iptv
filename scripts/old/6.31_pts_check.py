#!/usr/bin/env python3
import subprocess
import json
import argparse
import statistics

def probe_pts(url, max_frames=30, timeout=15):
    """
    使用 ffprobe 读取视频帧的 pkt_pts_time
    """
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v",
        "-show_entries", "frame=pkt_pts_time",
        "-of", "json",
        "-read_intervals", f"%+{max_frames}",
        url
    ]

    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            text=True
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "reason": "timeout",
        }

    if proc.returncode != 0:
        return {
            "ok": False,
            "reason": "ffprobe_error",
            "stderr": proc.stderr.strip()
        }

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {
            "ok": False,
            "reason": "json_parse_error"
        }

    frames = data.get("frames", [])
    pts_list = []

    for f in frames:
        pts = f.get("pkt_pts_time")
        if pts is None:
            continue
        try:
            pts_list.append(float(pts))
        except ValueError:
            continue

    if len(pts_list) < 5:
        return {
            "ok": False,
            "reason": "too_few_pts",
            "pts_count": len(pts_list)
        }

    # 判断是否单调递增（允许一次小回退）
    backward = 0
    for i in range(1, len(pts_list)):
        if pts_list[i] <= pts_list[i - 1]:
            backward += 1

    pts_span = max(pts_list) - min(pts_list)

    return {
        "ok": True,
        "pts_count": len(pts_list),
        "pts_span": round(pts_span, 3),
        "backward_count": backward,
        "is_monotonic": backward <= 1,
        "pts_samples": pts_list[:10]  # 仅保留前10个，便于调试
    }

def main():
    parser = argparse.ArgumentParser(description="PTS 时间推进检测（直播/轮回识别）")
    parser.add_argument("--input", required=True, help="输入 CSV（必须包含 地址 列）")
    parser.add_argument("--output", required=True, help="输出 JSON 文件")
    parser.add_argument("--frames", type=int, default=30, help="采样帧数")
    parser.add_argument("--timeout", type=int, default=15, help="超时秒")
    args = parser.parse_args()

    results = {}

    import csv
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("地址", "").strip()
            if not url.startswith("http"):
                continue

            print(f"⏱️ 检测 PTS: {url}")
            results[url] = probe_pts(
                url,
                max_frames=args.frames,
                timeout=args.timeout
            )

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n✅ PTS 检测完成，结果已保存到 {args.output}")

if __name__ == "__main__":
    main()
