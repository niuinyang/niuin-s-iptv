#!/usr/bin/env python3
import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/middle/chunk"
CACHE_DIR = "output/cache/chunk"

os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# 时间点顺序，保证每次生成的时间点轮转
TIME_KEYS = ["0811", "1612", "2113"]
TIME_POINTS = {
    "0811": "08:11",
    "1612": "16:12",
    "2113": "21:13"
}

MANUAL_TRIGGER_RECORD = "output/cache/manual_trigger_record.json"


def get_chunks():
    files = os.listdir(CHUNK_DIR)
    chunks = [f for f in files if f.startswith("chunk-") and f.endswith(".csv")]
    return sorted(chunks)


def load_manual_record():
    if os.path.exists(MANUAL_TRIGGER_RECORD):
        with open(MANUAL_TRIGGER_RECORD, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return {}


def save_manual_record(data, days_keep=7):
    """
    保存 manual_record，只保留最近 days_keep 天的数据
    """
    os.makedirs(os.path.dirname(MANUAL_TRIGGER_RECORD), exist_ok=True)

    beijing_tz = ZoneInfo("Asia/Shanghai")
    today_dt = datetime.now(beijing_tz)

    filtered = {}
    for day_str, count in data.items():
        try:
            day_date = datetime.strptime(day_str, "%Y%m%d")
            day_date = day_date.replace(tzinfo=beijing_tz)
            if (today_dt - day_date).days < days_keep:
                filtered[day_str] = count
        except Exception:
            filtered[day_str] = count

    with open(MANUAL_TRIGGER_RECORD, "w", encoding="utf-8") as f:
        json.dump(filtered, f, indent=2, ensure_ascii=False)


def generate_workflow(chunk_name, time_key, time_str, manual_count):
    base = os.path.splitext(chunk_name)[0]  # chunk-1
    workflow_name = f"hash-{base}-{time_key}"
    filename = f"{workflow_name}.yml"
    filepath = os.path.join(WORKFLOW_DIR, filename)

    chunk_id = base.split("-")[1]  # 1

    content = f"""name: {workflow_name}

on:
  workflow_run:
    workflows: ["A生成并执行缓存workflow"]
    types:
      - completed
  workflow_dispatch:
    inputs:
      manual_count:
        description: 'Manual trigger count'
        required: false
        default: '{manual_count}'

permissions:
  contents: write

jobs:
  run-cache:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.x'

      - name: Install dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y ffmpeg
          pip install pillow tqdm imagehash

      - name: Run cache script
        run: |
          python scripts/B_1cache.py --input {CHUNK_DIR}/{chunk_name} --timepoint {time_key} --chunk_id {chunk_id}

      - name: Push cache changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add output/cache/
          git commit -m "Update cache (manual record + chunk output)" || echo "No changes to commit"
          git pull --rebase origin main || true
          git push origin HEAD:main || true
        env:
          GITHUB_TOKEN: ${{{{ secrets.PUSH_TOKEN1 }}}}
"""

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"✅ Generated workflow: {filename}")


def main():
    beijing_tz = ZoneInfo("Asia/Shanghai")
    today = datetime.now(beijing_tz).strftime("%Y%m%d")

    chunks = get_chunks()
    manual_record = load_manual_record()
    manual_count = manual_record.get(today, 0)

    keys = TIME_KEYS
    time_key = keys[manual_count % len(keys)]
    time_str = TIME_POINTS[time_key]

    print(f"Manual trigger count today: {manual_count}, will generate workflows for time point {time_key} ({time_str})")

    for chunk in chunks:
        generate_workflow(chunk, time_key, time_str, manual_count)

    manual_record[today] = manual_count + 1

    save_manual_record(manual_record)


if __name__ == "__main__":
    main()
