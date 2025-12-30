#!/usr/bin/env python3
import os
import re

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/middle/chunk"

def clean_dir(path):
    """删除目录内所有文件，但保留所有子目录结构"""
    if not os.path.exists(path):
        return
    for root, dirs, files in os.walk(path):
        for f in files:
            os.remove(os.path.join(root, f))

print("🧹 清空旧的 fast / deep / final 结果文件...")

clean_dir("output/middle/fast")
clean_dir("output/middle/deep")
clean_dir("output/middle/final")

os.makedirs(WORKFLOW_DIR, exist_ok=True)

TEMPLATE = """name: Scan_{n}

on:
  workflow_run:
    workflows: ["1-预处理-下载-转txt-合并-分割-生成"]
    types:
      - completed
  workflow_dispatch:

permissions:
  contents: write

jobs:
  scan_{n}:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install ffmpeg
        run: sudo apt-get update && sudo apt-get install -y ffmpeg

      - name: Cache pip
        uses: actions/cache@v3
        with:
          path: ~/.cache/pip
          key: ${{{{ runner.os }}}}-pip-${{{{ hashFiles('requirements.txt') }}}}
          restore-keys: |
            ${{{{ runner.os }}}}-pip-

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run fast scan for {n}
        run: |
          mkdir -p output/middle/fast/ok output/middle/fast/not
          python scripts/6.1_fast_scan.py \\
            --input output/middle/chunk/{n}.csv \\
            --output output/middle/fast/ok/fast_{n}.csv \\
            --invalid output/middle/fast/not/fast_{n}-invalid.csv

      - name: Run deep scan for {n}
        run: |
          mkdir -p output/middle/deep/ok output/middle/deep/not
          python scripts/6.2_deep_scan.py \\
            --input output/middle/fast/ok/fast_{n}.csv \\
            --output output/middle/deep/ok/deep_{n}.csv \\
            --invalid output/middle/deep/not/deep_{n}-invalid.csv
    
      - name: Run final scan for {n}
        run: |
          mkdir -p output/middle/final/ok output/middle/final/not
          python scripts/6.3_final_scan.py \\
            --input output/middle/deep/ok/deep_{n}.csv \\
            --output output/middle/final/ok/final_{n}.csv \\
            --invalid output/middle/final/not/final_{n}-invalid.csv \\
            --cache_dir output/cache
            
      - name: Commit and Push Outputs
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add output/middle/fast \\
                  output/middle/deep \\
                  output/middle/final \\
                  output/cache

          if git diff --cached --quiet; then
            echo "No output updates."
            exit 0
          fi

          git commit -m "Update scan outputs for {n} [skip ci]"

          MAX_RETRIES=5
          COUNT=1

          until git push --quiet; do
            echo "Push failed (attempt $COUNT/$MAX_RETRIES), retrying..."

            git stash push -m "auto-stash" || true
            git pull --rebase --quiet || true
            git stash pop || true

            COUNT=$((COUNT+1))
            if [ $COUNT -gt $MAX_RETRIES ]; then
              echo "🔥 Push failed after $MAX_RETRIES attempts."
              exit 1
            fi

            sleep 2
          done

          echo "Push outputs succeeded."
"""

print("🧹 清理旧的 workflow 文件...")

for f in os.listdir(WORKFLOW_DIR):
    if re.match(r"scan_.+\.yml", f):
        os.remove(os.path.join(WORKFLOW_DIR, f))

if not os.path.exists(CHUNK_DIR):
    raise RuntimeError(f"❌ CHUNK_DIR 不存在：{CHUNK_DIR}")

chunks = sorted([
    f for f in os.listdir(CHUNK_DIR)
    if re.match(r"chunk-\d+\.csv", f)
])

if not chunks:
    raise RuntimeError(f"❌ 未找到任何 chunk CSV 文件，请检查目录：{CHUNK_DIR}")

print(f"📦 找到 {len(chunks)} 个 chunk 文件")

for chunk_file in chunks:
    chunk_id = os.path.splitext(chunk_file)[0]
    workflow_filename = f"scan_{chunk_id}.yml"
    workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename)

    with open(workflow_path, "w", encoding="utf-8") as f:
        f.write(TEMPLATE.format(n=chunk_id))

    print(f"✅ 已生成 workflow: {workflow_filename}")

print("\n🌀 生成 workflow 完成。请提交并推送。")
