#!/usr/bin/env python3
import os
import re

WORKFLOW_DIR = ".github/workflows"
CHUNK_DIR = "output/middle/chunk"
HASH_CHUNK_DIR = "output/hash/chunk"  # 新增：chunk hash 目录

os.makedirs(WORKFLOW_DIR, exist_ok=True)

# ============================================================
# Scan workflow 模板
# 👉 核心修改点：
# 1. 弃用 artifact 下载，改为 git fetch origin main + reset 同步代码
# 2. 增加 "拉取远端最新代码、合并结果文件并提交" 步骤，支持重试
# 3. 删除 artifact 上传步骤
# 4. 增加 checkout fetch-depth:0，确保完整拉取历史
# 5. 增加 git 认证 token 环境变量 PUSH_TOKEN1，供 reset 和 push 使用
# 6. 修改 git reset --hard 为 git pull --rebase，避免覆盖其他 chunk 提交
# ============================================================

TEMPLATE = """name: Scan_{n}

on:
  workflow_run:
    workflows: ["1-预处理-下载-转txt-合并-分割-生成"]
    types:
      - completed
  workflow_dispatch:

permissions:
  contents: write
# >>> MODIFIED: 需要写权限以提交代码

jobs:
  scan_{n}:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0  # >>> MODIFIED: 拉取完整历史，确保 reset 工作正常

      - name: 同步最新代码（pull --rebase 替代 reset）
        env:
          PUSH_TOKEN1: ${{{{ secrets.PUSH_TOKEN1 }}}}  # >>> MODIFIED: 添加 git token
          REPO: ${{{{ github.repository }}}}
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git remote set-url origin https://x-access-token:${{PUSH_TOKEN1}}@github.com/${{REPO}}.git
          git fetch origin main
          git pull --rebase origin main  # <<< FIX: 避免 hard reset 覆盖其他 chunk 提交

      - name: Check chunk files
        run: ls -lh output/middle/chunk

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

      - name: Run hash scan for {n}
        run: |
          mkdir -p output/hash/chunk
          python scripts/6.3_hash_scan.py \\
            --input output/middle/deep/ok/deep_{n}.csv \\
            --output output/hash/chunk/hash_{n}.json \\
            --concurrency 15 \\
            --timeout 15 \\
            --retry 2

      - name: Commit and push scan results with retry
        env:
          PUSH_TOKEN1: ${{{{ secrets.PUSH_TOKEN1 }}}}
          REPO: ${{{{ github.repository }}}}
          BRANCH: main
        run: |
          set -e
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git remote set-url origin https://x-access-token:${{PUSH_TOKEN1}}@github.com/${{REPO}}.git

          MAX_RETRIES=5
          RETRY_DELAY=10
          RETRY_COUNT=0

          while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
            echo "尝试拉取最新代码并合并，尝试次数: $((RETRY_COUNT + 1))"
            git fetch origin ${{BRANCH}}
            git pull --rebase origin ${{BRANCH}}  # <<< FIX: 避免 hard reset 覆盖其他 chunk 提交

            # 复制当前 workflow 产生的结果文件到 repo，添加到 git 暂存区
            git add output/middle/fast/ok/fast_{n}.csv output/middle/fast/not/fast_{n}-invalid.csv output/middle/deep/ok/deep_{n}.csv output/middle/deep/not/deep_{n}-invalid.csv output/hash/chunk/hash_{n}.json

            # 检查是否有变更
            if git diff --cached --quiet; then
              echo "没有新变更，退出提交"
              exit 0
            fi

            git commit -m "自动提交 Scan_{n} 扫描结果 [ci skip]" || echo "无新提交内容"

            # 尝试推送
            if git push origin ${{BRANCH}}; then
              echo "推送成功"
              exit 0
            else
              echo "推送失败，等待 $RETRY_DELAY 秒后重试"
              RETRY_COUNT=$((RETRY_COUNT + 1))
              sleep $RETRY_DELAY
            fi
          done

          echo "超过最大重试次数，推送失败"
          exit 1
"""

print("🧹 清理旧的 scan_* workflow 文件...")

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

print("\n🌀 Scan workflow 生成完成，请提交并推送。")