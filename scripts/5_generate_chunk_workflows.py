#!/usr/bin/env python3
import os                           # 导入操作系统接口模块，用于文件和目录操作
import re                           # 导入正则表达式模块，用于字符串匹配
import json                         # 导入JSON模块，虽然本脚本未使用，但常用于JSON操作

WORKFLOW_DIR = ".github/workflows" # 定义GitHub Actions工作流文件存放的目录
CHUNK_DIR = "output/middle/chunk"  # 定义存放分片CSV文件的目录

# --------------------------------------------
# 定义函数：清空指定目录内的所有文件，但保留目录结构
# --------------------------------------------
def clean_dir(path):
    """删除目录内所有文件，但保留所有子目录结构"""
    if not os.path.exists(path):     # 如果目录不存在，直接返回不做任何操作
        return
    for root, dirs, files in os.walk(path):  # 遍历目录及其所有子目录
        for f in files:              # 遍历每个目录下的所有文件
            os.remove(os.path.join(root, f)) # 删除每个文件，保留目录不变

print("🧹 清空旧的 fast / deep / final 结果文件...")

clean_dir("output/middle/fast")    # 清理 fast 结果目录下的文件
clean_dir("output/middle/deep")    # 清理 deep 结果目录下的文件
clean_dir("output/middle/final")   # 清理 final 结果目录下的文件

# --------------------------------------------
# 确保 workflow 文件目录存在，如果不存在则创建
# --------------------------------------------
os.makedirs(WORKFLOW_DIR, exist_ok=True)

# 定义 GitHub Actions workflow 文件的模板字符串，后续会为每个 chunk 生成对应的 workflow 文件
TEMPLATE = """name: Scan_{n}

on:
  workflow_run:
    workflows: ["1预处理-下载-合并-分割-生成"]     # 监听该工作流完成后触发
    types:
      - completed                               # 仅当指定工作流完成时触发
  workflow_dispatch:                            # 支持手动触发

permissions:
  contents: write                              # 允许写入仓库内容

jobs:
  scan_{n}:
    runs-on: ubuntu-latest                     # 使用最新Ubuntu环境运行
    steps:
      - name: Checkout repository              # 第一步：检出仓库代码
        uses: actions/checkout@v4

      - name: Setup Python 3.11                  # 安装Python 3.11环境
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install ffmpeg                     # 安装ffmpeg工具，命令行视频处理工具
        run: sudo apt-get update && sudo apt-get install -y ffmpeg

      - name: Install dependencies               # 安装Python依赖，来自requirements.txt
        run: pip install -r requirements.txt

      - name: Run fast scan for {n}              # 运行快速扫描脚本，处理对应chunk文件
        run: |
          mkdir -p output/middle/fast/ok output/middle/fast/not   # 创建结果输出目录
          python scripts/6.1_fast_scan.py \                         # 执行快速扫描脚本
            --input output/middle/chunk/{n}.csv \                 # 输入对应chunk文件
            --output output/middle/fast/ok/fast_{n}.csv \         # 合格输出路径
            --invalid output/middle/fast/not/fast_{n}-invalid.csv # 不合格输出路径
            
      - name: Run deep scan for {n}              # 运行深度扫描脚本，进一步校验快速扫描合格数据
        run: |
          mkdir -p output/middle/deep/ok output/middle/deep/not   # 创建深度扫描结果目录
          python scripts/6.2_deep_scan.py \                        # 执行深度扫描脚本
            --input output/middle/fast/ok/fast_{n}.csv \          # 输入快速扫描合格文件
            --output output/middle/deep/ok/deep_{n}.csv \         # 深度扫描合格输出路径
            --invalid output/middle/deep/not/deep_{n}-invalid.csv # 深度扫描不合格输出路径

      - name: Run final scan for {n}             # 运行最终扫描脚本，做最后一步验证和处理
        run: |
          mkdir -p output/middle/final/ok output/middle/final/not   # 创建最终结果目录
          python scripts/6.3_final_scan.py \                        # 执行最终扫描脚本
            --input output/middle/deep/ok/deep_{n}.csv \           # 输入深度扫描合格文件
            --output output/middle/final/ok/final_{n}.csv \        # 最终合格输出路径
            --invalid output/middle/final/not/final_{n}-invalid.csv # 最终不合格输出路径
            --cache_dir output/cache                                # 指定缓存目录

      - name: Commit and Push Outputs               # 提交并推送扫描结果
        run: |
          git config user.name "github-actions[bot]"               # 设置提交用户名
          git config user.email "github-actions[bot]@users.noreply.github.com" # 设置提交邮箱

          git add output/cache \                                    # 添加缓存目录及扫描结果目录
                  output/middle/fast \
                  output/middle/deep \
                  output/middle/final

          if git diff --cached --quiet; then                      # 如果没有变更，输出提示并退出
            echo "No output updates."
            exit 0
          fi

          git commit -m "Update scan outputs for {n} [skip ci]"   # 提交变更，跳过CI触发

          MAX_RETRIES=5                                           # 最大重试次数设为5
          COUNT=1                                                 # 初始化重试计数器为1

          until git push --quiet; do                              # 循环执行push，直到成功或超过重试次数
            echo "Push failed (attempt $COUNT/$MAX_RETRIES), retrying..."

            git stash push -m "auto-stash" || true               # 保存当前变更到stash，防止冲突
            git pull --rebase --quiet || true                    # 拉取远程最新代码并rebase
            git stash pop || true                                 # 恢复stash内容

            COUNT=$((COUNT+1))                                    # 重试计数器加1
            if [ $COUNT -gt $MAX_RETRIES ]; then                  # 如果超过最大重试次数则退出并报错
              echo "🔥 Push failed after $MAX_RETRIES attempts."
              exit 1
            fi

            sleep 2                                              # 等待2秒后重试
          done

          echo "Push outputs succeeded."                         # 推送成功提示
"""

print("🧹 清理旧的 workflow 文件...")

# 遍历 workflow 目录下的文件，删除旧的以 scan_ 开头的 yml 文件
for f in os.listdir(WORKFLOW_DIR):
    if re.match(r"scan_.+\.yml", f):       # 匹配文件名以 scan_ 开头，后缀为 .yml
        os.remove(os.path.join(WORKFLOW_DIR, f))   # 删除匹配的文件

# 确认 chunk 目录存在，否则抛出异常
if not os.path.exists(CHUNK_DIR):
    raise RuntimeError(f"❌ CHUNK_DIR 不存在：{CHUNK_DIR}")

# 获取 chunk 目录下所有符合格式 chunk-数字.csv 的文件，排序后存入列表
chunks = sorted([
    f for f in os.listdir(CHUNK_DIR)
    if re.match(r"chunk-\d+\.csv", f)      # 严格匹配 chunk-数字.csv 格式文件
])

# 如果没有找到任何 chunk 文件，则抛出异常提示
if not chunks:
    raise RuntimeError(f"❌ 未找到任何 chunk CSV 文件，请检查目录：{CHUNK_DIR}")

print(f"📦 找到 {len(chunks)} 个 chunk 文件")

# 遍历所有 chunk 文件，为每个生成对应的 workflow 文件
for chunk_file in chunks:
    chunk_id = os.path.splitext(chunk_file)[0]  # 去除扩展名，得到如 chunk-1 的字符串

    workflow_filename = f"scan_{chunk_id}.yml" # 生成 workflow 文件名，如 scan_chunk-1.yml
    workflow_path = os.path.join(WORKFLOW_DIR, workflow_filename) # 生成 workflow 文件完整路径

    with open(workflow_path, "w", encoding="utf-8") as f:   # 打开文件写入
        f.write(TEMPLATE.format(n=chunk_id))                # 按模板格式化写入内容，替换占位符 {n}

    print(f"✅ 已生成 workflow: {workflow_filename}")       # 输出生成成功的文件名提示

print("\n🌀 生成 workflow 完成。请提交并推送。")              # 输出最终完成提示，提醒提交代码
