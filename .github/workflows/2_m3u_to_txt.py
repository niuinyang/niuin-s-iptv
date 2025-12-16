name: 2_M3U文件转TXT文件

on:
  workflow_dispatch:
  schedule:
    - cron: '10 16 * * *'  # UTC 16:10 = 北京时间 00:10

permissions:
  contents: write

jobs:
  parse_m3u_txt:
    name: 解析并转换频道文件
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          persist-credentials: true

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Cache pip
        uses: actions/cache@v3
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt') }}
          restore-keys: |
            ${{ runner.os }}-pip-

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Ensure input/output directories exist
        run: |
          mkdir -p input/download/net/original
          mkdir -p input/download/net/txt

      - name: Run M3U/TXT parse script
        run: python scripts/parse_m3u_to_txt.py

      - name: Fix permissions before commit
        run: chmod -R u+rw input/download/net/txt

      - name: Commit & Push results
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add input/download/net/txt

          if git diff --cached --quiet; then
            echo "✅ No changes to commit."
            exit 0
          fi

          git commit -m "更新频道文件解析输出 [ci skip]"

          git pull --rebase origin main
          git push origin main
