import os
import glob
import pandas as pd

def merge_csv_files(input_folder, output_file):
    # 找到所有csv文件
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {input_folder}")
        return

    df_list = []
    for file in csv_files:
        df = pd.read_csv(file)

        # 统一字段名（如果存在 name 则改成 频道名）
        if "name" in df.columns and "频道名" not in df.columns:
            df = df.rename(columns={"name": "频道名"})

        df_list.append(df)

    merged_df = pd.concat(df_list, ignore_index=True)

    # 按“地址”列去重
    if "地址" in merged_df.columns:
        before = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=["地址"])
        after = len(merged_df)
        print(f"Deduplicated by 地址: {before} → {after}")
    else:
        print("Warning: Column '地址' not found, skip deduplication.")

    # 保存合并 CSV
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    merged_df.to_csv(output_file, index=False)
    print(f"Merged CSV saved to {output_file}")

    # 导出 M3U 文件
    generate_m3u(merged_df, output_file.replace(".csv", ".m3u"))


def generate_m3u(df, m3u_path):
    """
    基于 DataFrame 自动生成 M3U 文件
    使用字段：
    - 频道名：频道名称
    - 地址：播放源 URL
    """

    required = {"频道名", "地址"}
    if not required.issubset(df.columns):
        print(f"Cannot generate M3U — missing columns: {required - set(df.columns)}")
        return

    lines = ["#EXTM3U"]

    for _, row in df.iterrows():
        name = str(row["频道名"])
        url = str(row["地址"])

        lines.append(f"#EXTINF:-1,{name}")
        lines.append(url)

    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"M3U exported to {m3u_path}")


if __name__ == "__main__":
    merge_csv_files("output/middle/final/ok", "output/middle/working.csv")
