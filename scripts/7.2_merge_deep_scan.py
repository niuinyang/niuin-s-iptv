#!/usr/bin/env python3
import os
import csv
import argparse

def merge_csv_dir(input_dir, output_file):
    csv_files = sorted(
        f for f in os.listdir(input_dir)
        if f.endswith(".csv")
    )

    if not csv_files:
        print(f"⚠️ 目录为空，跳过: {input_dir}")
        return

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    header_written = False
    total_rows = 0

    with open(output_file, "w", newline="", encoding="utf-8") as fout:
        writer = None

        for fname in csv_files:
            fpath = os.path.join(input_dir, fname)
            with open(fpath, "r", encoding="utf-8") as fin:
                reader = csv.reader(fin)
                try:
                    header = next(reader)
                except StopIteration:
                    continue

                if not header_written:
                    writer = csv.writer(fout)
                    writer.writerow(header)
                    header_written = True

                for row in reader:
                    if row:
                        writer.writerow(row)
                        total_rows += 1

    print(f"✅ 合并完成: {output_file}（{total_rows} 行数据）")


def main(args):
    merge_csv_dir(
        args.ok_dir,
        args.output_ok
    )
    merge_csv_dir(
        args.not_dir,
        args.output_not
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="合并 deep scan 的 ok / not CSV 文件"
    )

    parser.add_argument(
        "--ok-dir",
        default="output/middle/deep/ok",
        help="deep ok CSV 目录"
    )
    parser.add_argument(
        "--not-dir",
        default="output/middle/deep/not",
        help="deep not CSV 目录"
    )
    parser.add_argument(
        "--output-ok",
        default="output/middle/deep/deep_total_ok.csv",
        help="合并后的 ok CSV"
    )
    parser.add_argument(
        "--output-not",
        default="output/middle/deep/deep_total_not.csv",
        help="合并后的 not CSV"
    )

    args = parser.parse_args()
    main(args)
