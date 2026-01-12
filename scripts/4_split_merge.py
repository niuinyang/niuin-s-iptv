#!/usr/bin/env python3
import csv                        # 导入csv模块，用于读取和写入CSV文件
import os                         # 导入os模块，用于文件和路径操作
import sys                        # 导入sys模块，用于系统相关操作（如退出程序）

# === 自动定位仓库根目录 ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))   # 获取当前脚本文件所在目录的绝对路径（scripts目录）
REPO_ROOT = os.path.dirname(SCRIPT_DIR)                   # 获取脚本目录的上一级目录，即仓库根目录

def split_deep_scan(
        input_path=os.path.join(REPO_ROOT, "output/middle/merge/networksource_total.csv"),  # 输入CSV文件路径，默认值
        chunk_size=1000,                    # 每个分片文件包含的最大行数，默认1000
        output_dir=os.path.join(REPO_ROOT, "output/middle/chunk")  # 输出分片文件目录，默认路径
    ):
    """
    读取 CSV，将其按指定大小分割成多个分片文件 chunk-N.csv。
    删除旧分片文件，路径基于仓库根目录，避免 GitHub Actions 路径错乱。
    """

    print("=== 路径检查 ===")
    print("脚本目录 SCRIPT_DIR:", SCRIPT_DIR)                         # 输出脚本目录路径，方便调试
    print("仓库根目录 REPO_ROOT:", REPO_ROOT)                         # 输出仓库根目录路径
    print("当前工作目录 os.getcwd():", os.getcwd())                    # 输出当前运行时的工作目录
    print("输入文件绝对路径:", os.path.abspath(input_path))           # 输出输入文件的绝对路径
    print("chunk 输出目录绝对路径:", os.path.abspath(output_dir))      # 输出输出目录的绝对路径

    # 检查输入文件是否存在
    if not os.path.exists(input_path):                                # 如果输入文件不存在
        print(f"错误：输入文件不存在 - {input_path}")                # 输出错误信息
        sys.exit(1)                                                   # 退出程序，返回错误码1

    # 创建输出目录（如果不存在）
    os.makedirs(output_dir, exist_ok=True)                            # 创建输出目录，exist_ok=True表示目录已存在不报错

    # === 读取 CSV ===
    print("\n=== 读取 CSV 文件 ===")
    try:
        with open(input_path, newline='', encoding="utf-8") as f:    # 以utf-8编码打开输入CSV文件
            reader = csv.DictReader(f)                              # 使用DictReader按行读取，返回字典格式
            headers = reader.fieldnames                             # 记录CSV的列名字段
            rows = list(reader)                                     # 将所有行转换成列表，方便后续处理
    except UnicodeDecodeError:                                      # 如果utf-8解码失败，捕获异常
        print("UTF-8 解码失败，尝试自动检测编码...")                 # 输出提示信息
        import chardet                                               # 导入chardet库，用于自动检测文件编码
        with open(input_path, "rb") as f:                           # 以二进制方式读取文件
            data = f.read()                                          # 读取全部数据
            detected = chardet.detect(data)                          # 自动检测编码
            encoding = detected.get("encoding", "utf-8")             # 获取检测到的编码，默认utf-8

        print(f"检测到编码: {encoding}")                             # 输出检测到的编码

        text = data.decode(encoding, errors="ignore")                # 按检测编码解码文件内容，忽略错误
        rows = list(csv.DictReader(text.splitlines()))               # 用解码后的文本按行读取CSV数据
        headers = rows[0].keys() if rows else []                      # 获取表头（字段名）

    total_rows = len(rows)                                           # 计算读取的总行数
    print(f"读取行数: {total_rows}")                                  # 输出行数

    # === 拆分 CSV ===
    total_chunks = (total_rows + chunk_size - 1) // chunk_size       # 计算需要生成多少个chunk文件（向上取整）
    print(f"预计生成 {total_chunks} 个分片文件")                      # 输出预计分片数量

    for start in range(0, total_rows, chunk_size):                    # 按chunk_size步长循环分片起始位置
        chunk_rows = rows[start:start + chunk_size]                   # 取出当前分片的所有行数据
        chunk_index = start // chunk_size + 1                         # 计算当前分片序号（1起始）
        chunk_name = f"chunk-{chunk_index}.csv"                       # 生成分片文件名
        chunk_path = os.path.join(output_dir, chunk_name)             # 拼接分片文件完整路径

        with open(chunk_path, "w", newline='', encoding="utf-8") as cf:  # 以utf-8编码写入分片文件
            writer = csv.DictWriter(cf, fieldnames=headers)             # 创建DictWriter对象，指定字段名
            writer.writeheader()                                         # 写入CSV表头
            writer.writerows(chunk_rows)                                 # 写入当前分片的所有行数据

        print(f"✔ 已生成: {chunk_path}（行数 {len(chunk_rows)}）")        # 输出生成的文件名和行数

    print("\n🎉 所有分片文件已完成")                                   # 结束提示

if __name__ == "__main__":
    split_deep_scan()   # 脚本直接执行时调用分割函数，使用默认参数
