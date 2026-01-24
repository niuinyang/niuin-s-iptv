import pandas as pd
import os

# 输入输出路径
input_file = 'output/middle/standardize/standardize.csv'
output_file = 'output/middle/finalized/finalized.csv'

# 来源文件映射字典
source_file_map = {
    "1sddxzb.m3u": "济南电信组播",
    "2sddxdb.m3u": "济南电信单播",
    "3jnltzb.m3u": "济南联通组播",
    "4sdqdlt.m3u": "青岛联通单播",
    "5sdyd_ipv6.m3u": "山东移动单播",
    "6shyd_ipv6.m3u": "上海移动单播",
}

# 最终列顺序（严格按照你的最新要求）
final_columns = [
    "频道名",
    "是否匹配标准名",
    "原始名",
    "地址",
    "分组",
    "来源文件",
    "HTTP响应时间",
    "FB探测时间",
    "静态筛除原因",
    "轮播筛除原因",
    "首次出现时间",
    "原分组",
    "原国家分组",
    "原语言分组",
    "分辨率",
    "tvg-id",
    "tvg-name",
    "图标",
    "状态码",
    "FB是否成功",
    "视频编码",
    "帧率",
    "音频",
    "是否静态灰区",
    "静态灰区原因",
    "静态灰区建议动作",
    "最近动态值",
    "主phash值",
    "主phash总出现次数",
    "主phash占比",
    "TOP3_phash重复比例",
    "2s锚点一致率",
    "32s锚点一致率",
    "最近动态分",
    "历史动态稳定分",
    "内容集中度分",
    "锚点软加分",
    "静态评分",
    "当前置信度",
    "历史置信度"
]

def map_source_file(val):
    if pd.isna(val):
        return val
    val_str = str(val).strip()
    if val_str in source_file_map:
        return source_file_map[val_str]
    else:
        # 去除后缀
        base_name = os.path.splitext(val_str)[0]
        return f"网络文件：{base_name}"

def main():
    # 读取输入CSV，指定编码和保持原始字符串
    df = pd.read_csv(input_file, dtype=str)

    # 列名映射 —— 你之前的列名修改统一映射，方便处理
    col_rename_map = {
        "频道名": "频道名",
        "国家分组": "原国家分组",
        "语言分组": "原语言分组",
        "人工分组": "分组",
        "检测时间": "HTTP响应时间",
        "ffprobe是否成功": "FB是否成功",
        "ffprobe探测时间": "FB探测时间",
        "筛除原因": "静态筛除原因",
        "是否灰区": "是否静态灰区",
        "灰区原因": "静态灰区原因",
        "灰区建议动作": "静态灰区建议动作",
        "锚点一致率-2秒（阈值 ≥ 95%）": "2s锚点一致率",
        "锚点一致率-32秒（阈值 ≥ 95%）": "32s锚点一致率",
        "前三主phash重复比例（阈值 ≥ 70%）": "TOP3_phash重复比例",
        "动态值（阈值 ≤ 10）": "最近动态值",
        "最近一次动态分": "最近动态分",
        "轮播_筛除原因": "轮播筛除原因",
        "频道标准名": "频道名",
    }

    df.rename(columns=col_rename_map, inplace=True)

    # 来源文件列内容映射处理
    if "来源文件" in df.columns:
        df["来源文件"] = df["来源文件"].apply(map_source_file)
    else:
        # 如果缺失，创建空列
        df["来源文件"] = ""

    # 确保所有final_columns存在，不存在列补空
    for col in final_columns:
        if col not in df.columns:
            df[col] = ""

    # 按最终列顺序输出
    df_final = df[final_columns]
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # 输出CSV，不写索引，utf-8编码
    df_final.to_csv(output_file, index=False, encoding="utf-8")

if __name__ == "__main__":
    main()
