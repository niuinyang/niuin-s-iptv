# 9_standardize_names.py
import os
import pandas as pd
import re
from opencc import OpenCC

# 配置路径
PATH_MYSOURCE = 'output/middle/merge/mysource_total.csv'
PATH_LOOPSCAN = 'output/middle/loop/loop_scan_total.csv'
PATH_CHANNEL_DATA = 'output/middle/channel_data/channel_data.csv'
PATH_OUTPUT_STANDARDIZE = 'output/middle/standardize/standardize.csv'

# 初始化繁体转简体转换器
cc = OpenCC('t2s')

def safe_read_csv(path):
    """安全读取csv，返回空DataFrame时处理"""
    if os.path.exists(path):
        try:
            return pd.read_csv(path, dtype=str, encoding='utf-8').fillna('')
        except Exception as e:
            print(f"读取文件 {path} 失败: {e}")
            return pd.DataFrame()
    else:
        print(f"文件不存在: {path}")
        return pd.DataFrame()

def mechanical_standardize(name):
    """机械式频道名标准化"""
    if not isinstance(name, str):
        return ''
    # 去除中英文括号及内容
    name = re.sub(r'（.*?）|\(.*?\)|【.*?】|\[.*?]', '', name)
    # 转繁体到简体
    name = cc.convert(name)
    # 去除空格和特殊字符（保留汉字、数字、字母、+、！）
    # 汉字 Unicode \u4e00-\u9fa5，数字0-9，字母a-zA-Z，符号+和！
    name = re.sub(r'[^0-9a-zA-Z\u4e00-\u9fa5+！]', '', name)
    # 转小写
    name = name.lower()
    return name

def unify_column_name(df, old_name, new_name):
    if old_name in df.columns:
        df.rename(columns={old_name: new_name}, inplace=True)

def main():
    # 1. 读取文件
    df_mysource = safe_read_csv(PATH_MYSOURCE)
    df_loopscan = safe_read_csv(PATH_LOOPSCAN)
    df_channel = safe_read_csv(PATH_CHANNEL_DATA)

    # 2. 统一标准库“来源”字段名为“来源文件”
    unify_column_name(df_channel, '来源', '来源文件')

    # 3. 合并自有源和网络源，缺失字段补空
    df_total = pd.concat([df_mysource, df_loopscan], ignore_index=True, sort=False).fillna('')

    # 4. 对合并数据和标准库的匹配字段“频道名”和“原始名”做标准化，生成std_key
    df_total['std_key'] = df_total['频道名'].map(mechanical_standardize)
    df_channel['std_key'] = df_channel['原始名'].map(mechanical_standardize)

    # 5. 仅保留标准库中是否已维护==是的记录用于匹配
    df_channel_match = df_channel[df_channel['是否已维护'].str.strip() == '是'].copy()

    # === 修改点 1: 对已维护标准库数据机械标准化后的重复 std_key 去重，保留第一个 ===
    df_channel_match = df_channel_match.drop_duplicates(subset=['std_key'], keep='first')

    # 6. 构建标准库std_key到字段映射的字典
    # === 修改点 2: 不再包含'来源文件'字段 ===
    channel_map = df_channel_match.set_index('std_key')[['标准名', '分组']].to_dict('index')

    # 7. 匹配过程
    def match_row(std_key, original_name):
        if std_key in channel_map:
            item = channel_map[std_key]
            return pd.Series([
                '是',              # 是否匹配
                item['标准名'],     # 频道标准名
                item['分组']       # 人工分组
                # 不返回匹配名来源，去除该列
            ])
        else:
            return pd.Series([
                '否',
                original_name,
                '待匹配未分组'
            ])

    # === 修改点 3: 应用匹配只返回3列，不再包含匹配名来源列 ===
    df_total[['是否匹配标准名', '频道标准名', '人工分组']] = df_total.apply(
        lambda row: match_row(row['std_key'], row['频道名']), axis=1
    )

    # 8. 统计新增未匹配频道
    matched_std_keys = set(df_channel_match['std_key'])
    unmatched = df_total[~df_total['std_key'].isin(matched_std_keys)]

    # 去重未匹配频道，准备新增行
    df_new = unmatched[['频道名', 'std_key']].drop_duplicates(subset=['std_key'])

    # 新增行字段准备
    df_new_channel_rows = pd.DataFrame({
        '原始名': df_new['频道名'],
        '标准名': [''] * len(df_new),
        '分组': [''] * len(df_new),
        # === 修改点 4: 不包含来源文件列 ===
        '是否已维护': ['否'] * len(df_new)
    })

    # 9. 标准库中旧已维护记录（去重后）用于写回
    df_channel_yes = df_channel[df_channel['是否已维护'].str.strip() == '是'].copy()
    # === 修改点 5: 同样去重已维护的旧库数据，避免重复 ===
    df_channel_yes = df_channel_yes.drop_duplicates(subset=['std_key'], keep='first')

    # 10. 本次匹配次数统计（以标准名计数）
    # 只统计匹配成功的行
    match_counts = df_total[df_total['是否匹配标准名'] == '是'].groupby('频道标准名').size()
    match_counts_dict = match_counts.to_dict()

    # 给标准库赋匹配次数，未匹配默认0
    def get_match_count(row):
        return match_counts_dict.get(row['标准名'], 0)

    df_channel_yes['本次匹配次数'] = df_channel_yes.apply(get_match_count, axis=1)

    # 新增未匹配记录匹配次数设0
    df_new_channel_rows['本次匹配次数'] = 0

    # 11. 合并写回标准库
    df_channel_out = pd.concat([df_channel_yes, df_new_channel_rows], ignore_index=True, sort=False)

    # 12. 写标准库文件（去除来源文件列，只写5列）
    df_channel_out = df_channel_out[
        ['原始名', '标准名', '分组', '是否已维护', '本次匹配次数']
    ]

    df_channel_out.to_csv(PATH_CHANNEL_DATA, index=False, encoding='utf-8-sig')

    # 13. 写标准化结果文件，包含合并文件所有列 + 3列匹配结果
    # === 修改点 6: 不包含匹配名来源列 ===
    
    # === 新增修复点：确保输出目录存在（GitHub Actions 必须） ===
    os.makedirs(os.path.dirname(PATH_OUTPUT_STANDARDIZE), exist_ok=True)
    
    cols_output = list(df_total.columns.drop(['std_key']))  # 去除 std_key
    additional_cols = ['是否匹配标准名', '频道标准名', '人工分组']
    final_cols = cols_output + [c for c in additional_cols if c not in cols_output]

    df_total.to_csv(PATH_OUTPUT_STANDARDIZE, columns=final_cols, index=False, encoding='utf-8-sig')

    print("标准化处理完成，结果已写入：")
    print(f" - 标准化结果文件: {PATH_OUTPUT_STANDARDIZE}")
    print(f" - 标准库更新文件: {PATH_CHANNEL_DATA}")

if __name__ == '__main__':
    main()
