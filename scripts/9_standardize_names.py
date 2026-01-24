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
    name = re.sub(r'（.*?）|\(.*?\)|【.*?】|\[.*?]', '', name)
    name = cc.convert(name)
    name = re.sub(r'[^0-9a-zA-Z\u4e00-\u9fa5+！]', '', name)
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

    # 4. 标准化 key
    df_total['std_key'] = df_total['频道名'].map(mechanical_standardize)
    df_channel['std_key'] = df_channel['原始名'].map(mechanical_standardize)

    # === loop_scan 也生成 std_key（用于反查原分组信息）===
    df_loopscan['std_key'] = df_loopscan['频道名'].map(mechanical_standardize)

    # === 新增：对齐 loop_scan 字段名（国家分组 / 语言分组 → 原国家分组 / 原语言分组）===
    if '国家分组' in df_loopscan.columns:
        df_loopscan['原国家分组'] = df_loopscan['国家分组']
    else:
        df_loopscan['原国家分组'] = ''

    if '语言分组' in df_loopscan.columns:
        df_loopscan['原语言分组'] = df_loopscan['语言分组']
    else:
        df_loopscan['原语言分组'] = ''

    if '原分组' not in df_loopscan.columns:
        df_loopscan['原分组'] = ''

    # === 构建 std_key -> 原分组信息 映射（来自 loop_scan）===
    loop_group_map = (
        df_loopscan
        .drop_duplicates(subset=['std_key'], keep='first')
        .set_index('std_key')[['原分组', '原国家分组', '原语言分组']]
        .to_dict('index')
    )

    # 5. 仅保留标准库中是否已维护==是的记录用于匹配
    df_channel_match = df_channel[df_channel['是否已维护'].str.strip() == '是'].copy()
    df_channel_match = df_channel_match.drop_duplicates(subset=['std_key'], keep='first')

    # 6. 构建标准库映射
    channel_map = df_channel_match.set_index('std_key')[['标准名', '分组']].to_dict('index')

    # 7. 匹配过程
    def match_row(std_key, original_name):
        if std_key in channel_map:
            item = channel_map[std_key]
            return pd.Series(['是', item['标准名'], item['分组']])
        else:
            return pd.Series(['否', original_name, '待匹配未分组'])

    df_total[['是否匹配标准名', '频道标准名', '人工分组']] = df_total.apply(
        lambda row: match_row(row['std_key'], row['频道名']), axis=1
    )

    # 8. 统计新增未匹配频道
    matched_std_keys = set(df_channel_match['std_key'])
    unmatched = df_total[~df_total['std_key'].isin(matched_std_keys)]
    df_new = unmatched[['频道名', 'std_key']].drop_duplicates(subset=['std_key'])

    # 9. 新增未维护标准库行
    df_new_channel_rows = pd.DataFrame({
        '原始名': df_new['频道名'],
        '标准名': [''] * len(df_new),
        '分组': [''] * len(df_new),
        '是否已维护': ['否'] * len(df_new),
        'std_key': df_new['std_key']
    })

    # 10. 已维护标准库
    df_channel_yes = df_channel[df_channel['是否已维护'].str.strip() == '是'].copy()
    df_channel_yes = df_channel_yes.sort_values(by=['std_key']).drop_duplicates(subset=['std_key'], keep='first')

    # 11. 匹配次数
    match_counts = df_total[df_total['是否匹配标准名'] == '是'].groupby('频道标准名').size().to_dict()

    df_channel_yes['本次匹配次数'] = df_channel_yes['标准名'].map(lambda x: match_counts.get(x, 0))
    df_new_channel_rows['本次匹配次数'] = 0

    # === 从 loop_scan 回填 原分组 / 国家 / 语言 ===
    def fill_origin_groups(std_key):
        info = loop_group_map.get(std_key, {})
        return pd.Series([
            info.get('原分组', ''),
            info.get('原国家分组', ''),
            info.get('原语言分组', '')
        ])

    df_channel_yes[['原分组', '原国家分组', '原语言分组']] = (
        df_channel_yes['std_key'].apply(fill_origin_groups)
    )

    df_new_channel_rows[['原分组', '原国家分组', '原语言分组']] = (
        df_new_channel_rows['std_key'].apply(fill_origin_groups)
    )

    # 12. 合并写回标准库
    df_channel_out = pd.concat([df_channel_yes, df_new_channel_rows], ignore_index=True, sort=False)

    df_channel_out = df_channel_out[
        ['原始名', '标准名', '分组', '是否已维护', '本次匹配次数', '原分组', '原国家分组', '原语言分组']
    ]

    df_channel_out.to_csv(PATH_CHANNEL_DATA, index=False, encoding='utf-8-sig')

    # 13. 输出标准化结果
    os.makedirs(os.path.dirname(PATH_OUTPUT_STANDARDIZE), exist_ok=True)

    cols_output = list(df_total.columns.drop(['std_key']))
    additional_cols = ['是否匹配标准名', '频道标准名', '人工分组']
    final_cols = cols_output + [c for c in additional_cols if c not in cols_output]

    df_total.to_csv(PATH_OUTPUT_STANDARDIZE, columns=final_cols, index=False, encoding='utf-8-sig')

    print("标准化处理完成：")
    print(f" - 标准化结果文件: {PATH_OUTPUT_STANDARDIZE}")
    print(f" - 标准库更新文件: {PATH_CHANNEL_DATA}")

if __name__ == '__main__':
    main()
