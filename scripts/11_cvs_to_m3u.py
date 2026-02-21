import csv
import os
import re

# 配置区（根据需求可修改）

# 分组配置，顺序决定输出顺序，value=True/False控制是否输出
# 这两个配置针对dxl和sjmz均需独立配置
GROUP_CONFIG_DXL = {
    '央视频道': True,
    '4K频道': True,
    '卫视频道': True,
    '香港频道': True,
    '澳门频道': True,
    '台湾频道': True,
    '国际频道': True,
    '山东频道': True,
    '央视新媒': True,
    '数字频道': True,
    '待匹配未分组': True,
    # 其他分组默认不输出
}

GROUP_CONFIG_SJMZ = {
    '央视频道': True,
    '4K频道': True,
    '卫视频道': True,
    '香港频道': True,
    '澳门频道': True,
    '台湾频道': True,
    '国际频道': True,
    '山东频道': True,
    '央视新媒': True,
    '数字频道': True,
    '待匹配未分组': True,
}

# 自有源列表和是否启用及排序顺序（dxl）
OWN_SOURCE_ENABLED_DXL = {
    '济南联通单播': True,
    '济南电信单播': True,
    '济南电信组播': True,
    '青岛联通单播': False,
    '山东移动单播': False,
    '上海移动单播': False,
}
OWN_SOURCE_PRIORITY_DXL = {
    '济南联通单播': 0,
    '济南电信单播': 1,
    '济南电信组播': 2,
    '青岛联通单播': 3,
    '山东移动单播': 4,
    '上海移动单播': 5,
}

# 自有源列表和是否启用及排序顺序（sjmz）
OWN_SOURCE_ENABLED_SJMZ = {
    '济南联通单播': False,
    '山东移动单播': False,
    '济南电信单播': False,
    '济南电信组播': True,
    '青岛联通单播': False,
    '上海移动单播': False,
}
OWN_SOURCE_PRIORITY_SJMZ = {
    '山东移动单播': 0,
    '济南电信单播': 1,
    '济南电信组播': 2,
    '青岛联通单播': 3,
    '济南联通单播': 4,
    '上海移动单播': 5,
}

# 分辨率分类函数
def resolution_class(w, h):
    try:
        w, h = int(w), int(h)
    except:
        return "LD"
    if w >= 1280 or h >= 720:
        return "HD"
    elif w >= 640 and h >= 360:
        return "SD"
    else:
        return "LD"

# 频道名排序辅助函数
def channel_name_sort_key(name):
    # 识别CCTV系列，按数字排序
    m = re.match(r'(CCTV)(\d+)', name, re.IGNORECASE)
    if m:
        prefix, num = m.group(1).upper(), int(m.group(2))
        return (0, prefix, num)
    # 不是CCTV系列，判断首字符是中文还是英文
    first_char = name[0]
    if '\u4e00' <= first_char <= '\u9fff':  # 中文范围
        import pypinyin
        py = pypinyin.lazy_pinyin(first_char)
        first_letter = py[0][0] if py else first_char
        return (1, first_letter.lower(), name)
    else:
        return (2, first_char.lower(), name)

# 频道名拼音排序依赖包检测
try:
    import pypinyin
except ImportError:
    print("提示：请安装pypinyin模块，用于中文频道名排序。执行命令：pip install pypinyin")
    exit(1)

# === 修改点：修改判定函数，网络源 = 非自有源 ===
# 判断是否为自有源
def is_own_source(row, own_source_enabled):
    source = row.get('来源文件', '')
    return own_source_enabled.get(source, False)

# 判断是否为网络源，改为非自有源即网络源
def is_network_source(row, own_source_enabled):
    return not is_own_source(row, own_source_enabled)
# === 修改点结束 ===

# 判断是否输出条件满足
def can_output(row):
    s_reason = (row.get('静态筛除原因') or '').strip()
    l_reason = (row.get('轮播筛除原因') or '').strip()
    audio = (row.get('音频') or '').strip()
    if s_reason not in ('未筛除', ''):
        return False
    if l_reason not in ('未筛除', ''):
        return False
    if audio not in ('有音频', ''):
        return False
    return True

# 解析分辨率，返回宽高整数
def parse_resolution(res):
    # 支持格式如 "1920x1080" 或 "1280×720" 等
    if not res:
        return 0, 0
    res = res.lower().replace('×', 'x')
    parts = res.split('x')
    if len(parts) == 2:
        try:
            w = int(parts[0])
            h = int(parts[1])
            return w, h
        except:
            return 0, 0
    return 0, 0

# 转换为M3U条目
def make_m3u_item(row):
    name = row.get('标准名', '').strip()
    group = row.get('分组', '').strip()
    logo = row.get('图标', '').strip()
    url = row.get('地址', '').strip()

    # 基础 EXTINF 属性
    extinf_attrs = []
    if logo:
        extinf_attrs.append(f'tvg-logo="{logo}"')
    if group:
        extinf_attrs.append(f'group-title="{group}"')

    # === 回播参数（新增） ===
    catchup = (row.get('回播类型') or '').strip()
    catchup_days = (row.get('回播天数') or '').strip()
    catchup_source = (row.get('回播地址') or '').strip()

    if catchup_source:
        # 有回播才写
        if catchup:
            extinf_attrs.append(f'catchup="{catchup}"')
        if catchup_days:
            extinf_attrs.append(f'catchup-days="{catchup_days}"')
        extinf_attrs.append(f'catchup-source="{catchup_source}"')
    # === 回播参数结束 ===

    extinf_str = " ".join(extinf_attrs)
    extinf_line = f'#EXTINF:-1 {extinf_str},{name}'

    return f"{extinf_line}\n{url}"

# 读取CSV数据
def read_csv(file_path):
    with open(file_path, encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)

# 排序网络源函数
def sort_network_sources(sources):
    # FB探测时间，分辨率分类，分辨率高低排序等
    def fb_val(row):
        try:
            return float(row.get('FB探测时间', ''))
        except:
            return None
    def curr_conf(row):
        try:
            return float(row.get('当前置信度', '0'))
        except:
            return 0
    def hist_conf(row):
        try:
            return float(row.get('历史置信度', '0'))
        except:
            return 0

    def sort_key(row):
        fb = fb_val(row)
        w, h = parse_resolution(row.get('分辨率', ''))
        res_cls = resolution_class(w, h)

        # 先判定排序大类
        # 分区划分顺序：
        # 1 FB<=3 HD
        # 2 FB<=3 SD
        # 3 3<FB<10 HD
        # 4 3<FB<10 SD
        # 5 FB<=3 LD
        # 6 其他（无视分辨率）
        if fb is None:
            big_cls = 6
        elif fb <= 3:
            if res_cls == "HD":
                big_cls = 1
            elif res_cls == "SD":
                big_cls = 2
            elif res_cls == "LD":
                big_cls = 5
            else:
                big_cls = 6
        elif 3 < fb < 10:
            if res_cls == "HD":
                big_cls = 3
            elif res_cls == "SD":
                big_cls = 4
            else:
                big_cls = 6
        else:
            big_cls = 6

        # 内部排序：
        # 分辨率从高到低，这里用宽*高作为分辨率大小参考
        res_val = w * h
        # 当前置信度降序，历史置信度降序
        curr_c = curr_conf(row)
        hist_c = hist_conf(row)
        # FB探测时间升序，作为辅助排序
        fb_sort = fb if fb is not None else 9999

        return (
            big_cls,
            -res_val,
            fb_sort,
            -curr_c,
            -hist_c
        )

    sources.sort(key=sort_key)
    return sources

# 排序自有源函数
def sort_own_sources(sources, own_source_priority):
    # 先按自有源顺序优先级排序，再按分辨率从高到低排序
    def priority(row):
        src = row.get('来源文件', '')
        return own_source_priority.get(src, 9999)

    def resolution_val(row):
        w, h = parse_resolution(row.get('分辨率', ''))
        return w * h

    sources.sort(key=lambda r: (priority(r), -resolution_val(r)))
    return sources

# 频道名排序函数
def sort_channels(channels):
    # channels是dict key=频道名, value=list of sources
    # 先对频道名排序
    names = list(channels.keys())
    names.sort(key=channel_name_sort_key)
    return names

# 主处理函数
def process(file_path, output_path, own_source_enabled, own_source_priority, group_config):
    data = read_csv(file_path)
    # 过滤符合输出条件的源
    data = [row for row in data if can_output(row)]
    # 过滤出启用的分组数据
    data = [row for row in data if group_config.get(row.get('分组',''), False)]

    # 按分组排序（依据配置顺序）
    def group_order(row):
        return list(group_config.keys()).index(row.get('分组','')) if row.get('分组','') in group_config else 9999
    data.sort(key=group_order)

    # 按分组聚合
    from collections import defaultdict, OrderedDict
    grouped = defaultdict(list)
    for row in data:
        grouped[row['分组']].append(row)

    # 按分组顺序处理
    sorted_groups = [g for g in group_config.keys() if group_config[g]]
    output_lines = ['#EXTM3U']
    for group in sorted_groups:
        if group not in grouped:
            continue
        group_rows = grouped[group]
        # 频道名分组
        channel_dict = defaultdict(list)
        for row in group_rows:
            channel_dict[row['标准名']].append(row)
        # 频道名排序
        sorted_channels = sort_channels(channel_dict)

        for ch_name in sorted_channels:
            sources = channel_dict[ch_name]
            # === 修改点：判定自有源和网络源时传入own_source_enabled ===
            own_sources = [s for s in sources if is_own_source(s, own_source_enabled)]
            network_sources = [s for s in sources if is_network_source(s, own_source_enabled)]

            # 筛掉不启用的自有源（这里冗余，is_own_source已经判断了）
            own_sources = [s for s in own_sources if own_source_enabled.get(s.get('来源文件',''), False)]

            # 排序自有源
            own_sources = sort_own_sources(own_sources, own_source_priority)
            # 排序网络源
            network_sources = sort_network_sources(network_sources)

            # 先自有源后网络源
            final_sources = own_sources + network_sources

            # 输出
            for src in final_sources:
                m3u_line = make_m3u_item(src)
                output_lines.append(m3u_line)
    # === 修改点结束 ===

    # 写文件
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))
    print(f"生成文件: {output_path}")

# 主入口
def main():
    input_csv = 'output/middle/finalized/finalized.csv'  # 请修改为你的实际输入CSV路径

    process(
        file_path=input_csv,
        output_path='output/final/final_dxl.m3u',
        own_source_enabled=OWN_SOURCE_ENABLED_DXL,
        own_source_priority=OWN_SOURCE_PRIORITY_DXL,
        group_config=GROUP_CONFIG_DXL
    )
    process(
        file_path=input_csv,
        output_path='output/final/final_sjmz.m3u',
        own_source_enabled=OWN_SOURCE_ENABLED_SJMZ,
        own_source_priority=OWN_SOURCE_PRIORITY_SJMZ,
        group_config=GROUP_CONFIG_SJMZ
    )

if __name__ == '__main__':
    main()
