#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
⚠️ 已退役为历史参考，不再作为任何链路的数据源（v10 / 2026-11-07）。

现在 Kafka `music_events` 的唯一造数器是 `code/gen_events.py`：
实时 Flink 与离线 Flume 消费同一 topic 里的同一批消息（docs/17 §3.2）。
本文件保留是为了复现 docs/05 / docs/14 的历史压测与脏数据归因实验，
**不要再跑 --mode stream 往 music_events 写数据**，否则会重新制造两套数据源。

音乐 APP 埋点数据生成器（公共属性 + 事件属性 两层结构）
========================================================
对应 docs/04-埋点设计与存储方案.md。

两种模式
--------
  batch   批量生成文件（可复现，用于离线数仓、压力测试、重灌历史）
          python code/gen_music_data.py --mode batch --events 50000

  stream  持续生成并发送到 Kafka（用于实时链路验证与演示）
          python code/gen_music_data.py --mode stream --rate 10 --total 20000

设计要点
--------
1. 公共属性作顶层字段，事件属性放 properties 对象，与主流埋点平台一致。
2. 事件时间用「当前时间」，保证实时链路的时间戳与落库时间接近，延迟指标才有意义。
3. 注入 6 类脏数据，注入记录写 dirty_manifest*.csv，作为质检召回率的 ground truth。
4. 流式模式必须设 --total 上限，否则会一直写下去。

依赖：流式模式需要 kafka-python；批量模式仅标准库。
"""

import argparse
import csv
import hashlib
import json
import os
import random
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta
from itertools import accumulate

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------- 业务字典
APP_VERSIONS = [("8.0.0", 6), ("8.1.2", 14), ("8.2.0", 24), ("8.2.1", 28),
                ("8.3.0", 20), ("9.0.0-beta", 8)]
OS_TYPES = [("android", 52), ("ios", 41), ("web", 5), ("car", 2)]
DEVICE_MODELS = {
    "android": ["Xiaomi 14 Pro", "Xiaomi 14", "HUAWEI Mate 60 Pro", "HUAWEI P60",
                "OPPO Find X7", "vivo X100 Pro", "HONOR Magic6 Pro", "Samsung Galaxy S24",
                "Redmi K70 Pro", "OnePlus 12", "realme GT5 Pro", "iQOO 12"],
    "ios": ["iPhone 15 Pro Max", "iPhone 15 Pro", "iPhone 15", "iPhone 14 Pro",
            "iPhone 14", "iPhone 13", "iPhone 12", "iPad Air (第5代)"],
    "web": ["Chrome 120 / Windows", "Safari 17 / macOS", "Edge 120 / Windows"],
    "car": ["BYD DiLink 4.0", "Tesla V11", "蔚来 NOMI", "理想 Li OS"],
}
NETWORKS = [("wifi", 54), ("5g", 26), ("4g", 18), ("3g", 2)]
CARRIERS = ["", "中国移动", "中国联通", "中国电信", "中国广电"]
CHANNELS = [("推荐流", 30), ("歌单", 19), ("搜索", 13), ("电台", 10),
            ("收藏夹", 8), ("排行榜", 7), ("分享链接", 5), ("首页banner", 4),
            ("push推送", 4)]
PAGES = ["player", "playlist_detail", "search_result", "home", "radio",
         "artist_detail", "album_detail"]
# 省级覆盖 + 按网民规模加权（权重参考各省网民量级，非精确统计）
# 三元素：(省, 代表城市, 权重)。覆盖全部 34 个省级行政区。
CITIES = [
    ("广东", "深圳", 1100), ("广东", "广州", 900), ("山东", "济南", 700),
    ("河南", "郑州", 700), ("江苏", "南京", 650), ("四川", "成都", 650),
    ("浙江", "杭州", 550), ("河北", "石家庄", 550), ("湖南", "长沙", 500),
    ("湖北", "武汉", 450), ("安徽", "合肥", 450), ("广西", "南宁", 400),
    ("云南", "昆明", 350), ("江西", "南昌", 350), ("福建", "福州", 350),
    ("陕西", "西安", 350), ("辽宁", "沈阳", 350), ("贵州", "贵阳", 300),
    ("山西", "太原", 300), ("重庆", "重庆", 300), ("黑龙江", "哈尔滨", 300),
    ("吉林", "长春", 250), ("甘肃", "兰州", 250), ("内蒙古", "呼和浩特", 250),
    ("新疆", "乌鲁木齐", 250), ("上海", "上海", 250), ("北京", "北京", 250),
    ("天津", "天津", 200), ("海南", "海口", 150), ("宁夏", "银川", 100),
    ("青海", "西宁", 100), ("西藏", "拉萨", 50), ("台湾", "台北", 50),
    ("香港", "香港", 50), ("澳门", "澳门", 20),
]

# 预计算加权抽样用的两个平行列表，避免每次调用重复构造
CITY_PAIRS = [(p, c) for p, c, _ in CITIES]
CITY_WEIGHTS = [w for _, _, w in CITIES]
GENRES = [("流行", 26), ("摇滚", 12), ("民谣", 11), ("电子", 10), ("说唱", 9),
          ("R&B", 8), ("古风", 7), ("二次元", 6), ("爵士", 5), ("古典", 4)]

EVENT_TYPES = [("play_start", 27), ("play_end", 29), ("skip", 19),
               ("collect", 7), ("search", 8), ("share", 3), ("page_view", 4),
               # 商业化：订阅与购买。真实音乐 App 里订单量远小于播放量，
               # 千分之几的比例才能让「付费率」落在个位数百分比的合理区间，
               # 权重放大就会让大屏出现「付费率 40%」这种一眼假的数字。
               ("vip_subscribe", 0.8), ("order_pay", 1.2), ("order_refund", 0.06),
               # 客户端质量：播放失败（网络超时 / 解码失败 / 版权下架 / CDN 404）
               ("play_error", 1.0)]
END_REASONS = [("natural", 62), ("skip", 24), ("pause", 6), ("exit", 5), ("interrupt", 3)]
SEARCH_TYPES = ["联想", "手动", "语音"]
SHARE_CHANNELS = ["微信", "朋友圈", "微博", "QQ", "复制链接"]

# ---------------------------------------------------------------- 商业化目录
# 金额写死在 SKU 里，事件里带的就是这个值 —— 这样「收入 = Σ 已支付订单金额」
# 才可能对上账。如果每条订单随机金额，SQL 一加总就会和「单价 × 订单数」差出几倍，
# 答辩时被追问一句「你这收入怎么算的」就露馅。
SKUS = [
    # (sku_id, 名称, 单价元, 有效天数)
    ("vip_month",    "VIP连续包月", 15.00,  30),
    ("vip_quarter",  "VIP季卡",     40.00,  90),
    ("vip_year",     "VIP年卡",    148.00, 365),
    ("song_single",  "单曲购买",     3.00,   0),
    ("digital_album", "数字专辑",   25.00,   0),
]
VIP_SKUS = ["vip_month", "vip_quarter", "vip_year"]
PAY_CHANNELS = [("微信支付", 46), ("支付宝", 38), ("Apple Pay", 12), ("余额", 4)]
# 客户端播放失败原因：码表固定，便于 DWD 侧按 error_code 聚合
ERROR_CODES = [("E1001", "网络超时", 42), ("E1002", "音频解码失败", 26),
               ("E1003", "版权已下架", 18), ("E1004", "CDN 节点异常", 14)]
REFUND_REASONS = [("重复购买", 38), ("误操作", 34), ("内容不符预期", 18), ("其他", 10)]
SKU_PRICE = {s[0]: s[2] for s in SKUS}

# ---------------------------------------------------------------- 内容词库
# 歌名 = 修饰词 + 意象词（或再加后缀），三层组合可产出数万种，避免重名
SONG_MOD = ["温柔的", "孤单的", "最后的", "遥远的", "沉默的", "热烈的", "未寄出的",
            "没说完的", "深夜的", "凌晨的", "夏天的", "冬天的", "南方的", "北方的",
            "消失的", "错过的", "无人知晓的", "被遗忘的", "关于", "写给", "再见",
            "第一个", "第二个", "第三个", "那年"]
SONG_CORE = ["晚风", "盛夏", "归途", "山海", "孤岛", "星河", "旧信", "野渡", "晴空",
             "潮汐", "远行", "月色", "故里", "少年", "春天", "暗涌", "荒原", "回声",
             "光年", "南方", "海风", "雨夜", "黎明", "黄昏", "列车", "森林", "灯塔",
             "岛屿", "山谷", "海岸线", "宇宙", "银河", "流星", "鲸鱼", "蝴蝶", "玫瑰",
             "麦田", "铁轨", "橘子", "柠檬", "云层", "雪原", "长夜", "街角", "旧巷",
             "晚霞", "晨雾", "沙漏", "信纸", "落叶", "风筝", "渡口", "潮声", "北岛"]
SONG_TAIL = ["的你", "之外", "以后", "序曲", "手记", "与我", "时刻", "独白", "进行曲",
             "不眠", "未眠", "入梦", "如诗", "如你", "无声", "有光", "不散", "不老",
             "很远", "很近", "之前", "之后"]

# 姓名：百家姓 + 单双字名，组合数万种
SURNAMES = list("赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜") + \
           list("戚谢邹喻柏水窦章云苏潘葛奚范彭郎鲁韦昌马苗凤花方俞任袁柳鲍史唐费廉岑薛")
GIVEN_1 = list("伟芳娜敏静丽强磊军洋勇艳杰娟涛明超秀霞平刚桂英华建文辉红梅琳琪瑶婷宁翔")
GIVEN_2 = ["子墨", "思远", "雨桐", "浩然", "欣怡", "嘉豪", "梓涵", "文博", "晓彤", "一鸣",
           "语汐", "家豪", "若彤", "俊杰", "梦琪", "泽宇", "静怡", "子轩", "佳宁", "宇轩",
           "婧琪", "明轩", "书豪", "雅静", "天佑", "晨曦", "紫萱", "锦程", "亦然", "清和",
           "知遥", "望舒", "斯年", "以南", "颂一", "念安", "星野", "寒山", "遇安", "执之"]

# 昵称：音乐 APP 常见风格，模板 + 词库组合，重名率低
NICK_TMPL = ["{a}的{b}", "{a}{b}", "听{a}的{b}", "{a}里的{b}", "爱听{a}的{b}",
             "不睡{a}", "{a}收藏家", "循环{a}", "{a}与{b}", "{a}和{b}"]
NICK_A = ["晚风", "深夜", "耳机", "凌晨", "南方", "月光", "夏天", "旧唱片", "黑胶", "民谣",
          "摇滚", "爵士", "晴天", "雨天", "黄昏", "旅人", "孤独", "温柔", "沉默", "热忱",
          "黄昏后", "三点钟", "地下铁", "斑马线", "天台", "雨伞", "末班车", "凌晨四点"]
NICK_B = ["猫", "少年", "女孩", "旅人", "守夜人", "失眠者", "主义者", "收藏家", "听众",
          "过客", "药师", "岛屿", "星球", "列车", "信箱", "电台", "日记", "情书", "碎片",
          "回声", "动物园", "杂货铺", "便利店", "夜班"]

# 艺人名：两到三字，接近华语独立音乐人的命名习惯
ARTIST_SURNAME = list("林陈苏周许蔡黄吴郑谢洪范杨张李王刘邓何罗高")
ARTIST_GIVEN = ["夏", "默", "见", "声", "川", "野", "岸", "屿", "禾", "舟",
                "棠", "序", "南", "北", "夕", "烬", "眠", "知", "遥", "野子",
                "小满", "不晚", "未见", "半山", "十七", "六月", "阿肆", "小时"]

# 歌单名：真实场景风格，不是「XX 歌单」这种模板
PLAYLIST_NAMES = [
    "深夜emo必听", "通勤路上单曲循环", "跑步BGM 180bpm", "写代码时的背景音",
    "2026华语新歌速递", "一个人的雨天", "睡前一小时", "周末咖啡馆",
    "老歌重听计划", "被算法埋没的好歌", "民谣与远方", "摇滚不死",
    "轻音乐助眠", "开车必备", "青春记忆杀", "午后阳光",
    "古风入梦", "电音派对", "爵士小酒馆", "治愈系钢琴",
    "地铁通勤BGM", "清晨第一首", "加班续命歌单", "失恋第37天",
]

PUBLIC_COLS = ["event_id", "event_name", "event_time", "server_time", "user_id",
               "device_id", "is_login", "is_vip", "app_version", "os", "os_version",
               "device_model", "network", "carrier", "ip", "province", "city",
               "session_id", "session_seq", "is_foreground", "channel", "page_id"]

# 脏数据注入概率（单条事件命中一类即停止，避免同一记录叠加多类）
RATES = {"missing": 0.010, "invalid_enum": 0.004, "time_reversed": 0.004,
         "duration_exceed": 0.004, "future_ts": 0.003, "duplicate": 0.006}


def weighted(items):
    return [x[0] for x in items], [x[1] for x in items]


def zipf_weights(n, alpha=1.15):
    return [1.0 / ((i + 1) ** alpha) for i in range(n)]


# ---------------------------------------------------------------- 维表
def unique_name(gen, used, sep="·"):
    """生成唯一名称：重复则重试，多次失败后加序号兜底。"""
    for _ in range(20):
        n = gen()
        if n not in used:
            used.add(n)
            return n
    i, base = 2, gen()
    while f"{base}{sep}{i}" in used:
        i += 1
    n = f"{base}{sep}{i}"
    used.add(n)
    return n


def build_users(n_users, rng, now=None):
    """只生成用户维表。

    单独抽出来是为了支持「只扩用户、不动歌曲」：真实歌曲维表是
    gen_real_dims.py 从公开数据抓来的 1,486 首，一旦走 --regenerate-dims
    就会被虚构数据整个覆盖。用户和内容是两个独立维度，重建一个不该牵连另一个。
    """
    now = now or datetime.now()

    # 70% 真实姓名风格，30% 网名风格
    def gen_nick():
        if rng.random() < 0.7:
            given = rng.choice(GIVEN_1) if rng.random() < 0.35 else rng.choice(GIVEN_2)
            return rng.choice(SURNAMES) + given
        return rng.choice(NICK_TMPL).format(a=rng.choice(NICK_A), b=rng.choice(NICK_B))

    users, used_nick = [], set()
    for i in range(1, n_users + 1):
        users.append({
            "user_id": f"U{i:06d}",
            "nickname": unique_name(gen_nick, used_nick),
            "age": rng.choices([rng.randint(16, 22), rng.randint(23, 30), rng.randint(31, 45)],
                               weights=[25, 45, 30])[0],
            "gender": rng.choices(["M", "F"], weights=[54, 46])[0],
            "city": rng.choices(CITY_PAIRS, weights=CITY_WEIGHTS)[0][1],
            # 12% 的用户是「今天新注册」的，其余分布在过去 1~730 天。
            # 这个分布是新老用户判定的唯一依据：如果所有人注册时间都在过去，
            # 维表里就一个「新用户」都没有，大屏的新用户永远是 0 —— 指标不是算出来的，
            # 是数据造出来的，造的时候就得让它有区分度。
            "register_time": (now - timedelta(minutes=rng.randint(1, 600))
                              if rng.random() < 0.12
                              else now - timedelta(days=rng.randint(1, 730))
                              ).strftime("%Y-%m-%d %H:%M:%S"),
            "vip_level": rng.choices([0, 1, 2, 3], weights=[70, 15, 10, 5])[0],
        })
    return users


def build_dims(n_users, n_songs, n_artists, n_albums, n_playlists, rng):
    now = datetime.now()

    # --- 用户：70% 真实姓名风格，30% 网名风格 ---
    users = build_users(n_users, rng, now)

    # --- 艺人 ---
    def gen_artist_name():
        return rng.choice(ARTIST_SURNAME) + rng.choice(ARTIST_GIVEN)

    used_artist = set()
    artists = [{"artist_id": f"A{i:05d}",
                "artist_name": unique_name(gen_artist_name, used_artist),
                "country": rng.choices(["中国", "韩国", "日本", "美国"], weights=[80, 8, 7, 5])[0]}
               for i in range(1, n_artists + 1)]

    # --- 歌名 / 专辑名：三层组合，唯一性由 unique_name 保证 ---
    def gen_song_name():
        r = rng.random()
        if r < 0.45:
            return rng.choice(SONG_MOD) + rng.choice(SONG_CORE)
        if r < 0.85:
            return rng.choice(SONG_CORE) + rng.choice(SONG_TAIL)
        return rng.choice(SONG_MOD) + rng.choice(SONG_CORE) + rng.choice(SONG_TAIL)

    used_album = set()
    albums = [{"album_id": f"AL{i:05d}",
               "album_name": unique_name(gen_song_name, used_album),
               "artist_id": f"A{rng.randint(1, n_artists):05d}",
               "release_date": (now - timedelta(days=rng.randint(1, 2200))).strftime("%Y-%m-%d")}
              for i in range(1, n_albums + 1)]

    g_vals, g_wts = weighted(GENRES)
    used_song = set()
    songs = []
    for i in range(1, n_songs + 1):
        songs.append({
            "song_id": f"S{i:06d}",
            "song_name": unique_name(gen_song_name, used_song),
            "artist_id": f"A{rng.randint(1, n_artists):05d}",
            "album_id": f"AL{rng.randint(1, n_albums):05d}",
            "genre": rng.choices(g_vals, weights=g_wts)[0],
            "duration": rng.choices(
                [rng.randint(75, 140), rng.randint(140, 220), rng.randint(220, 300), rng.randint(300, 420)],
                weights=[12, 42, 36, 10])[0],
            "release_date": (now - timedelta(days=rng.randint(1, 2200))).strftime("%Y-%m-%d"),
        })

    # --- 歌单 ---
    def gen_playlist_name():
        base = rng.choice(PLAYLIST_NAMES)
        if rng.random() < 0.45:
            base += " " + rng.choice(["Vol.2", "精选集", "第二期", "2026版", "重制版"])
        return base

    used_pl = set()
    playlists = [{"playlist_id": f"P{i:05d}",
                  "playlist_name": unique_name(gen_playlist_name, used_pl),
                  "creator_user_id": f"U{rng.randint(1, n_users):06d}",
                  "genre": rng.choices(g_vals, weights=g_wts)[0]}
                 for i in range(1, n_playlists + 1)]
    return users, artists, albums, songs, playlists


# ---------------------------------------------------------------- 事件构造
class EventFactory:
    """按「公共属性 + 事件属性」两层结构构造埋点事件。"""

    def __init__(self, users, songs, playlists, rng):
        self.rng = rng
        self.song_by_id = {s["song_id"]: s for s in songs}
        self.user_ids = [u["user_id"] for u in users]
        # 设备号必须由 user_id 确定性派生，不能用随机数。
        # 之前是 "DV"+随机十六进制，意味着生成器每重启一次，同一个用户就换一台设备，
        # 后果是 UV 跨天不可比、回访用户恒为 0、留存率算不出来 —— 设备标识是
        # 一切用户侧指标的根基，它不稳定，上层所有指标都是假的。
        self.user_device = {u["user_id"]: "DV" + hashlib.md5(
                                u["user_id"].encode("utf-8")).hexdigest()[:8]
                            for u in users}
        # 设备属性按设备固定，不能每条事件重掷骰子。
        # 之前 os / os_version / device_model / carrier 是每条事件随机生成的，
        # 结果同一台设备一会儿 android 一会儿 iOS —— 大屏「OS 分布」按设备去重后
        # 加起来 1280 台 > 总设备 989 台，这种自相矛盾的数据一算就露馅。
        # network 故意保留逐事件随机：用户真会在 WiFi 和 4G 之间切换。
        self.device_profile = {}
        for u in users:
            dev = self.user_device[u["user_id"]]
            os_type = rng.choices(*weighted(OS_TYPES))[0]
            province, city = rng.choices(CITY_PAIRS, weights=CITY_WEIGHTS)[0]
            self.device_profile[dev] = {
                "os": os_type,
                "os_version": rng.choice(["12", "13", "14", "16", "17"]),
                "device_model": rng.choice(DEVICE_MODELS[os_type]),
                "carrier": rng.choice(CARRIERS),
                # 归属地也按设备固定。之前 province/city 是逐事件随机的，
                # 同一台设备这条在北京、下一条在上海 —— 地域分布按设备去重后
                # 每台设备只能取一条，看上去总量对得上，但用户画像完全不可信
                # （一个人不可能一分钟内跨三个省）。
                "province": province,
                "city": city,
            }
        self.song_ids = [s["song_id"] for s in songs]
        self.playlist_ids = [p["playlist_id"] for p in playlists]
        # VIP 状态以维表 vip_level 为准（不是每条事件掷骰子）。
        # 之前 is_vip 是 8% 随机布尔值，导致同一个用户这条事件是 VIP、下一条不是，
        # 大屏的「VIP 占比」只是一个随机数，没有任何业务含义。
        # vip_upgraded 记录本次运行中订阅过 VIP 的用户，后续事件同步变成 VIP。
        self.user_vip = {u["user_id"]: int(u.get("vip_level", 0)) for u in users}
        self.vip_upgraded = set()
        # 用户热度分布：alpha 越小越平。
        # 用 1.0（经典 Zipf）时，前 1% 用户吃掉 63% 的播放量 —— 明明有 10 万用户，
        # 事件里翻来覆去就是那几百个 ID，看起来像「用户很少」。
        # 0.7 让前 1% 约占 25%，接近真实音乐 App 的收听集中度
        # （头部明显、但长尾足够厚），配合上面的会话粘性，
        # 4 万条事件能覆盖约 1 万名独立用户。
        self.u_w = list(zipf_weights(len(self.user_ids), 0.7))
        # 预计算累积权重：random.choices 传 cum_weights 走 bisect（O(log n)），
        # 传 weights 则每次调用都要重新累加整条权重表（O(n)）。
        # 10 万用户时后者单次约 10ms，等于把生成器锁死在 100 条/秒 —— 必须预计算。
        self.u_cum = list(accumulate(self.u_w))
        # 歌曲热度：alpha 决定榜单头尾差距。
        # 数学关系很简单：Zipf 分布下 Top1 / TopN 的期望比值 ≈ N^alpha。
        #   alpha=1.15 → 20^1.15 ≈ 31 倍（实测 Top1 5404 / Top20 172 = 31.4，完全吻合）
        #   alpha=0.60 → 20^0.60 ≈ 6 倍
        # 31 倍的问题不是"假"，而是失真：真实音乐热歌榜 Top1 与第 20 名通常差 4~8 倍，
        # 31 倍意味着第一名几乎是第二名到第二十名的总和，榜单看着只剩一首歌。
        # 另外 alpha 太陡会让曲风分布被少数几首头部歌带偏（60 首 R&B 一度占了 13% 播放）。
        self.s_w = list(zipf_weights(len(self.song_ids), 0.6))
        rng.shuffle(self.s_w)
        self.s_cum = list(accumulate(self.s_w))

    def build(self, session_ctx=None):
        rng, now = self.rng, datetime.now()

        # ---- 会话粘性 ----
        # 真人打开 App 是「一个会话里连续产生多条事件」（播放→切歌→收藏→退出），
        # 不是每条事件重新抛一次用户骰子。
        # 之前每条事件独立抽用户，后果有两个，且都是结构性的：
        #   1) session_id 每条都是新的、session_seq 永远是 1 ——
        #      「会话数 / 人均会话时长 / 跳出率」全部算不出来；
        #   2) 一个用户在一段时间里只出现一次，人均事件数 1.2 条，
        #      看起来像所有人都点开即走。
        # 现在：会话开始时抽一次用户，连续产生 3~12 条事件后再换人。
        if session_ctx and session_ctx.get("left", 0) > 0:
            uid = session_ctx["user_id"]
            session_id = session_ctx["session_id"]
            seq = session_ctx["seq"] + 1
            left = session_ctx["left"] - 1
            login = session_ctx["login"]
        else:
            uid = rng.choices(self.user_ids, cum_weights=self.u_cum)[0]
            session_id = "s_" + uuid.UUID(int=rng.getrandbits(128)).hex[:12]
            seq = 1
            # 会话长度：约 25% 是「跳出会话」（只产生 1 条事件就退出，
            # 比如点开听几秒就关了），其余 2~6 条。
            # 跳出会话必须存在 —— 真实音乐 App 的跳出率约 20%~40%，
            # 如果所有会话都 ≥2 条，大屏的「跳出率」恒为 0，一眼就是造的数据。
            if rng.random() < 0.25:
                left = 0
            else:
                left = rng.randint(2, 6) - 1
            # 登录态在会话开始时定一次，会话内保持不变 ——
            # 用户不会在一分钟里反复登录登出；逐条随机会让同一 session 忽登忽游，
            # 登录率这个指标因此失去意义。
            login = rng.random() > 0.24

        sid = rng.choices(self.song_ids, cum_weights=self.s_cum)[0]
        song = self.song_by_id[sid]
        event_name = rng.choices(*weighted(EVENT_TYPES))[0]
        dev_id = self.user_device[uid]
        prof = self.device_profile[dev_id]          # 设备属性稳定，见 __init__
        os_type = prof["os"]
        province, city = prof["province"], prof["city"]
        # 商业化事件必须处于登录态：没登录没法下单、支付、退款。
        # 不加这一条，冒烟测试里就出现了「未登录用户订阅 VIP」的脏数据，
        # 付费率会因此偏高（分母含未登录设备、分子含未登录订单），而且查不出来。
        if event_name in ("vip_subscribe", "order_pay", "order_refund"):
            login = True

        # 写回会话上下文（商业化事件把 login 抬成 True 后要同步，
        # 否则同一会话的下一条又变回游客）
        session_ctx = {"user_id": uid, "session_id": session_id, "seq": seq,
                       "left": left, "login": login}

        et = int(now.timestamp() * 1000)
        ev = {
            "event_id": uuid.UUID(int=rng.getrandbits(128)).hex,
            "event_name": event_name,
            "event_time": et,
            "server_time": et + rng.randint(10, 200),
            "user_id": uid if login else "",
            # 设备号与登录态无关：未登录用户只是 user_id 为空，device_id 始终有值。
            # 否则「未登录」会被质检规则 device_id_missing 误判为异常，
            # 导致 12% 的正常行为被拦在 DWD 之外，UV 统计偏低。
            "device_id": dev_id,
            "is_login": "true" if login else "false",
            "is_vip": "true" if (self.user_vip.get(uid, 0) > 0
                                 or uid in self.vip_upgraded) else "false",
            "app_version": rng.choices(*weighted(APP_VERSIONS))[0],
            "os": os_type,
            "os_version": prof["os_version"],
            "device_model": prof["device_model"],
            # network 故意逐事件随机：用户真的会在 WiFi / 4G 之间切换，
            # 把它固定在设备上反而不真实。
            "network": rng.choices(*weighted(NETWORKS))[0],
            "carrier": prof["carrier"],
            "ip": f"{rng.randint(11, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
            "province": province,
            "city": city,
            "session_id": session_id,
            "session_seq": seq,
            "is_foreground": "true" if rng.random() > 0.15 else "false",
            "channel": rng.choices(*weighted(CHANNELS))[0],
            "page_id": rng.choice(PAGES),
            "properties": self._props(event_name, song),
        }
        # 订阅成功即升级：此后该用户的事件 is_vip 都是 true，
        # 让「VIP 占比」随时间单调上升，而不是一条事件一个随机值。
        if event_name == "vip_subscribe" and ev["is_login"] == "true":
            self.vip_upgraded.add(uid)
        return ev, session_ctx

    def _props(self, event_name, song):
        rng = self.rng
        sid, dur = song["song_id"], song["duration"]
        if event_name == "play_start":
            return {"song_id": sid, "playlist_id": rng.choice(self.playlist_ids),
                    "quality": rng.choice(["标准", "高品", "无损"]),
                    "play_mode": rng.choice(["顺序", "随机", "单曲循环"]),
                    "song_index": rng.randint(1, 50)}
        if event_name == "play_end":
            reason = rng.choices(*weighted(END_REASONS))[0]
            played = dur if reason == "natural" else int(dur * rng.uniform(0.05, 0.7))
            return {"song_id": sid, "song_duration": dur, "play_duration": played,
                    "play_progress": round(played / dur, 4), "end_reason": reason}
        if event_name == "skip":
            return {"song_id": sid, "played_duration": int(dur * rng.uniform(0.02, 0.35)),
                    "skip_type": rng.choices(["手动", "自动"], weights=[82, 18])[0]}
        if event_name == "collect":
            return {"song_id": sid,
                    "collect_type": rng.choices(["add", "cancel"], weights=[88, 12])[0]}
        if event_name == "search":
            # 真实搜索习惯：搜意象词片段 / 完整歌名 / 歌手名 / 模糊短语
            r = rng.random()
            if r < 0.50:
                kw = rng.choice(SONG_CORE)
            elif r < 0.75:
                kw = rng.choice(SONG_MOD) + rng.choice(SONG_CORE)
            elif r < 0.92:
                kw = rng.choice(ARTIST_SURNAME) + rng.choice(ARTIST_GIVEN)
            else:
                kw = rng.choice(["适合下雨天", "好听的", "最近很火", "抖音", "翻唱", "纯音乐"])
            cnt = rng.choices([0, rng.randint(1, 15), rng.randint(16, 80), rng.randint(81, 300)],
                              weights=[8, 30, 42, 20])[0]
            return {"keyword": kw,
                    "search_type": rng.choices(SEARCH_TYPES, weights=[52, 41, 7])[0],
                    "result_count": cnt,
                    "clicked_song_id": rng.choice(self.song_ids) if cnt else ""}
        if event_name == "share":
            return {"song_id": sid, "share_channel": rng.choice(SHARE_CHANNELS)}
        # ---- 商业化：订阅 / 购买 / 退款 ----
        # 金额一律取自 SKU_PRICE，绝不随机。这是「收入可对账」的前提：
        # 大屏的收入 = Σ 已支付订单金额，必须等于「Σ 订单数 × 单价」，
        # 随机金额会让两个算法差出几倍，答辩时一问就穿。
        if event_name in ("vip_subscribe", "order_pay", "order_refund"):
            oid = "OD" + uuid.UUID(int=rng.getrandbits(128)).hex[:16]
            if event_name == "vip_subscribe":
                sku = rng.choices(VIP_SKUS, weights=[52, 26, 22])[0]
                return {"sku_id": sku, "amount": SKU_PRICE[sku], "order_id": oid,
                        "pay_channel": rng.choices(*weighted(PAY_CHANNELS))[0],
                        "plan_days": {s[0]: s[3] for s in SKUS}[sku], "song_id": ""}
            if event_name == "order_pay":
                sku = rng.choices(["song_single", "digital_album"], weights=[80, 20])[0]
                return {"sku_id": sku, "amount": SKU_PRICE[sku], "order_id": oid,
                        "pay_channel": rng.choices(*weighted(PAY_CHANNELS))[0],
                        "plan_days": 0, "song_id": rng.choice(self.song_ids)}
            # 退款按原额原路（真实退款语义），金额同样取自 SKU 单价
            sku = rng.choice(list(SKU_PRICE))
            return {"sku_id": sku, "amount": SKU_PRICE[sku], "order_id": oid,
                    "pay_channel": rng.choices(*weighted(PAY_CHANNELS))[0],
                    "plan_days": 0,
                    "refund_reason": rng.choices(*weighted(REFUND_REASONS))[0]}
        # ---- 客户端质量：播放失败 ----
        if event_name == "play_error":
            code, msg, _ = rng.choices(ERROR_CODES, weights=[c[2] for c in ERROR_CODES])[0]
            return {"song_id": sid, "error_code": code, "error_msg": msg,
                    "play_position": rng.randint(0, max(1, dur)),
                    "network": rng.choices(*weighted(NETWORKS))[0]}
        return {"page_id": rng.choice(PAGES), "referrer_page": rng.choice(PAGES),
                "stay_duration": rng.randint(1, 600)}


# ---------------------------------------------------------------- 脏数据注入
def inject_dirt(ev, rng, manifest):
    """按概率注入一类脏数据并记录 ground truth。"""
    def mark(kind, detail):
        manifest.append({"event_id": ev["event_id"], "dirt_type": kind,
                         "detail": detail, "event_name": ev["event_name"],
                         "server_time": ev["server_time"]})

    r, acc = rng.random(), 0.0
    acc += RATES["missing"]
    if r < acc:
        f = rng.choice(["device_id", "user_id", "event_name"])
        ev[f] = ""
        mark("missing_field", f)
        return
    acc += RATES["invalid_enum"]
    if r < acc:
        old = ev["event_name"]
        ev["event_name"] = rng.choice(["PLAY", "playing", "unknown"])
        mark("invalid_enum", f"{old}->{ev['event_name']}")
        return
    acc += RATES["time_reversed"]
    if r < acc:
        ev["server_time"] = ev["event_time"] - rng.randint(60000, 600000)
        mark("time_reversed", "server_time 早于 event_time 超过 60s")
        return
    acc += RATES["duration_exceed"]
    if r < acc and ev["event_name"] == "play_end":
        p = ev["properties"]
        p["play_duration"] = p["song_duration"] + rng.randint(5, 300)
        p["play_progress"] = round(p["play_duration"] / p["song_duration"], 2)
        mark("duration_exceed", f"{p['play_duration']}>{p['song_duration']}")
        return
    acc += RATES["future_ts"]
    if r < acc:
        et = ev["event_time"] + rng.randint(2, 30) * 86400000
        ev["event_time"], ev["server_time"] = et, et + 100
        mark("future_timestamp", "event_time 推后 2~30 天")


# ---------------------------------------------------------------- 输出
def write_csv(path, rows, columns):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def read_csv(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return rows or None


def load_dims():
    """复用磁盘上已有的维表，不重新生成。

    为什么要这个：dim_song / dim_artist / dim_album 已由 code/gen_real_dims.py
    从 MusicBrainz 抓取为**真实歌曲与歌手**。如果每次发数据都走 build_dims，
    这些真实数据会被虚构的"盛夏之下/赵语汐"覆盖掉。

    用户、歌单仍然可以是生成的——前者本就是虚构的用户画像，
    后者按需求允许虚构（但曲目来自真实歌曲）。
    """
    dim = os.path.join(BASE_DIR, "data", "dim")
    users = read_csv(os.path.join(dim, "dim_user.csv"))
    songs = read_csv(os.path.join(dim, "dim_song.csv"))
    artists = read_csv(os.path.join(dim, "dim_artist.csv"))
    albums = read_csv(os.path.join(dim, "dim_album.csv"))
    playlists = read_csv(os.path.join(dim, "dim_playlist.csv"))
    if not (users and songs and playlists):
        return None
    for s in songs:
        try:
            s["duration"] = int(float(s["duration"]))
        except (TypeError, ValueError):
            s["duration"] = 200
    for u in users:
        try:
            u["age"] = int(float(u["age"]))
            u["vip_level"] = int(float(u["vip_level"]))
        except (TypeError, ValueError):
            u["age"], u["vip_level"] = 25, 0
    return users, artists or [], albums or [], songs, playlists



def run_batch(args, factory, rng):
    events, manifest = [], []
    for _ in range(args.events):
        ev, _ = factory.build()
        inject_dirt(ev, rng, manifest)
        events.append(ev)
        if rng.random() < RATES["duplicate"]:
            dup = json.loads(json.dumps(ev))
            manifest.append({"event_id": dup["event_id"], "dirt_type": "duplicate_event_id",
                             "detail": "copied", "event_name": dup["event_name"],
                             "server_time": dup["server_time"]})
            events.append(dup)
    events.sort(key=lambda e: e["event_time"])

    flat = []
    for e in events:
        row = {k: e[k] for k in PUBLIC_COLS}
        row["properties"] = json.dumps(e["properties"], ensure_ascii=False)
        flat.append(row)

    write_csv(os.path.join(BASE_DIR, "data", "events", "events.csv"),
              flat, PUBLIC_COLS + ["properties"])
    with open(os.path.join(BASE_DIR, "data", "events", "events_kafka.jsonl"), "w",
              encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    write_csv(os.path.join(BASE_DIR, "data", "quality", "dirty_manifest.csv"), manifest,
              ["event_id", "dirt_type", "detail", "event_name", "server_time"])
    return events, manifest


def run_stream(args, factory, rng):
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        linger_ms=20, batch_size=32768, acks=1,
    )
    print(f"Kafka={args.bootstrap}  topic={args.topic}")
    print(f"速率 {args.rate} 条/秒，总量上限 {args.total} 条，Ctrl+C 中断")

    interval, per_tick = 0.1, max(1, int(round(args.rate * 0.1)))
    produced, manifest, session_ctx = 0, [], None
    t0 = time.time()
    try:
        while produced < args.total:
            tick = time.time()
            for _ in range(min(per_tick, args.total - produced)):
                ev, session_ctx = factory.build(session_ctx=session_ctx)
                inject_dirt(ev, rng, manifest)
                producer.send(args.topic, json.dumps(ev, ensure_ascii=False).encode("utf-8"))
                produced += 1
                if rng.random() < RATES["duplicate"]:
                    dup = json.loads(json.dumps(ev))
                    producer.send(args.topic, json.dumps(dup, ensure_ascii=False).encode("utf-8"))
                    manifest.append({"event_id": dup["event_id"], "dirt_type": "duplicate_event_id",
                                     "detail": "copied", "event_name": dup["event_name"],
                                     "server_time": dup["server_time"]})
                    produced += 1
            gap = interval - (time.time() - tick)
            if gap > 0:
                time.sleep(gap)
            if produced % (per_tick * 20) == 0:
                cost = time.time() - t0
                print(f"  已发送 {produced} 条 | 耗时 {cost:.1f}s | 实际 {produced / cost:.0f} 条/s", flush=True)
    except KeyboardInterrupt:
        print("\n手动中断")
    finally:
        producer.flush()
        producer.close()
        write_csv(os.path.join(BASE_DIR, "data", "quality", "dirty_manifest_stream.csv"),
                  manifest, ["event_id", "dirt_type", "detail", "event_name", "server_time"])
        cost = time.time() - t0
        print(f"结束：发送 {produced} 条，脏数据 {len(manifest)} 条，平均 {produced / cost:.0f} 条/s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["batch", "stream"], default="batch")
    ap.add_argument("--users", type=int, default=100000,
                    help="用户规模。10 万是刻意选的：3 千用户时 UV、留存这类指标没有统计意义，"
                         "而且事件里翻来覆去就那几百个 ID，一眼看出是造的数据")
    ap.add_argument("--songs", type=int, default=5000)
    ap.add_argument("--artists", type=int, default=800)
    ap.add_argument("--albums", type=int, default=2000)
    ap.add_argument("--playlists", type=int, default=600)
    ap.add_argument("--events", type=int, default=50000, help="batch 模式总条数")
    ap.add_argument("--rate", type=int, default=10, help="stream 模式每秒条数")
    ap.add_argument("--total", type=int, default=20000, help="stream 模式总量上限")
    ap.add_argument("--bootstrap", default="WWC161:9092")
    ap.add_argument("--topic", default="music_events")
    ap.add_argument("--seed", type=int, default=None,
                    help="随机种子；batch 默认 42（可复现），stream 默认取当前时间戳"
                         "（否则每次重启 event_id 序列完全相同，与历史数据撞车）")
    ap.add_argument("--regenerate-dims", action="store_true",
                    help="重新生成虚构维表。默认复用磁盘上已有维表"
                         "（含 gen_real_dims.py 抓取的真实歌曲数据）")
    ap.add_argument("--regen-users-only", action="store_true",
                    help="只按 --users 重建 dim_user.csv，歌曲/歌手/专辑/歌单原样保留。"
                         "扩用户规模时用这个，别用 --regenerate-dims（会覆盖 1,486 首真实歌曲）")
    ap.add_argument("--dims-only", action="store_true",
                    help="生成维表后立即退出，不产生任何事件（配合 --regen-users-only 只做维表迁移）")
    args = ap.parse_args()

    # seed 选择逻辑：
    #   batch  固定 42，保证实验数据可复现，同一份数据能反复重放对比
    #   stream 默认时间戳，保证每次重启生成的 event_id 不与历史冲突
    #   显式传 --seed 时以参数为准
    if args.seed is not None:
        seed = args.seed
    elif args.mode == "batch":
        seed = 42
    else:
        seed = int(time.time() * 1000) % (2 ** 31)
    rng = random.Random(seed)

    # 维表：默认复用磁盘上已有的数据（真实歌曲/歌手），需要虚构维表时显式 --regenerate-dims
    reused = None if args.regenerate_dims else load_dims()

    # 只扩用户：内容维表（歌曲/歌手/专辑/歌单）必须是已有那份，
    # 只重建用户并落盘，其余一个字节都不动。
    if args.regen_users_only:
        if not reused:
            sys.exit("磁盘上没有可复用的内容维表，请先跑一次 --regenerate-dims")
        users = build_users(args.users, rng)
        _, artists, albums, songs, playlists = reused
        write_csv(os.path.join(BASE_DIR, "data", "dim", "dim_user.csv"), users,
                  ["user_id", "nickname", "age", "gender", "city", "register_time", "vip_level"])
        print(f"[1/2] 已重建用户维表：{len(users)} 用户（内容维表复用：{len(songs)} 歌曲 / "
              f"{len(artists)} 歌手 / {len(albums)} 专辑 / {len(playlists)} 歌单）")
    elif reused:
        users, artists, albums, songs, playlists = reused
        print(f"[1/2] 复用已有维表：{len(users)} 用户 / {len(artists)} 歌手 / "
              f"{len(albums)} 专辑 / {len(songs)} 歌曲 / {len(playlists)} 歌单")
    else:
        print(f"[1/2] 生成维表 ... (seed={seed})")
        users, artists, albums, songs, playlists = build_dims(
            args.users, args.songs, args.artists, args.albums, args.playlists, rng)
        write_csv(os.path.join(BASE_DIR, "data", "dim", "dim_user.csv"), users,
                  ["user_id", "nickname", "age", "gender", "city", "register_time", "vip_level"])
        write_csv(os.path.join(BASE_DIR, "data", "dim", "dim_song.csv"), songs,
                  ["song_id", "song_name", "artist_id", "album_id", "genre", "duration", "release_date"])
        write_csv(os.path.join(BASE_DIR, "data", "dim", "dim_artist.csv"), artists,
                  ["artist_id", "artist_name", "country"])
        write_csv(os.path.join(BASE_DIR, "data", "dim", "dim_album.csv"), albums,
                  ["album_id", "album_name", "artist_id", "release_date"])
        write_csv(os.path.join(BASE_DIR, "data", "dim", "dim_playlist.csv"), playlists,
                  ["playlist_id", "playlist_name", "creator_user_id", "genre"])

    if args.dims_only:
        # 提前返回：EventFactory 会为每个用户建一份设备画像，10 万用户时纯属浪费
        print("[2/2] --dims-only：跳过事件生成")
        return

    factory = EventFactory(users, songs, playlists, rng)

    if args.mode == "batch":
        print("[2/2] 批量生成事件 ...")
        events, manifest = run_batch(args, factory, rng)
        print(f"事件 {len(events)} 条，脏数据 {len(manifest)} 条")
        print("事件类型：", dict(Counter(e["event_name"] for e in events).most_common()))
        print("脏数据类型：", dict(Counter(m["dirt_type"] for m in manifest).most_common()))
    else:
        print("[2/2] 持续发送到 Kafka ...")
        run_stream(args, factory, rng)


if __name__ == "__main__":
    main()
