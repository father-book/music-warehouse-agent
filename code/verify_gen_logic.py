#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_gen_logic.py —— 在本地复算 gen_offline_ods.py v6 的造数逻辑

为什么要在本地先算一遍：
  造数脚本跑一次 30 天要 5~6 分钟，加上下游 DWD/DWS/ADS 重跑要 40 分钟。
  而「留存是不是等差」「曲风完播率有没有伪相关」这两件事，本质是**整数运算**，
  不依赖 Spark —— 完全可以在本地用纯 Python 按同样的公式算一遍。
  算对了再上集群，避免「跑完 40 分钟发现曲线还是直线」。

复算的三件事：
  ① 留存曲线：cohort = 首次活跃日，day1~day7 的 retained / cohort_size
  ② 曲风完播率：四个曲风是否都在整体值附近（无系统性偏差）
  ③ 歌曲覆盖：5000 首是否每首都出现过，分布是否均匀
用法：python3 verify_gen_logic.py
"""
SESSION_EVENTS = 20
SONGS_PER_SESSION = 5
DAYS = 30
SESS_PER_DAY = 50000
ROWS_PER_DAY = SESS_PER_DAY * SESSION_EVENTS
USER_CNT = 100000
SONG_CNT = 5000

# ── 与 gen_offline_ods.py 完全一致的参数推导 ─────────────────────────────────
L0_SLOTS = max(1, SESS_PER_DAY // 10)
L1_SLOTS = max(1, SESS_PER_DAY * 3 // 10)
L2_SLOTS = SESS_PER_DAY - L0_SLOTS - L1_SLOTS
L0_POOL = L0_SLOTS
L1_POOL = L1_SLOTS * 2            # pool = 2×slots ⇒ 窗口永不绕圈
L2_POOL = L2_SLOTS * 2
R1, R2 = 0.75, 0.28               # 两层各自的回访衰减：off_d = slots×(1-r^d)
L1_END = L0_SLOTS + L1_SLOTS

GENRE_SLOTS = ["流行"] * 8 + ["摇滚"] * 5 + ["民谣"] * 4 + ["R&B"] * 3


def pmod(a, b):
    """Spark 的 pmod：结果符号跟随除数（这里全正，等价于 Python %）"""
    return a % b


def pick_k(k, pool, salt, dt):
    """当天从池里取用户：k * (7919 + dt*salt) mod pool。

    为什么不是「k*7919 + dt*salt」（平移）：
      平移会让两天的集合差一个固定偏移，交集 = slots - dt*salt，
      又变回等差数列 —— 换了个方向的同一个 bug。
    改成「改变乘数」：两天的集合是**两个不同步长的等差数列**，
    交集接近随机子集的期望 slots²/pool，这才是真正的「部分重叠」。
    salt 取 30 的倍数 ⇒ 乘数恒为奇数且不被 3、5 整除 ⇒ 与 pool 互质 ⇒ 当天无撞号。
    """
    return pmod(k * (7919 + dt * salt), pool)


def user_idx_of(dt, sess):
    """三层用户池 + 按比例的「滑出窗口」。

    L0 铁杆：ID 固定，每天都来 → 贡献常数留存 10%
    L1 高频：每天从池尾滑出 L1_SLOTS×(1-0.75^d) 个位置 → 重叠 15000×0.75^d
    L2 低频：同理，重叠 30000×0.40^d
    合成 day_n = (5000 + 15000×0.75^n + 30000×0.40^n) / 50000
      n=1 → 56.5%   n=2 → 36.5%   n=3 → 26.5%   n=7 → 14.1%
    首日陡降、之后逐日收窄并趋平 —— 这才是留存曲线，不是直线。
    """
    if sess < L0_SLOTS:
        return sess
    if sess < L1_END:
        off = int(L1_SLOTS * (1 - R1 ** dt))
        return L0_POOL + pmod((sess - L0_SLOTS + off) * 7919, L1_POOL)
    off = int(L2_SLOTS * (1 - R2 ** dt))
    return L0_POOL + L1_POOL + pmod((sess - L1_END + off) * 7919, L2_POOL)


def song_num_of(dt, sess, song_no):
    """歌曲 ID：三项加权后取模。

    为什么不用乘法哈希：combo 是连续整数，combo*C mod 2^32 再右移取高位
    对连续输入仍是**仿射**的（相邻 combo 的增量恒定），打不散。
    实测那个写法下同一会话 5 首歌的 song_num 恒差 2460（20 的倍数），
    曲风完全相同，伪相关反而更重。

    这里的做法是「让三个变量落在互不干扰的偏移上」：
      sess*5   → 周期 1000，只依赖 sess mod 1000（低位）
      song_no*1007 → 1007 mod 20 = 7，5 首歌的曲风错开 7 个槽位
      dt*7919  → 每天整体平移 2919，日榜不同
    而 is_complete 用的是 sess//1000（高位）+ seq，与 sess mod 1000 正交。
    低/高位分开取，才是真正的独立。
    """
    return pmod(sess * 5 + song_no * 1007 + dt * 7919, SONG_CNT)


print("=" * 78)
print("用户池：L0 槽%d/池%d  L1 槽%d/池%d (p=%.2f)  L2 槽%d/池%d (p=%.2f)"
      % (L0_SLOTS, L0_POOL, L1_SLOTS, L1_POOL, L1_SLOTS / L1_POOL,
         SESS_PER_DAY - L1_END, L2_POOL, (SESS_PER_DAY - L1_END) / L2_POOL))

# ── ① 留存：cohort = 首次活跃日 ──────────────────────────────────────────────
print("=" * 78)
print("① 留存曲线（cohort = 首次活跃日）")
day_users = []
first_day = {}
for d in range(DAYS):
    us = set()
    for s in range(SESS_PER_DAY):
        u = user_idx_of(d, s)
        us.add(u)
        if u not in first_day:
            first_day[u] = d
    day_users.append(us)
    print("   day%02d 活跃用户 %6d（累计去重 %6d）" % (d, len(us), len(first_day)))

print("\n   同期群留存：")
print("   %-10s %8s %8s %8s %8s %8s %8s %8s" %
      ("cohort", "size", "day1", "day2", "day3", "day4", "day5", "day7"))
for c in range(5):
    cohort = {u for u in day_users[c] if first_day[u] == c}
    if not cohort:
        continue
    cells = []
    for n in (1, 2, 3, 4, 5, 7):
        if c + n < DAYS:
            ret = len(cohort & day_users[c + n]) / float(len(cohort))
            cells.append("%7.2f%%" % (ret * 100))
        else:
            cells.append("     -  ")
    print("   day%02d-%02d %8d %s" % (c, c, len(cohort), " ".join(cells)))

# 旧公式对照，证明「等差」不是错觉
print("\n   旧公式对照（user_idx = (dt*7313 + sess) % 100000）：")
old = [{(d * 7313 + s) % 100000 for s in range(SESS_PER_DAY)} for d in range(8)]
c0 = old[0]
for n in range(1, 8):
    print("     day%d retained %6d  (差值 %6d)"
          % (n, len(c0 & old[n]), len(c0 & old[n - 1]) - len(c0 & old[n])))

# ── ② 曲风完播率 ─────────────────────────────────────────────────────────────
print("=" * 78)
print("② 曲风完播率（抽 3 天全量算，与 spark 口径一致：complete / play_start）")
g_start = {}
g_end = {}
g_natural = {}
song_seen = set()
for d in (0, 1, 2):
    for s in range(SESS_PER_DAY):
        local_base = s * SESSION_EVENTS
        del local_base
        for sn in range(SONGS_PER_SESSION):
            sg = song_num_of(d, s, sn)
            song_seen.add(sg)
            genre = GENRE_SLOTS[sg % 20]
            g_start[genre] = g_start.get(genre, 0) + 1     # seg==0 → play_start
            # seg==1 → play_end / skip
            seq = sn * 4 + 1
            is_complete = ((s // 1000) * 3 + seq * 7) % 10 < 7
            if is_complete:
                dur = 120 + sg % 179
                k = pmod(s * 7919 + 104729 * d, 29)
                prog = (dur - k) / float(dur)
                g_end[genre] = g_end.get(genre, 0) + 1
                if prog >= 0.9:
                    g_natural[genre] = g_natural.get(genre, 0) + 1

tot_s = sum(g_start.values())
tot_c = sum(g_natural.values())
for g in ["流行", "摇滚", "民谣", "R&B"]:
    print("   %-4s 起播 %8d (%5.2f%%)  结束 %8d  完播 %8d  完播率 %6.2f%%"
          % (g, g_start[g], g_start[g] * 100.0 / tot_s, g_end[g],
             g_natural[g], g_natural[g] * 100.0 / g_start[g]))
print("   整体完播率 %.2f%%（起播 %d / 完播 %d）" % (tot_c * 100.0 / tot_s, tot_s, tot_c))
spread = max(g_natural[g] * 1.0 / g_start[g] for g in g_start) - \
         min(g_natural[g] * 1.0 / g_start[g] for g in g_start)
print("   曲风间极差 %.3f 个百分点  %s"
      % (spread * 100, "✓ 无系统性差异" if spread < 0.02 else "✗ 仍有伪相关，需要再改"))

# ── ③ 歌曲覆盖 ───────────────────────────────────────────────────────────────
print("=" * 78)
cnt = [0] * SONG_CNT
for d in (0, 1, 2):
    for s in range(0, SESS_PER_DAY, 7):        # 抽样 1/7 会话，控制本地耗时
        for sn in range(SONGS_PER_SESSION):
            cnt[song_num_of(d, s, sn)] += 1
covered = sum(1 for x in cnt if x > 0)
mx, mn = max(cnt), min(cnt)
print("③ 歌曲覆盖（抽样 %d 会话 × 3 天）：覆盖 %d/%d 首，单首命中 %d~%d 次，均值 %.1f"
      % (len(range(0, SESS_PER_DAY, 7)), covered, SONG_CNT, mn, mx, sum(cnt) / float(SONG_CNT)))
print("   %s" % ("✓ 全覆盖且分布均匀" if covered == SONG_CNT and mx < 3 * (sum(cnt) / float(SONG_CNT))
                 else "✗ 覆盖或均匀性有问题"))
print("=" * 78)
