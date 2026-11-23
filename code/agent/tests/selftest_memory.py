"""长期偏好自测（显式写入 / 数字不落库 / 越权隔离 / 口径失效 / 缓存指纹）。

设计边界在测什么
----------------
  · 只有显式说出口的话才写；"那昨天呢"这种追问绝不沉淀；
  · content 里不能留数字——那是最容易被模型抄进新答案的东西；
  · 只按 user_id 取，别人的偏好一条都看不到、删不到；
  · 口径版本不一致的偏好一条都不注入（5.24 倍事故：旧口径不能影响新答案）；
  · 偏好变了缓存 key 变，不会回放"没带偏好时"的旧答案；
  · eval / cli 不写不读：128 题基线逐字不变。

运行：
    python code/agent/tests/selftest_memory.py
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import copy
import sys

import answercache
import memory
from conf import load_config
from governance import store

OK = FAIL = 0
FAILED: list[str] = []


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        FAILED.append(name)
        print("  ✗ %s %s" % (name, extra))


def t_rules() -> None:
    print("\n[1] 归纳规则：只有显式指令，且不落数字")
    for s in ("以后都按播放人数算", "记住：默认看 11-08 的 DAU", "下次开始统一按 VIP 口径"):
        ck("「%s」识别为显式" % s, memory.is_remember(s))
    for s in ("那昨天呢", "2026-11-08 的 DAU 是多少", "播放量怎么样"):
        ck("「%s」不当成显式" % s, not memory.is_remember(s))
    ck("忘掉指令能识别", memory.is_forget("忘掉刚才那条偏好"))
    ck("同一句偏好去重键一致",
       memory.memory_key("以后按播放人数算") == memory.memory_key("以后按播放人数算"))
    item = memory.extract_preference("记住：以后都按播放人数算，只看 VIP 用户")
    ck("能剥出偏好", item is not None and item["kind"] == "preference", str(item))
    ck("偏好不含数字", item is not None and "2026" not in item["content"])
    st = {"metrics": ["play_uv"], "time_window": {"label": "11-08"},
          "question": "以后默认这样查"}
    item2 = memory.extract_preference("以后默认这样查", st)
    ck("带上指标与窗口", bool(item2) and "play_uv" in str(item2["payload"])
       and "11-08" in item2["content"], str(item2))


def t_semantic(cfg: dict) -> None:
    print("\n[3b] 语义召回：同义改写 + 人数/次数硬规则")
    items = [
        {"mem_id": 1, "content": "以后按播放人数算", "payload": {}},
        {"mem_id": 2, "content": "以后按播放次数算", "payload": {}},
        {"mem_id": 3, "content": "默认看华东地区的收入", "payload": {}},
    ]
    best = lambda q: max(items, key=lambda i: memory._score(i, q, cfg))["content"]
    ck("同义改写召回到人数偏好", best("去重用户数怎么算") == "以后按播放人数算")
    ck("字面命中召回到次数偏好", best("播放次数") == "以后按播放次数算")
    ck("地域偏好不被误召回", best("华东收入") == "默认看华东地区的收入")
    cfg_off = copy.deepcopy(cfg)
    cfg_off["memory"]["semantic"] = False
    ck("关掉语义退化为字符方案仍可用",
       isinstance(memory._score(items[0], "人数", cfg_off), float))
    try:
        import vecindex
        ck("BGE 编码器单例复用", memory._encoder(cfg) is memory._encoder(cfg))
    except Exception as e:
        ck("语义失败时静默退回字符方案",
           isinstance(memory._score(items[0], "人数", cfg), float), str(e)[:60])


def t_prompt(cfg: dict) -> None:
    print("\n[2] prompt 注入：空记忆逐字不变，非空必须声明不复用数字")
    import promptlib
    state = {"question": "播放量怎么样"}
    a = promptlib.build_generate_messages(state, cfg)
    b = promptlib.build_generate_messages(dict(state, memory_turns=[]), cfg)
    ck("空记忆 prompt 与无记忆逐字相同", a == b)
    item = {"mem_id": 1, "kind": "preference", "content": "以后按播放人数算",
            "payload": {"metrics": ["play_uv"]}}
    c = promptlib.build_generate_messages(dict(state, memory_turns=[item]), cfg)
    txt = "\n".join(str(m.get("content") or "") for m in c)
    ck("偏好文本进了 prompt", "以后按播放人数算" in txt)
    ck("提示不得复用历史数字", "不得当作本轮答案" in txt)
    ck("没有把 SQL 结果表塞进 prompt", "answer_brief" not in txt)


def t_cache(cfg: dict) -> None:
    print("\n[3] 缓存指纹：偏好变了 key 就变")
    from governance.contracts import RequestContext, PolicyDecision
    ctx = RequestContext.system(source="web", user_id="u_mem")
    dec = PolicyDecision.unrestricted_for("analyst")
    cfg2 = copy.deepcopy(cfg)
    cfg2["cache"] = dict(cfg2.get("cache") or {}, enabled=True)
    a = answercache.make_key("播放量怎么样", cfg2, dec)
    b = answercache.make_key("播放量怎么样", cfg2, dec,
                             extra="mem:" + memory.fingerprint(
                                 [{"mem_id": 1, "content": "按人数"}]))
    c = answercache.make_key("播放量怎么样", cfg2, dec,
                             extra="mem:" + memory.fingerprint(
                                 [{"mem_id": 2, "content": "按次数"}]))
    ck("无偏好与有偏好 key 不同", a != b)
    ck("偏好不同 key 不同", b != c)


def t_store(cfg: dict) -> None:
    print("\n[4] 落库：可写、可查、可删、越权隔离")
    cfg2 = copy.deepcopy(cfg)
    u1, u2 = "u_mem_selftest_1", "u_mem_selftest_2"
    # 先清掉上轮残留（软删自己的）
    for r in memory.list_all(u1, cfg2):
        memory.remove(r["mem_id"], u1, cfg2)
    for r in memory.list_all(u2, cfg2):
        memory.remove(r["mem_id"], u2, cfg2)

    ok, err = memory.add(u1, "以后都按播放人数算", cfg2)
    ck("写入成功", ok, err)
    rows = memory.list_all(u1, cfg2)
    ck("查得到刚写的偏好", any("播放人数" in r["content"] for r in rows), str(len(rows)))
    ck("偏好不带数字", all("2026" not in r["content"] for r in rows))
    ck("别人看不到", not memory.list_all(u2, cfg2))

    dup, _ = memory.add(u1, "以后都按播放人数算", cfg2)
    ck("重复写入不新增行", dup and len(memory.list_all(u1, cfg2)) == len(rows))

    recall = memory.recall(u1, "播放量按人数算", cfg2)
    ck("相关问句能召回", bool(recall), str(recall)[:120])
    ck("无关问句不召回", not memory.recall(u1, "今天天气怎么样", cfg2))
    ck("别人召回不到", not memory.recall(u2, "播放量按人数算", cfg2))

    assert rows
    mid = rows[0]["mem_id"]
    ck("删除自己的偏好成功", memory.remove(mid, u1, cfg2))
    ck("别人删不到", not memory.remove(mid, u2, cfg2))
    ck("删完查不到", not memory.list_all(u1, cfg2))

    # 口径版本不一致 → 不召回
    memory.add(u1, "以后按播放人数算", cfg2)
    stale_key = "pref:stale-selftest"
    store.upsert_memory(u1, "default", "preference", stale_key,
                        "过期口径按旧口径算", {}, "v-stale", "", cfg=cfg2)
    stale_id = next((x["mem_id"] for x in memory.list_all(u1, cfg2)
                     if x["caliber_version"] == "v-stale"), -1)
    bad = memory.recall(u1, "过期口径按旧口径算", cfg2)
    ck("过期口径的偏好不注入", all(r["mem_id"] != stale_id for r in bad), str(bad)[:120])
    for r in memory.list_all(u1, cfg2):
        memory.remove(r["mem_id"], u1, cfg2)


def main() -> int:
    cfg = load_config()
    print("=" * 62)
    print("长期偏好自测（显式写入 / 数字不落库 / 越权 / 口径 / 缓存）")
    print("=" * 62)
    t_rules()
    t_prompt(cfg)
    t_cache(cfg)
    t_semantic(cfg)
    t_store(cfg)
    print("\n" + "-" * 62)
    print("%d 通过 / %d 失败" % (OK, FAIL))
    if FAILED:
        print("失败项：")
        for f in FAILED:
            print("  · %s" % f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
