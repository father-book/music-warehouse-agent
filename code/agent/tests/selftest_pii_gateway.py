"""PII 进模型前脱敏的自测（P0-3）。

为什么这条必须有独立自测：

  `governance/pii.py` 早就写了，审计和结果层也在用，但**用户问句进模型那一段
  一直是原文**。这类漏洞不会报错、不会掉分、评测也全绿——只有真的去看
  "发出去的那几个字节"才发现。所以这里不测"pii.py 能不能脱"（那是它自己的事），
  只测三条边界：

    1. 送到 transport 的 messages 里没有原文；
    2. 写进 cost_ledger 的 prompt_head / note 里没有原文；
    3. 命中处数（pii_hits）能带回给上层，且 0 和 3 长得不一样。

做法是**打桩，不打网络、不写库**：

  · `llm._transport_chat` 换成捕获器——它是唯一真正发请求的地方，
    换掉它就等于"拦在出网口"看发了什么；
  · `store.insert_cost` 换成捕获器——`cost_ledger` 有不可变触发器，
    自测往里塞垃圾行就再也删不掉，所以断言"交给台账的那一行"，
    而 `insert_cost` 是逐列 `rec.get()` 落库的，交什么就写什么。

真实链路由 `deploy_smoke.py` 覆盖，这里只保证这一层的契约。

    python code/agent/tests/selftest_pii_gateway.py
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import copy
import json
import sys

import gateway
import llm
from conf import load_config
from governance import pii, store

OK = 0
FAIL = 0
FAILED: list[str] = []

# 三条假 PII：手机号 / 身份证 / 邮箱。假数据也得长得对，否则测的是假绿。
PHONE = "13800001111"
IDCARD = "110101199003071234"
EMAIL = "zhang.san@example.com"
RAW = (PHONE, IDCARD, EMAIL)

SENT: list[list[dict]] = []
LEDGER: list[dict] = []


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        FAILED.append(name)
        print("  ✗ %s %s" % (name, extra))


def _fake_transport(messages, cfg, **kw):
    """出网口的捕获器：记下真正会被发出去的东西，回一个合法响应。"""
    SENT.append(copy.deepcopy(messages))
    return {"content": "已收到", "reasoning": "",
            "usage": {"prompt_tokens": 120, "completion_tokens": 5},
            "elapsed_ms": 7, "attempts": 1, "tool_calls": None}


def _fake_insert_cost(rec, cfg=None):
    """台账的捕获器：记下将要落库的那一行（不写库）。"""
    LEDGER.append(dict(rec))
    return True, ""


def _ask(messages, cfg, **kw):
    SENT.clear()
    LEDGER.clear()
    return gateway.chat(messages, cfg, caller="selftest_pii",
                        max_tokens=16, **kw)


def _no_raw(text: str) -> bool:
    return not any(r in (text or "") for r in RAW)


def main() -> int:
    cfg = copy.deepcopy(load_config())
    # 走网关但不出网、不查额度：预算与限流不是这条自测的対象，
    # 留着它们只会让自测依赖"今天花了多少钱"。
    cfg.setdefault("gateway", {})
    cfg["gateway"]["daily_cost_cny"] = 0
    cfg["gateway"]["max_calls_per_min"] = 0

    llm._transport_chat = _fake_transport
    store.insert_cost = _fake_insert_cost

    print("PII 进模型前脱敏 —— %s" % gateway.__name__)

    # ---------------------------------------------------------------- 1 命中
    print("\n[1] 问句里带手机号 / 身份证 / 邮箱")
    q = ("用户手机 %s、身份证 %s 的播放记录，顺便看下 %s 这个账号的付费"
         % (PHONE, IDCARD, EMAIL))
    messages = [
        {"role": "system", "content": "你是数据问答助手，只输出 JSON。"},
        {"role": "user", "content": q},
    ]
    snapshot = copy.deepcopy(messages)          # 用它验"调用方的对象没被就地改"
    out = _ask(messages, cfg)

    ck("模型被调了一次（桩打对了）", len(SENT) == 1, "SENT=%d" % len(SENT))
    sent_text = json.dumps(SENT[0] if SENT else [], ensure_ascii=False)
    ck("送进 transport 的消息里没有原文",
       _no_raw(sent_text), "仍含：" + ",".join(r for r in RAW if r in sent_text))
    ck("transport 拿到的确实是脱敏后的问句",
       "[PHONE]" in sent_text and "[IDCARD]" in sent_text and "[EMAIL]" in sent_text,
       sent_text[:160])
    ck("pii_hits = 3", out.get("pii_hits") == 3, "got=%r" % out.get("pii_hits"))
    ck("脱敏后再没有可命中的 PII",
       pii.scan(sent_text) == [], str(pii.scan(sent_text)))

    # ---------------------------------------------------------------- 2 台账
    print("\n[2] 台账（交给 insert_cost 的那一行）")
    ck("写了一条台账", len(LEDGER) == 1, "LEDGER=%d" % len(LEDGER))
    rec = LEDGER[0] if LEDGER else {}
    head = rec.get("prompt_head") or ""
    ck("prompt_head 没有原文", _no_raw(head), head[:120])
    ck("prompt_head 是脱敏后的问句（不是空串）",
       "[PHONE]" in head and head.strip() != "", head[:120])
    ck("prompt_head 里也没有可命中的 PII", pii.scan(head) == [], str(pii.scan(head)))
    ck("台账其他文本字段也没有原文",
       _no_raw((rec.get("answer_head") or "") + (rec.get("note") or "")
               + (rec.get("error") or "")),
       str(rec.get("note"))[:120])
    ck("备注里写明 pii_hits=3", "pii_hits=3" in (rec.get("note") or ""),
       str(rec.get("note"))[:120])

    # ---------------------------------------------------------------- 3 不就地改
    print("\n[3] 调用方的 messages 不能被就地改写")
    ck("原 messages 仍是原文（审计/回放要对得上）",
       messages == snapshot, "被改了：%s" % json.dumps(messages, ensure_ascii=False)[:160])

    # ---------------------------------------------------------------- 4 反向
    print("\n[4] 反向：没有 PII 的问题不能被误伤")
    clean = "2026-11-08 的 DAU 是多少，比 11-07 多了多少"
    out2 = _ask([{"role": "user", "content": clean}], cfg)
    ck("pii_hits = 0", out2.get("pii_hits") == 0, "got=%r" % out2.get("pii_hits"))
    ck("正文一字未改（日期和数字还在）",
       SENT and SENT[0][-1]["content"] == clean,
       (SENT[0][-1]["content"] if SENT else "")[:120])
    ck("无命中时备注不塞空标记",
       "pii_hits" not in (LEDGER[0].get("note") or ""),
       str(LEDGER[0].get("note"))[:80])

    # ---------------------------------------------------------------- 5 多消息
    print("\n[5] system 里带的 PII 也要脱（不能只看最后一条 user）")
    out3 = _ask([
        {"role": "system", "content": "运营同事 %s 专用通道" % PHONE},
        {"role": "user", "content": "昨天的付费用户数"},
    ], cfg)
    ck("跨 message 累计命中 pii_hits = 1", out3.get("pii_hits") == 1,
       "got=%r" % out3.get("pii_hits"))
    ck("system 里的手机号也被脱了",
       _no_raw(json.dumps(SENT[0], ensure_ascii=False) if SENT else ""),
       (SENT[0][0]["content"] if SENT else "")[:120])

    # ---------------------------------------------------------------- 6 结构
    # 只单测 `_redact_messages`，不让它走完整 chat：网关的 `_estimate_tokens`
    # 目前假设 content 是 str（多模态 parts 会 TypeError），这是网关既有的边界，
    # 本项目所有节点都传字符串，不在 P0-3 里改它。这里验的是"脱敏不碰非字符串"。
    print("\n[6] 非字符串 content 不能被改坏（单测 _redact_messages）")
    parts = [{"role": "user",
              "content": [{"type": "text", "text": "图片见附件"}]}]
    red, n6 = gateway._redact_messages(parts)
    ck("多模态 parts 原样透传", red[0]["content"] == parts[0]["content"],
       str(red[0]["content"])[:120])
    ck("这种消息命中数 = 0", n6 == 0, "got=%r" % n6)
    ck("空消息列表不炸", gateway._redact_messages([]) == ([], 0))
    ck("content 为空的 message 原样保留",
       gateway._redact_messages([{"role": "user", "content": ""}])
       == ([{"role": "user", "content": ""}], 0))

    print("\n%d 通过 / %d 失败" % (OK, FAIL))
    if FAILED:
        print("失败项：" + "；".join(FAILED))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
