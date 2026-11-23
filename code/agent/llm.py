"""传输层：一次 DeepSeek 补全（OpenAI 兼容接口）。

**这个模块不再对外提供 `chat()`**。阶段 7 起，所有模型调用必须经
`gateway.chat()`：预算、限流、台账、出网都在网关那层做，这里只管把字节发出去。

为什么要真的改名而不是"约定别直接用"
----------------------------------
原来函数叫 `chat`、注释写着"想调模型只能走这里"。结果呢：只要有人
`from llm import chat` 就能绕过全部治理，而且**看起来完全正常**——没有报错、
没有日志、月底账单上多出一截。约定管不住人，接口才管得住。所以：
  · 对外入口只剩 `_transport_chat`，名字本身就写着"别调我"；
  · 函数开头有 `_assert_gateway()`，直连会当场抛 LLMError；
  · `selftest_*.py` 是唯一例外（它们测的就是传输层本身），
    而"生产代码里没有第二处引用"由 `selftest_gateway.py` 静态扫描兜底。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json
import sys
import time
from typing import Any

import httpx

import vault

# 429/5xx 值得重试；400 是我们自己 prompt 写错，重试只会重复烧钱
_RETRY_STATUS = {408, 409, 429, 500, 502, 503, 504}
_MAX_RETRY = 3


class LLMError(RuntimeError):
    """模型调用失败。调用方捕获后转成"生成失败"，不要让整张图崩掉。"""


class _Deadline(LLMError):
    """整通调用超过总时限。单独一类，因为它的处置方式不同：重试没意义。"""


def _post(client: httpx.Client, url: str, headers: dict, body: dict,
          budget: float) -> tuple[int, bytes]:
    """发一次请求，并给整通调用一个**墙钟**上限。

    为什么 httpx 的 `read` 超时不够
    ----------------------------
    `read` 算的是「两次读之间」的间隔，不是这次调用的总时长。服务端只要每
    几十秒吐一个字节（心跳、半死的连接都这样），就能合法地把一次调用拖成十分钟。
    实测撞到过一次：`generate` 卡了 607 秒，日志里没有任何超时记录，最后靠
    模型自己吐完才继续——用户侧看到的就是"卡住了"，而且没人能解释。

    所以这里自己算 deadline：超过就主动断开，抛 _Deadline。
    """
    deadline = time.monotonic() + budget
    with client.stream("POST", url, headers=headers, json=body) as r:
        buf = bytearray()
        for chunk in r.iter_bytes():
            buf.extend(chunk)
            if time.monotonic() > deadline:
                raise _Deadline("模型响应超过 %.0fs 仍未读完，已主动断开" % budget)
        return r.status_code, bytes(buf)


def endpoint(cfg: dict) -> str:
    return cfg["generator"]["base_url"].rstrip("/") + "/chat/completions"


def headers(cfg: dict) -> dict:
    # 全项目只有这一行真的拿到 key 明文，且它只活在这个 dict 里。
    # 没有它，请求发不出去；有它，任何打印这个 dict 的地方都会泄露——
    # 所以调用方一律不许回显 headers（gateway 台账只记 prompt，不记请求头）。
    key = vault.require(cfg["generator"]["api_key_env"]).reveal()
    return {
        "Authorization": "Bearer " + key,
        "Content-Type": "application/json",
    }


def _assert_gateway() -> None:
    """只有网关（和传输层自测）能调这个东西。

    这是围栏不是边界：改名/反射都能绕。它防的是"无意绕过"——那才是真实
    发生过的退化方式，而不是假想的攻击者。

    判据看**调用帧所属的文件**而不只看模块名：`python gateway.py` 时模块名
    是 __main__，光比名字会把网关自己拦掉（第一次就是这么挂的）。
    """
    import os
    try:
        g = sys._getframe(2).f_globals
    except ValueError:                       # 解释器底部，没有上层帧
        return
    name = g.get("__name__") or ""
    fname = os.path.basename(g.get("__file__") or "")
    if name == "gateway" or fname == "gateway.py":
        return
    if name.startswith("selftest_") or fname.startswith("selftest_"):
        return
    raise LLMError(
        "直连模型被拒：调用方是 %s。所有模型调用必须经 gateway.chat()"
        "（预算/限流/台账/出网都在那一层，绕过去等于没有治理）" % (fname or name or "?"))


def _transport_chat(
    messages: list[dict],
    cfg: dict,
    *,
    json_mode: bool = False,
    temperature: float | None = None,
    max_tokens: int | None = None,
    tools: list[dict] | None = None,
    tool_choice: str | dict | None = None,
) -> dict[str, Any]:
    """一次补全（**仅供 gateway 使用**）。

    返回 {content, usage, elapsed_ms, model, attempts, tool_calls}
    tool_calls 为空列表表示模型没有要调工具。
    失败抛 LLMError（含最后一次的错误原因）。
    """
    _assert_gateway()
    g = cfg["generator"]
    body: dict[str, Any] = {
        "model": g["model"],
        "messages": messages,
        "temperature": float(g["temperature"] if temperature is None else temperature),
        "max_tokens": int(max_tokens or g["max_tokens"]),
        "stream": False,
    }
    if tools:
        body["tools"] = tools
        if tool_choice:
            body["tool_choice"] = tool_choice
        # json_mode 与 tools 在 OpenAI 兼容接口上基本互斥（DeepSeek 会回 400）。
        # 调工具时本来就要模型自由组织 message，不需要强约束成 JSON。
        json_mode = False
    if json_mode:
        # DeepSeek 要求 prompt 里出现 "json" 字样，否则会返回 400
        body["response_format"] = {"type": "json_object"}

    timeout = httpx.Timeout(connect=10.0, read=float(g.get("timeout_sec", 120)), write=30.0, pool=10.0)
    # 总预算：一次 generate 里所有重试加起来不许超过它。
    # 默认 240s = 读超时的两倍，够一次重试，又不至于让用户盯着转圈十分钟。
    budget_left = float(g.get("total_budget_sec", 240))
    last_err = ""

    for attempt in range(1, _MAX_RETRY + 1):
        if budget_left <= 2:
            last_err = last_err or "总时限已用完"
            break
        t0 = time.perf_counter()
        try:
            with httpx.Client(timeout=timeout) as client:
                status, raw = _post(client, endpoint(cfg), headers(cfg), body,
                                    min(float(g.get("timeout_sec", 120)), budget_left))
            elapsed = int((time.perf_counter() - t0) * 1000)
            budget_left -= (time.perf_counter() - t0)
            text = raw.decode("utf-8", "replace")

            if status == 200:
                d = json.loads(text)
                msg = ((d.get("choices") or [{}])[0]).get("message", {}) or {}
                return {
                    "content": msg.get("content", "") or "",
                    # 工具调用要原样带回去：网关要在台账里记"这次调了哪个工具"，
                    # ReAct 节点要用它决定下一步。丢弃它等于把 function calling
                    # 退化成"模型说了一句话"。
                    "tool_calls": msg.get("tool_calls") or [],
                    "usage": d.get("usage") or {},
                    "elapsed_ms": elapsed,
                    "model": d.get("model") or g["model"],
                    "attempts": attempt,
                }

            detail = text[:300]

            # json_mode 不被支持时降级重试一次（不同模型/版本支持面不一样）
            if status == 400 and json_mode and "response_format" in detail:
                body.pop("response_format", None)
                json_mode = False
                last_err = "模型不支持 response_format=json_object，已降级为纯文本"
                continue

            last_err = "HTTP %d: %s" % (status, detail)
            if status not in _RETRY_STATUS:
                # 服务端回显里可能带请求头（部分网关会把 Authorization 回显到
                # 错误体里）。异常消息最终会进 trace、审计表、前端页面，
                # 所以在这里统一擦一遍——这是最后一道，也是唯一必经的一道。
                raise LLMError(vault.scrub(last_err))

        except _Deadline as e:
            # 直接抛出去，不要 break 后统一 raise：那样会退化成普通 LLMError，
            # 上层就分不清"模型说错了"和"模型根本没回完"。重试也没意义——
            # 墙钟都超了，再等一遍只是把用户的等待翻倍。
            raise e
        except httpx.HTTPError as e:
            last_err = "%s: %s" % (type(e).__name__, str(e)[:200])
            elapsed = int((time.perf_counter() - t0) * 1000)
            budget_left -= (time.perf_counter() - t0)

        if attempt < _MAX_RETRY:
            time.sleep(1.5 * attempt)      # 1.5s → 3s，别打爆限流

    raise LLMError(vault.scrub(last_err or "模型调用失败"))


def parse_json_object(content: str) -> dict:
    """从模型输出里挖出第一个 JSON 对象。

    三种脏情况都见过：
      · 纯 JSON
      · ```json 围栏
      · 前面一段"好的，我来分析…"再跟 JSON
    """
    text = (content or "").strip()

    if text.startswith("```"):
        # 去掉首尾围栏，保留中间
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 兜底：括号配平扫描，找到第一个完整对象
    start = text.find("{")
    if start < 0:
        raise LLMError("模型输出里没有 JSON 对象：%s" % text[:200])
    depth, in_str, esc = 0, False, False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    break
    raise LLMError("模型输出的 JSON 不完整：%s" % text[:200])


if __name__ == "__main__":
    # 自检也走网关：连"文件底部的 smoke 脚本"都不该是那条绕过治理的小路。
    import gateway
    from conf import load_config
    cfg = load_config()
    out = gateway.chat([{"role": "user", "content": "只回复两个字：接通"}],
                       cfg, caller="smoke", max_tokens=10)
    print(out["content"].strip(), "|", out["model"],
          "|", out["usage"].get("total_tokens"), "tokens",
          "| ¥", out.get("cost_cny"), "| ledger_ok", out.get("ledger_ok"))
