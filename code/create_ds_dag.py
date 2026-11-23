"""在 DolphinScheduler 3.4.2 上创建离线 T+1 链路 DAG（9 节点）。

本机直连 http://192.168.128.160:12345/dolphinscheduler，用 Web API 创建：
  项目 music_offline → 工作流 music_offline_t1 → 上线 → 可选定时/手工触发。

设计：
  DS Worker 在 160，但 Hive/Spark 客户端与全部脚本在 161，
  因此每个节点的 rawScript 都是 `ssh root@WWC161 bash /root/offline/dag_t1.sh <step> ${dt}`，
  把逻辑收敛到一个文件里，避免两层引号转义。

依赖（与真实表依赖一致）：
  gen → wait → landing → {dwd03, dwd04, dwd11}
  dwd03 + dwd04 → dws05 → dws08_ads09 → sync_doris
  dwd11 是独立分支（新域表不进现有指标链），可并行执行。

用法：
  python code/create_ds_dag.py            # 创建 + 上线
  python code/create_ds_dag.py --layout   # 只修已上线工作流的画布坐标（不改任务/依赖）
  python code/create_ds_dag.py --update   # 按 NODES 更新线上定义（增删节点/改依赖/改全局参数）
  python code/create_ds_dag.py --schedule # 额外建每天 02:00 的定时（默认 OFFLINE 不自动跑）
  python code/create_ds_dag.py --online   # 把已建的定时调度发布上线
  python code/create_ds_dag.py --offline  # 把定时调度下线（保留定义，工作流仍可手动运行）
  python code/create_ds_dag.py --run      # 手工触发一次；强制 dt=2026-11-04 幂等重跑，不污染演示数据

画布布局：DS 3.4 的 locations 必须是「数组」而不是 map：
    [{"taskCode": 189..., "x": 120, "y": 300}, ...]
传 map 时后端解析失败，会把所有节点 fallback 成 (0,0) 叠在原点。

日期参数：工作流全局 dt 用内置 $[yyyy-MM-dd-1]（真 T+1）。
手工验证时用 startParams 覆盖成固定 2026-11-04，避免 gen 节点造出新的业务日期、
把「31 天终态」改成 32 天。
"""
import sys
import json
import urllib.request
import urllib.parse
import http.cookiejar

BASE = "http://192.168.128.160:12345/dolphinscheduler"
USER, PWD = "admin", "dolphinscheduler123"
PROJECT = "music_offline"
WF_NAME = "music_offline_t1"

# (节点名, dag_t1.sh 的 step, 前置节点名列表)
NODES = [
    ("gen_incremental", "gen", []),
    ("wait_flume", "wait", ["gen_incremental"]),
    ("landing_to_ods", "landing", ["wait_flume"]),
    ("dwd_play_end", "dwd03", ["landing_to_ods"]),
    ("dwd_play_start", "dwd04", ["landing_to_ods"]),
    ("dwd_domain_10tabs", "dwd11", ["landing_to_ods"]),
    ("dws_song_daily", "dws05", ["dwd_play_end", "dwd_play_start"]),
    ("dws_ads", "dws08_ads09", ["dws_song_daily"]),
    ("dws_ads_ext", "ext", ["dwd_domain_10tabs"]),
    ("sync_doris", "sync", ["dws_ads", "dws_ads_ext"]),
]

DESC = {
    "gen_incremental": "增量造数 100 万行写 Kafka（ODS 已有分区则跳过，幂等）",
    "wait_flume": "等 Flume 消费组 LAG=0，landing 就绪",
    "landing_to_ods": "text.gz 按 event_id 去重，按 JSON dt 写 ODS ORC",
    "dwd_play_end": "03 号脚本：播放结束明细（完播率分子）",
    "dwd_play_start": "04 号脚本：起播明细（完播率分母）",
    "dwd_domain_10tabs": "11 号脚本：10 张域表，与 03/04 合计覆盖 26 类事件",
    "dws_song_daily": "05 号脚本：歌曲日完播率",
    "dws_ads": "08+09 号脚本：用户/曲风/省份 DWS 与 5 张 ADS",
    "dws_ads_ext": "12 号脚本：3 张新 DWS + 2 张新 ADS（漏斗/搜索/收入）",
    "sync_doris": "10 号脚本：Hive ADS → Doris 7 张表全量刷新（DataArt 直查即生效）",
}

# 全局参数：真 T+1。手工触发时用 DEMO_DT 覆盖，见 --run。
GLOBAL_PARAMS = json.dumps([
    {"prop": "dt", "direct": "IN", "type": "VARCHAR",
     "value": "$[yyyy-MM-dd-1]"},
], ensure_ascii=False)
DEMO_DT = "2026-11-04"

_cj = http.cookiejar.CookieJar()
_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_cj))


def call(method, path, params=None, form=None, params_in_body=False):
    """params_in_body: 创建工作流的 JSON 参数很长，放 query 会触发 Jetty 414
    URI Too Long，改放 form body（Spring @RequestParam 会合并 query 与 form）。"""
    url = BASE + path
    if params and not params_in_body:
        url += "?" + urllib.parse.urlencode(params)
    data = None
    headers = {}
    payload = dict(form or {})
    if params_in_body:
        payload.update(params or {})
    if payload:
        data = urllib.parse.urlencode(payload).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with _opener.open(req, timeout=60) as r:
            body = r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print("!! HTTP %s %s -> %s" % (e.code, path, body[:300]))
        raise
    try:
        return json.loads(body)
    except Exception:
        return {"_raw": body[:300]}


def check(ret, what):
    if not (ret.get("success") or ret.get("code") == 0):
        print("!! %s 失败: %s" % (what, json.dumps(ret, ensure_ascii=False)[:500]))
        sys.exit(2)
    print("OK  %s" % what)
    return ret.get("data")


def login():
    return check(call("POST", "/login",
                      form={"userName": USER, "userPassword": PWD}), "登录")


def get_project():
    ret = call("GET", "/projects", {"pageSize": 50, "pageNo": 1})
    data = ret.get("data") or {}
    for p in data.get("totalList", []) or []:
        if p.get("name") == PROJECT:
            print("OK  项目已存在 code=%s" % p.get("code"))
            return p["code"]
    d = check(call("POST", "/projects",
                   {"projectName": PROJECT,
                    "description": "音乐 APP 离线数仓 T+1 链路（毕业设计）"}), "创建项目")
    return d["code"] if isinstance(d, dict) and "code" in d else d


def existing_workflow(pcode):
    ret = call("GET", "/projects/%s/workflow-definition/simple-list" % pcode)
    data = ret.get("data") or []
    for w in data:
        if w.get("name") == WF_NAME:
            return w.get("code") or w.get("workflowDefinitionCode")
    return None


# 画布坐标：(x, y)。
#   主链 y=280 一条水平线；col3 三个 DWD 上下分叉；
#   dwd_domain_10tabs → dws_ads_ext 是下方独立分支；
#   sync_doris 放在两条输入（y=280 / y=450）中间 y=365，收口对称、连线不穿节点。
LAYOUT = {
    "gen_incremental":    (120, 280),
    "wait_flume":         (380, 280),
    "landing_to_ods":     (640, 280),
    "dwd_play_end":       (900, 110),
    "dwd_play_start":     (900, 280),
    "dwd_domain_10tabs":  (900, 450),
    "dws_song_daily":    (1160, 280),
    "dws_ads":           (1420, 280),
    "dws_ads_ext":       (1160, 450),
    "sync_doris":        (1680, 365),
}


def build_locations(name_to_code):
    """DS 3.4 期望数组，字段名 taskCode（long）。"""
    return [{"taskCode": int(name_to_code[name]), "x": LAYOUT[name][0],
             "y": LAYOUT[name][1]}
            for name in LAYOUT if name in name_to_code]


def build_definitions(pcode, codes):
    defs, relations = [], []
    by_name = {name: c for (name, _, _), c in zip(NODES, codes)}

    for (name, step, pres), code in zip(NODES, codes):
        script = ("ssh -o StrictHostKeyChecking=no -o BatchMode=yes root@WWC161 "
                  "'bash /root/offline/dag_t1.sh %s ${dt}'" % step)
        defs.append({
            "code": code, "name": name, "version": 1,
            "description": DESC[name], "taskType": "SHELL",
            "taskParams": {
                "resourceList": [], "localParams": [], "rawScript": script,
                "dependence": {},
                "conditionResult": {"successNode": [], "failedNode": []},
                "waitStartTimeout": {},
            },
            "flag": "YES", "taskPriority": "MEDIUM", "workerGroup": "default",
            "failRetryTimes": 0, "failRetryInterval": 1,
            "timeoutFlag": "CLOSE", "timeoutNotifyStrategy": "", "timeout": 0,
            "delayTime": 0, "environmentCode": -1,
            "taskGroupId": 0, "taskGroupPriority": 0,
            "cpuQuota": -1, "memoryMax": -1,
        })
        if not pres:
            relations.append({
                "name": "", "preTaskCode": 0, "preTaskVersion": 0,
                "postTaskCode": code, "postTaskVersion": 1,
                "conditionType": "NONE", "conditionParams": "{}",
            })
        for pre in pres:
            relations.append({
                "name": "", "preTaskCode": by_name[pre], "preTaskVersion": 1,
                "postTaskCode": code, "postTaskVersion": 1,
                "conditionType": "NONE", "conditionParams": "{}",
            })
    return defs, relations, build_locations(by_name)


def fix_layout(pcode, wf):
    """只更新画布坐标：读现有定义 → 原样回传任务/依赖 → 换正确的 locations 数组。

    DS 3.4 的 update 是「整体替换」，所以任务和依赖必须一起带上，
    否则会把节点或连线删掉。额外字段（id/createTime/...）清掉，避免校验失败。
    """
    ret = call("GET", "/projects/%s/workflow-definition/%s" % (pcode, wf))
    d = (ret or {}).get("data") or {}
    w, tasks, rels = (d.get("workflowDefinition") or {},
                      d.get("taskDefinitionList") or [],
                      d.get("workflowTaskRelationList") or [])
    if not tasks:
        print("!! 读取工作流详情失败")
        sys.exit(2)

    keep = ("code", "name", "version", "description", "taskType", "taskParams",
            "flag", "taskPriority", "workerGroup", "failRetryTimes",
            "failRetryInterval", "timeoutFlag", "timeoutNotifyStrategy",
            "timeout", "delayTime", "environmentCode", "taskGroupId",
            "taskGroupPriority", "cpuQuota", "memoryMax")
    clean_tasks = [{k: t[k] for k in keep if k in t} for t in tasks]
    clean_rels = [{
        "name": "", "preTaskCode": r.get("preTaskCode", 0),
        "preTaskVersion": r.get("preTaskVersion", 0),
        "postTaskCode": r["postTaskCode"],
        "postTaskVersion": r.get("postTaskVersion", 1),
        "conditionType": r.get("conditionType") or "NONE",
        "conditionParams": json.dumps(r.get("conditionParams") or {}),
    } for r in rels]

    name_to_code = {t["name"]: t["code"] for t in tasks}
    loc = build_locations(name_to_code)
    params = {
        "name": w.get("name") or WF_NAME,
        "description": w.get("description") or "",
        "globalParams": w.get("globalParams") or "[]",
        "locations": json.dumps(loc),
        "taskDefinitionJson": json.dumps(clean_tasks, ensure_ascii=False),
        "taskRelationJson": json.dumps(clean_rels, ensure_ascii=False),
        "executionType": "PARALLEL", "timeout": 0,
    }
    check(call("PUT", "/projects/%s/workflow-definition/%s" % (pcode, wf),
               params, params_in_body=True), "更新画布布局")
    print("    9 个节点坐标：%s" % ", ".join(
        "%s(%d,%d)" % (n, LAYOUT[n][0], LAYOUT[n][1]) for n in LAYOUT))

    # 回读校验，确认后端确实存下了坐标而不是又 fallback 到 (0,0)
    got = (call("GET", "/projects/%s/workflow-definition/%s" % (pcode, wf))
           .get("data") or {}).get("workflowDefinition") or {}
    saved = json.loads(got.get("locations") or "[]")
    bad = [x for x in saved if (x.get("x"), x.get("y")) == (0, 0)]
    if len(saved) != len(NODES) or bad:
        print("!! 坐标回读异常：%s" % json.dumps(saved, ensure_ascii=False)[:400])
        sys.exit(2)
    print("OK  回读校验：%d 个节点坐标全部有效，无 (0,0)" % len(saved))


def update_definition(pcode, wf):
    """按 NODES 重建线上工作流定义（增删节点 / 改依赖 / 改全局参数）。

    保留已有节点的 code，只给新增节点申请新 code —— 否则历史实例与任务定义会断链。
    DS 的 PUT 是整体替换，所以任务、依赖、坐标必须一次性全部给出。
    """
    # DS 不允许编辑 ONLINE 的工作流（"does not allow edit"），先下线。
    # 末尾 main 会统一 release ONLINE，这里只负责临时下线。
    check(call("POST", "/projects/%s/workflow-definition/%s/release" % (pcode, wf),
               {"releaseState": "OFFLINE", "name": WF_NAME}), "临时下线以便编辑")

    ret = call("GET", "/projects/%s/workflow-definition/%s" % (pcode, wf))
    d = (ret or {}).get("data") or {}
    w = d.get("workflowDefinition") or {}
    old_tasks = d.get("taskDefinitionList") or []
    name_to_code = {t["name"]: int(t["code"]) for t in old_tasks}

    missing = [n[0] for n in NODES if n[0] not in name_to_code]
    if missing:
        new_codes = call("GET",
                         "/projects/%s/task-definition/gen-task-codes" % pcode,
                         {"genNum": len(missing)}).get("data") or []
        if len(new_codes) < len(missing):
            print("!! 申请 task code 失败")
            sys.exit(2)
        for n, c in zip(missing, new_codes):
            name_to_code[n] = int(c)
        print("    新增节点: %s" % ", ".join(
            "%s=%s" % (n, name_to_code[n]) for n in missing))
    print("    保留节点: %d 个（code 不变）" % len(old_tasks))

    codes = [name_to_code[n[0]] for n in NODES]
    defs, rels, loc = build_definitions(pcode, codes)
    params = {
        "name": WF_NAME,
        "description": (w.get("description")
                        or "离线 T+1 全链路：造数→Kafka→Flume→landing→ODS→DWD→DWS→ADS→Doris"),
        "globalParams": GLOBAL_PARAMS,
        "locations": json.dumps(loc),
        "taskDefinitionJson": json.dumps(defs, ensure_ascii=False),
        "taskRelationJson": json.dumps(rels, ensure_ascii=False),
        "executionType": "PARALLEL", "timeout": 0,
    }
    check(call("PUT", "/projects/%s/workflow-definition/%s" % (pcode, wf),
               params, params_in_body=True),
          "更新工作流定义（%d 节点）" % len(NODES))

    # 回读校验：节点数、依赖边数、坐标有效性、dt 是否已是内置参数
    got = (call("GET", "/projects/%s/workflow-definition/%s" % (pcode, wf))
           .get("data") or {})
    gw = got.get("workflowDefinition") or {}
    tasks = got.get("taskDefinitionList") or []
    relations = got.get("workflowTaskRelationList") or []
    saved = json.loads(gw.get("locations") or "[]")
    expect_rel = sum(len(pres) for _, _, pres in NODES) + 1
    bad = [x for x in saved if (x.get("x"), x.get("y")) == (0, 0)]
    if (len(tasks) != len(NODES) or len(relations) != expect_rel
            or len(saved) != len(NODES) or bad
            or "$[yyyy-MM-dd-1]" not in (gw.get("globalParams") or "")):
        print("!! 回读异常: tasks=%d (期望%d) rels=%d (期望%d) locs=%d bad=%d gp=%s"
              % (len(tasks), len(NODES), len(relations), expect_rel,
                 len(saved), len(bad), gw.get("globalParams")))
        sys.exit(2)
    print("OK  回读校验：%d 节点 / %d 依赖边 / %d 坐标 / dt=$[yyyy-MM-dd-1]"
          % (len(tasks), len(relations), len(saved)))


def list_schedules(pcode, wf):
    ret = call("GET", "/projects/%s/schedules" % pcode,
               {"workflowDefinitionCode": wf, "pageNo": 1, "pageSize": 20})
    return ((ret or {}).get("data") or {}).get("totalList") or []


def online_schedule(pcode, wf):
    """把已创建的每日 02:00 调度发布上线。"""
    lst = list_schedules(pcode, wf)
    if not lst:
        print("!! 未找到调度定义，先跑 --schedule 创建")
        sys.exit(2)
    sid = lst[0].get("id")
    check(call("POST", "/projects/%s/schedules/%s/online" % (pcode, sid)),
          "上线定时调度（id=%s）" % sid)
    print("    crontab=%s  timezone=%s" %
          (lst[0].get("crontab"), lst[0].get("timezoneId")))


def offline_schedule(pcode, wf):
    """下线定时调度：只关 trigger，保留调度定义，工作流仍 ONLINE 可手动运行。

    必须走 API，不能直接 UPDATE t_ds_schedules —— DS 的 offline 会同时把
    Quartz 中已注册的 job 删掉；只改库表的话 Master 内存里的 trigger 还在。
    """
    lst = list_schedules(pcode, wf)
    if not lst:
        print("!! 未找到调度定义（本来就没有自动调度）")
        return
    for s in lst:
        sid = s.get("id")
        state = str(s.get("releaseState", "")).upper()
        if state in ("OFFLINE", "0"):
            print("OK  调度 id=%s 已是 OFFLINE" % sid)
        else:
            check(call("POST", "/projects/%s/schedules/%s/offline" % (pcode, sid)),
                  "下线定时调度（id=%s）" % sid)
        print("    crontab=%s  timezone=%s  原状态=%s" %
              (s.get("crontab"), s.get("timezoneId"), s.get("releaseState")))

    bad = [x for x in list_schedules(pcode, wf)
           if str(x.get("releaseState", "")).upper() not in ("OFFLINE", "0")]
    if bad:
        print("!! 回读校验失败，仍有上线中的调度: %s" %
              json.dumps([{"id": x.get("id"), "releaseState": x.get("releaseState")}
                          for x in bad], ensure_ascii=False))
        sys.exit(2)
    print("OK  回读校验：%d 条调度全部 OFFLINE，后续不会自动触发" % len(lst))


def main():
    do_schedule = "--schedule" in sys.argv
    do_run = "--run" in sys.argv
    do_layout = "--layout" in sys.argv
    do_update = "--update" in sys.argv
    do_online = "--online" in sys.argv
    do_offline = "--offline" in sys.argv
    if do_online and do_offline:
        print("!! --online 与 --offline 不能同时使用")
        sys.exit(2)

    login()
    pcode = get_project()
    if not isinstance(pcode, int):
        pcode = int(pcode)

    wf = existing_workflow(pcode)
    if wf:
        print("OK  工作流已存在 code=%s，跳过创建" % wf)
    else:
        codes = call("GET", "/projects/%s/task-definition/gen-task-codes" % pcode,
                     {"genNum": len(NODES)}).get("data")
        if not codes or len(codes) < len(NODES):
            print("!! 取 task code 失败")
            sys.exit(2)
        codes = [int(c) for c in codes[:len(NODES)]]
        print("OK  task codes: %s" % codes)
        defs, rels, loc = build_definitions(pcode, codes)
        params = {
            "name": WF_NAME,
            "description": "离线 T+1 全链路：造数→Kafka→Flume→landing→ODS→DWD→DWS→ADS→Doris",
            "globalParams": GLOBAL_PARAMS,
            "locations": json.dumps(loc),
            "taskDefinitionJson": json.dumps(defs, ensure_ascii=False),
            "taskRelationJson": json.dumps(rels, ensure_ascii=False),
            "executionType": "PARALLEL", "timeout": 0,
        }
        d = check(call("POST", "/projects/%s/workflow-definition" % pcode,
                       params, params_in_body=True), "创建工作流（9 节点）")
        wf = (d or {}).get("code") if isinstance(d, dict) else None
        if not wf:
            wf = existing_workflow(pcode)
        print("    工作流 code=%s" % wf)

    if do_layout:
        fix_layout(pcode, wf)
    if do_update:
        update_definition(pcode, wf)

    check(call("POST", "/projects/%s/workflow-definition/%s/release" % (pcode, wf),
               {"releaseState": "ONLINE", "name": WF_NAME}), "上线工作流")

    if do_schedule:
        schedule = {
            "startTime": "2026-11-08 00:00:00", "endTime": "2027-12-31 00:00:00",
            "crontab": "0 0 2 * * ? *", "timezoneId": "Asia/Shanghai",
            "failureStrategy": "CONTINUE", "warningType": "NONE",
            "workflowInstancePriority": "MEDIUM", "workerGroup": "default",
            "environmentCode": -1,
        }
        check(call("POST", "/projects/%s/schedules" % pcode,
                   {"workflowDefinitionCode": wf,
                    "schedule": json.dumps(schedule, ensure_ascii=False)}),
              "创建每日 02:00 调度")

    if do_online:
        online_schedule(pcode, wf)
    if do_offline:
        offline_schedule(pcode, wf)

    if do_run:
        # 覆盖全局参数：用固定演示日重跑，gen 节点会因 ODS 已有分区而跳过，
        # 不会造出新业务日期、把 31 天终态改成 32 天。
        print("    手工验证强制 dt=%s（startParams 覆盖）" % DEMO_DT)
        check(call("POST", "/projects/%s/executors/start-workflow-instance" % pcode,
                   {"workflowDefinitionCode": wf, "scheduleTime": "",
                    "failureStrategy": "CONTINUE", "warningType": "NONE",
                    "workflowInstancePriority": "MEDIUM",
                    "taskDependType": "TASK_POST", "workerGroup": "default",
                    "execType": "START_PROCESS",
                    "startParams": json.dumps({"dt": DEMO_DT})}),
              "手工触发工作流")

    print("\n=== DONE ===")
    print("项目 code=%s  工作流 code=%s" % (pcode, wf))
    print("访问: %s/ui/#/projects/%s/workflow/definitions" % (BASE, pcode))


if __name__ == "__main__":
    main()
