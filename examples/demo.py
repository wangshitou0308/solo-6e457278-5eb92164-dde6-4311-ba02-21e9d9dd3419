"""端到端调用示例：先直接用 Store/Engine 演示核心场景，再演示 HTTP API 用法。

运行：
  python examples/demo.py            # 仅离线引擎演示（不需要启动服务）
  python examples/demo.py --http     # 同时调用 http://127.0.0.1:8000 的 HTTP API

--http 模式会自动在后台启动服务（用临时数据库），演示完自动关闭。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cache_sim.store import Store


def pprint(title, obj):
    print(f"\n=== {title} ===")
    print(json.dumps(obj, ensure_ascii=False, indent=2))


# --------------------------------------------------------------- 场景定义

def build_events():
    """一个完整的缓存策略验证序列：
    0s    定义源站 /article：max-age=5 + ETag（短 TTL 便于观察老化）
    1s    首次请求 -> MISS 回源
    3s    第二次请求 -> HIT
    10s   第三次请求 -> 过期，ETag 未变 -> 304 REVALIDATED
    75s   源站内容变更（新 ETag）；304 刷新的新鲜度此时早已老化
    80s   请求 -> 条件请求 ETag 不匹配 -> 200 REFRESHED
    200s  断网；请求一个 must-revalidate 的接口 -> 504
    201s  请求普通过期接口 -> STALE 兜底
    210s  手动清缓存
    220s  网络恢复后请求 -> 重新回源
    """
    return [
        # /article：max-age + ETag
        {"id": "o-article", "type": "origin", "at": 0, "url": "/article",
         "status": 200, "body": "v1-body", "etag": '"v1"',
         "headers": {"cache-control": "max-age=5", "content-type": "text/plain"}},
        {"id": "r1", "type": "request", "at": 1, "url": "/article"},
        {"id": "r2", "type": "request", "at": 3, "url": "/article"},
        {"id": "r3", "type": "request", "at": 10, "url": "/article"},
        {"id": "o-change", "type": "origin_change", "at": 75, "url": "/article",
         "body": "v2-body", "etag": '"v2"'},
        {"id": "r4", "type": "request", "at": 80, "url": "/article"},

        # /must：must-revalidate，断网过期必须 504
        {"id": "o-must", "type": "origin", "at": 0, "url": "/must",
         "body": "important", "etag": '"m1"',
         "headers": {"cache-control": "max-age=10, must-revalidate"}},
        {"id": "rm1", "type": "request", "at": 1, "url": "/must"},

        # /plain：普通可缓存，断网过期允许 STALE
        {"id": "o-plain", "type": "origin", "at": 0, "url": "/plain",
         "body": "plain-body", "etag": '"p1"',
         "headers": {"cache-control": "max-age=10"}},
        {"id": "rp1", "type": "request", "at": 1, "url": "/plain"},

        {"id": "net-down", "type": "network", "at": 200, "up": False},
        {"id": "rm2", "type": "request", "at": 200, "url": "/must"},
        {"id": "rp2", "type": "request", "at": 201, "url": "/plain"},
        {"id": "clear", "type": "clear", "at": 210},
        {"id": "net-up", "type": "network", "at": 215, "up": True},
        {"id": "rp3", "type": "request", "at": 220, "url": "/plain"},
    ]


# --------------------------------------------------------------- 离线演示

def demo_offline():
    db = tempfile.mktemp(suffix=".db")
    store = Store(db)
    try:
        sc = store.create_scenario("demo")
        sid = sc["id"]
        store.add_events(sid, build_events())

        # 分段运行：先跑到 3s，观察部分结果
        part = store.run(sid, until=3)
        pprint("运行到 t=3 的判定序列",
               [(r["at"], r.get("url"), r["verdict"])
                for r in part["results"] if r["type"] == "request"])

        # 继续运行剩余全部事件
        rest = store.run(sid)
        pprint("t>3 的判定序列",
               [(r["at"], r.get("url"), r["verdict"], f"status={r['status']}")
                for r in rest["results"] if r["type"] == "request"])

        pprint("最终计数器（回源次数/字节/命中…）", rest["counters"])

        # 看一次 304 重验证的完整判定轨迹
        r304 = next(r for r in store.get_state(sid)["results"]
                    if r.get("event_id") == "r3")
        pprint("r3 的判定轨迹（trace）", r304["trace"])

        # 快照接口 /snapshot 的字段在 HTTP 演示中展示

        # ---- 复制场景做 A/B 对比：同一序列，不同缓存规则 ----
        clone_b = store.clone_scenario(sid, "demo-no-etag-rule",
                                       {"default_cc": {"max-age": 30}})        # B 场景里给 /article 之外的资源也注入缺省指令的对比意义有限，
        # 这里另建一个纯无缓存头场景来对比 default_cc
        plain = store.create_scenario("ab-base")
        store.add_events(plain["id"], [
            {"id": "o", "type": "origin", "at": 0, "url": "/x", "body": "data"},
            {"id": "q1", "type": "request", "at": 1, "url": "/x"},
            {"id": "q2", "type": "request", "at": 2, "url": "/x"},
            {"id": "q3", "type": "request", "at": 3, "url": "/x"},
        ])
        store.run(plain["id"])
        tuned = store.clone_scenario(plain["id"], "ab-tuned",
                                     {"default_cc": {"max-age": 60}})
        store.run(tuned["id"])
        pprint("A/B 对比：base vs 注入 default max-age=60",
               {"base": store.get_state(plain["id"])["counters"],
                "tuned": store.get_state(tuned["id"])["counters"]})

        # ---- 陈旧容错（SWR/SIE）+ 请求合并 + 源站故障 ----
        swr = store.create_scenario("swr-sie-demo")
        store.add_events(swr["id"], [
            # /feed：max-age=10，SWR 窗口 30s，SIE 窗口 60s；源站响应延迟 5s
            {"id": "o", "type": "origin", "at": 0, "url": "/feed",
             "body": "feed-v1", "etag": '"f1"', "delay": 5,
             "headers": {"cache-control":
                         "max-age=10, stale-while-revalidate=30, stale-if-error=60"}},
            {"id": "q1", "type": "request", "at": 1, "url": "/feed"},
            # 陈旧 4s（SWR 窗口内）-> 直接返回陈旧副本，调度后台重验证（t=25 结算）
            {"id": "q2", "type": "request", "at": 20, "url": "/feed"},
            # 作业在途 -> 挂接合并，返回陈旧副本（节省一次回源）
            {"id": "q3", "type": "request", "at": 21, "url": "/feed"},
            # t=25 作业结算（304）-> 复用结果，HIT
            {"id": "q4", "type": "request", "at": 30, "url": "/feed"},
            # 源站开始返回 503
            {"id": "boom", "type": "origin_change", "at": 35,
             "url": "/feed", "status": 503},
            # 已超出 SWR 窗口（25+10+30=65）但在 SIE 窗口内 -> 回退陈旧副本
            {"id": "q5", "type": "request", "at": 70, "url": "/feed"},
            # 源站恢复
            {"id": "heal", "type": "origin_change", "at": 80,
             "url": "/feed", "status": 200},
            {"id": "q6", "type": "request", "at": 81, "url": "/feed"},
        ])
        out = store.run(swr["id"])
        pprint("SWR/SIE 判定序列（含后台作业结算）",
               [(r["at"], r.get("url") or r.get("job_id"),
                 r.get("verdict")
                 or (f"job:{r['outcome']}" if r["type"] == "job" else r["type"]))
                for r in out["results"]])
        pprint("SWR/SIE 计数器（后台重验证/合并/陈旧回退/节省回源）",
               {k: v for k, v in out["counters"].items()
                if k in ("background_revalidations", "jobs_settled",
                         "coalesced_requests", "stale_while_revalidate",
                         "stale_if_error", "origin_fetches_saved",
                         "origin_errors", "origin_fetches")})

        # 重置演示
        store.reset(sid)
        again = store.run(sid)
        pprint("reset 后重跑，回源次数恢复", again["counters"]["origin_fetches"])
        return sid
    finally:
        store.close()
        os.unlink(db)


# --------------------------------------------------------------- HTTP 演示

def http_call(method, path, body=None):
    url = f"http://127.0.0.1:8000{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"http_error": e.code, **json.loads(e.read())}


def demo_http():
    db = tempfile.mktemp(suffix=".db")
    proc = subprocess.Popen(
        [sys.executable, "-m", "cache_sim", "--port", "8000", "--db", db, "--quiet"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        time.sleep(0.8)
        print("\n" + "=" * 60)
        print("HTTP API 演示")
        print("=" * 60)

        print("\n1) GET /health ->", http_call("GET", "/health"))

        created = http_call("POST", "/scenarios", {
            "name": "http-demo",
            "config": {"cache_mode": "shared"},
            "events": build_events(),
        })
        sid = created["scenario"]["id"]
        print(f"2) POST /scenarios -> 场景 {sid}，事件 {created['events_added']} 个")

        run = http_call("POST", f"/scenarios/{sid}/run", {"until": 80})
        print("3) POST .../run {until:80} ->")
        for r in run["results"]:
            if r["type"] == "request":
                print(f"   t={r['at']:>4} {r['url']:<9} {r['verdict']:<12} "
                      f"status={r['status']} from_origin={r['from_origin']}")

        snap = http_call("GET", f"/scenarios/{sid}/snapshot?trace=0")
        cache_summary = [
            {"key": c["key"],
             "variants": [{"age": v["age"], "ttl": v["ttl"], "fresh": v["fresh"],
                           "etag": v["etag"]} for v in c["variants"]]}
            for c in snap["cache"]]
        pprint("4) GET .../snapshot（cache + counters 摘要）",
               {"virtual_time": snap["virtual_time"],
                "network_up": snap["network_up"],
                "cache": cache_summary, "counters": snap["counters"],
                "pending": [e["id"] for e in snap["pending_events"]]})

        http_call("POST", f"/scenarios/{sid}/run", {})
        # 副本：给所有缺少 Cache-Control 的响应注入 max-age=30。
        # 对比点：/article 等资源本身已带显式指令不受影响，但可以观察配置差异下的行为。
        clone = http_call("POST", f"/scenarios/{sid}/clone",
                          {"name": "http-demo-private",
                           "config": {"cache_mode": "private"}})
        cid = clone["clone"]["id"]
        http_call("POST", f"/scenarios/{cid}/run", {})
        cmp = http_call("GET", f"/scenarios/compare?ids={sid},{cid}")
        pprint("5a) 共享 vs 私有缓存模式对比（本场景无 private 响应，判定序列应一致）",
               cmp["comparisons"])

        # 5b) 真正有差异的 A/B：源站不发 Cache-Control
        base2 = http_call("POST", "/scenarios", {
            "name": "no-cc-base",
            "events": [
                {"id": "o", "type": "origin", "at": 0, "url": "/x", "body": "data"},
                {"id": "q1", "type": "request", "at": 1, "url": "/x"},
                {"id": "q2", "type": "request", "at": 2, "url": "/x"},
                {"id": "q3", "type": "request", "at": 3, "url": "/x"},
            ],
        })
        b2 = base2["scenario"]["id"]
        http_call("POST", f"/scenarios/{b2}/run", {})
        tuned2 = http_call("POST", f"/scenarios/{b2}/clone",
                           {"name": "no-cc-tuned",
                            "config": {"default_cc": {"max-age": 60}}})
        t2 = tuned2["clone"]["id"]
        http_call("POST", f"/scenarios/{t2}/run", {})
        cmp2 = http_call("POST", "/scenarios/compare", {"ids": [b2, t2]})
        pprint("5b) 无 Cache-Control：默认不缓存 vs 注入 max-age=60",
               {"base_counters": cmp2["items"][0]["counters"],
                "tuned_counters": cmp2["items"][1]["counters"],
                "comparison": cmp2["comparisons"][0]})

        http_call("POST", f"/scenarios/{sid}/reset", {})
        print("6) POST .../reset -> 执行状态已清空，事件序列保留")
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        os.unlink(db)


if __name__ == "__main__":
    demo_offline()
    if "--http" in sys.argv:
        demo_http()
    else:
        print("\n（加 --http 参数可同时演示 HTTP API；或先启动 python -m cache_sim 后用 curl）")
