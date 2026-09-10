"""HTTP API 端到端集成测试：真实起服务 + urllib 请求（离线回环）。"""

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.request
import urllib.error

from cache_sim.server import build_server

BASE = None
COUNTER = [8201]


def call(method, path, body=None):
    url = f"http://127.0.0.1:{BASE[1]}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


class HttpApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.tmp.close()
        port = COUNTER[0]
        COUNTER[0] += 1
        cls.httpd = build_server("127.0.0.1", port, cls.tmp.name, quiet=True)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        global BASE
        BASE = [None, port]
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.httpd.store.close()
        os.unlink(cls.tmp.name)

    def test_full_flow(self):
        code, body = call("GET", "/health")
        self.assertEqual(code, 200)
        self.assertTrue(body["offline"])

        # 创建 + 事件
        code, body = call("POST", "/scenarios", {
            "name": "api-flow",
            "config": {"cache_mode": "shared"},
            "events": [
                {"id": "o1", "type": "origin", "at": 0, "url": "/a",
                 "etag": '"e"', "headers": {"cache-control": "max-age=100"}},
                {"id": "r1", "type": "request", "at": 1, "url": "/a"},
                {"id": "r2", "type": "request", "at": 2, "url": "/a"},
            ],
        })
        self.assertEqual(code, 201)
        sid = body["scenario"]["id"]
        self.assertEqual(body["events_added"], 3)

        # 列表 / 详情
        code, body = call("GET", "/scenarios")
        self.assertEqual(any(s["id"] == sid for s in body["scenarios"]), True)

        # 追加事件
        code, body = call("POST", f"/scenarios/{sid}/events",
                          {"event": {"id": "r3", "type": "request",
                                     "at": 3, "url": "/a"}})
        self.assertEqual(code, 201)

        # 运行
        code, run = call("POST", f"/scenarios/{sid}/run", {})
        self.assertEqual(code, 200)
        verdicts = [r["verdict"] for r in run["results"] if r["type"] == "request"]
        self.assertEqual(verdicts, ["MISS", "HIT", "HIT"])
        self.assertIn("trace", run["results"][1])

        # 快照
        code, snap = call("GET", f"/scenarios/{sid}/snapshot?trace=0")
        self.assertEqual(code, 200)
        self.assertEqual(snap["counters"]["origin_fetches"], 1)
        self.assertTrue(snap["cache"][0]["variants"][0]["fresh"])
        self.assertEqual(snap["results"][0].get("trace", "absent"), "absent")

        # 结果
        code, res = call("GET", f"/scenarios/{sid}/results")
        self.assertEqual(code, 200)
        self.assertEqual(len(res["results"]), 4)

        # 重置
        code, body = call("POST", f"/scenarios/{sid}/reset", {})
        self.assertEqual(code, 200)
        self.assertEqual(body["state"]["counters"]["requests"], 0)

        # 404 / 400
        code, body = call("GET", "/scenarios/nonexistent00")
        self.assertEqual(code, 404)
        code, body = call("POST", "/scenarios", {"events": [{"type": "bogus", "at": 1}]})
        self.assertEqual(code, 400)

    def test_clone_and_compare_over_http(self):
        code, body = call("POST", "/scenarios", {
            "name": "base",
            "events": [
                {"id": "o1", "type": "origin", "at": 0, "url": "/a",
                 "body": "x"},
                {"id": "r1", "type": "request", "at": 1, "url": "/a"},
                {"id": "r2", "type": "request", "at": 2, "url": "/a"},
            ],
        })
        sid = body["scenario"]["id"]
        call("POST", f"/scenarios/{sid}/run", {})

        code, body = call("POST", f"/scenarios/{sid}/clone",
                          {"name": "tuned", "config": {"default_cc": {"max-age": 60}}})
        cid = body["clone"]["id"]
        call("POST", f"/scenarios/{cid}/run", {})

        code, cmp = call("POST", "/scenarios/compare", {"ids": [sid, cid]})
        self.assertEqual(code, 200)
        row = cmp["comparisons"][0]
        self.assertEqual(row["delta_origin_fetches"], -1)
        self.assertFalse(row["same_verdict_sequence"])

    def test_run_until_partial(self):
        code, body = call("POST", "/scenarios", {
            "name": "partial",
            "events": [
                {"id": "o1", "type": "origin", "at": 0, "url": "/a",
                 "headers": {"cache-control": "max-age=100"}},
                {"id": "r1", "type": "request", "at": 10, "url": "/a"},
                {"id": "r2", "type": "request", "at": 20, "url": "/a"},
            ],
        })
        sid = body["scenario"]["id"]
        code, part = call("POST", f"/scenarios/{sid}/run", {"until": 15})
        self.assertEqual(part["events_run"], 2)
        code, rest = call("POST", f"/scenarios/{sid}/run", {})
        self.assertEqual(rest["events_run"], 1)
        self.assertEqual(rest["results"][0]["verdict"], "HIT")

    def test_swr_sie_and_jobs_over_http(self):
        # SWR：窗口内返回陈旧副本 + 后台作业随虚拟时钟结算 + 请求合并
        code, body = call("POST", "/scenarios", {
            "name": "swr-http",
            "events": [
                {"id": "o", "type": "origin", "at": 0, "url": "/feed",
                 "body": "feed-v1", "etag": '"f1"', "delay": 5,
                 "headers": {"cache-control":
                             "max-age=10, stale-while-revalidate=30, "
                             "stale-if-error=60"}},
                {"id": "q1", "type": "request", "at": 1, "url": "/feed"},
                {"id": "q2", "type": "request", "at": 20, "url": "/feed"},
                {"id": "q3", "type": "request", "at": 21, "url": "/feed"},
            ],
        })
        self.assertEqual(code, 201)
        sid = body["scenario"]["id"]
        code, run = call("POST", f"/scenarios/{sid}/run", {})
        verdicts = [r["verdict"] for r in run["results"] if r["type"] == "request"]
        self.assertEqual(verdicts,
                         ["MISS", "STALE_WHILE_REVALIDATE", "STALE_WHILE_REVALIDATE"])
        c = run["counters"]
        self.assertEqual(c["background_revalidations"], 1)
        self.assertEqual(c["coalesced_requests"], 1)
        self.assertEqual(c["origin_fetches_saved"], 1)

        # 快照：待结算作业可见（finish_at = 20 + delay 5 = 25）
        code, snap = call("GET", f"/scenarios/{sid}/snapshot?trace=0")
        self.assertEqual(code, 200)
        self.assertEqual(len(snap["pending_jobs"]), 1)
        self.assertEqual(snap["pending_jobs"][0]["finish_at"], 25)
        self.assertEqual(snap["pending_jobs"][0]["attached"], 1)
        self.assertEqual(snap["origins"]["/feed"]["delay"], 5)

        # 推进虚拟时钟：作业结算(304)，请求复用结果 HIT；job 结果出现在结果流
        code, body = call("POST", f"/scenarios/{sid}/events",
                          {"event": {"id": "q4", "type": "request",
                                     "at": 30, "url": "/feed"}})
        code, run = call("POST", f"/scenarios/{sid}/run", {})
        kinds = [(r["type"], r.get("verdict") or r.get("outcome"))
                 for r in run["results"]]
        self.assertIn(("job", "revalidated"), kinds)
        self.assertIn(("request", "HIT"), kinds)

        # SIE：源站 5xx 时按 stale-if-error 回退陈旧副本
        # （t=70 已超出 SWR 窗口(25+10+30=65)，但仍在 SIE 窗口(25+10+60=95)内）
        call("POST", f"/scenarios/{sid}/events",
             {"events": [
                 {"id": "boom", "type": "origin_change", "at": 35,
                  "url": "/feed", "status": 503},
                 {"id": "q5", "type": "request", "at": 70, "url": "/feed"},
             ]})
        code, run = call("POST", f"/scenarios/{sid}/run", {})
        q5 = [r for r in run["results"] if r.get("event_id") == "q5"][0]
        self.assertEqual(q5["verdict"], "STALE_IF_ERROR")
        self.assertEqual(q5["status"], 200)
        self.assertEqual(run["counters"]["stale_if_error"], 1)
        self.assertEqual(run["counters"]["origin_errors"], 1)

        # compare 输出包含新统计维度
        code, body = call("POST", f"/scenarios/{sid}/clone", {"name": "swr-copy"})
        cid = body["clone"]["id"]
        call("POST", f"/scenarios/{cid}/run", {})
        code, cmp = call("POST", "/scenarios/compare", {"ids": [sid, cid]})
        row = cmp["comparisons"][0]
        for key in ("delta_stale_served", "delta_stale_while_revalidate",
                    "delta_stale_if_error", "delta_background_revalidations",
                    "delta_coalesced_requests", "delta_origin_fetches_saved",
                    "delta_origin_errors"):
            self.assertIn(key, row)


if __name__ == "__main__":
    unittest.main(verbosity=2)
