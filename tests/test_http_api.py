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


if __name__ == "__main__":
    unittest.main(verbosity=2)
