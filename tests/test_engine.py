"""缓存仿真引擎 + SQLite 存储的离线单元测试（标准库 unittest）。

运行：python -m unittest discover -s tests -v
"""

import copy
import os
import tempfile
import unittest

from cache_sim.engine import (Engine, DEFAULT_CONFIG, new_state,
                              HIT, MISS, REVALIDATED, REFRESHED,
                              NOT_MODIFIED, UNCACHEABLE, STALE, ERROR,
                              RANGE_HIT, RANGE_FILL, UNSATISFIABLE,
                              parse_range_spec, resolve_range, from_b64)
from cache_sim.store import Store


def run_events(config, events):
    eng = Engine(config)
    results = []
    for ev in events:
        r = eng.apply_event(ev)
        if r.get("type") == "request":
            results.append(r)
    return eng, results


def req(eid, at, url="/a", headers=None):
    return {"id": eid, "type": "request", "at": at, "url": url,
            "headers": headers or {}}


def origin(eid, at, url="/a", body="hello", **kw):
    return {"id": eid, "type": "origin", "at": at, "url": url,
            "body": body, **kw}


class BasicFreshnessTests(unittest.TestCase):
    def test_max_age_hit_then_stale_revalidate(self):
        ev = [
            origin("o1", 0, etag='"v1"', headers={"cache-control": "max-age=100"}),
            req("r1", 10),                    # 回源 MISS
            req("r2", 90),                    # HIT
            req("r3", 120),                   # 陈旧 -> 304
        ]
        eng, res = run_events({}, ev)
        self.assertEqual([r["verdict"] for r in res], [MISS, HIT, REVALIDATED])
        self.assertTrue(res[0]["from_origin"])
        self.assertFalse(res[1]["from_origin"])
        self.assertTrue(res[2]["from_origin"])
        self.assertEqual(res[2]["status"], 200)
        c = eng.state["counters"]
        self.assertEqual(c["origin_fetches"], 2)   # r1 + r3
        self.assertEqual(c["revalidations"], 1)
        self.assertEqual(c["not_modified"], 1)
        self.assertEqual(c["hits"], 2)            # HIT + REVALIDATED

    def test_expires_header(self):
        ev = [
            origin("o1", 0, headers={"expires": "100"}),
            req("r1", 10),
            req("r2", 50),
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[0]["verdict"], MISS)
        self.assertEqual(res[1]["verdict"], HIT)

    def test_s_maxage_shared(self):
        cfg = {"cache_mode": "shared"}
        ev = [
            origin("o1", 0, etag='"e"',
                   headers={"cache-control": "s-maxage=5, max-age=100"}),
            req("r1", 1),
            req("r2", 10),   # s-maxage=5 已过期 -> 重验证
        ]
        _, res = run_events(cfg, ev)
        self.assertEqual(res[0]["verdict"], MISS)
        self.assertEqual(res[1]["verdict"], REVALIDATED)

    def test_s_maxage_ignored_private_mode(self):
        cfg = {"cache_mode": "private"}
        ev = [
            origin("o1", 0, etag='"e"',
                   headers={"cache-control": "s-maxage=5, max-age=100"}),
            req("r1", 1),
            req("r2", 10),   # 私有模式忽略 s-maxage，max-age=100 仍新鲜
        ]
        _, res = run_events(cfg, ev)
        self.assertEqual(res[1]["verdict"], HIT)


class DirectiveTests(unittest.TestCase):
    def test_no_store_response(self):
        ev = [
            origin("o1", 0, headers={"cache-control": "no-store"}),
            req("r1", 1),
            req("r2", 2),
        ]
        eng, res = run_events({}, ev)
        self.assertEqual(res[0]["verdict"], UNCACHEABLE)
        self.assertEqual(res[1]["verdict"], UNCACHEABLE)
        self.assertEqual(eng.state["counters"]["origin_fetches"], 2)
        self.assertEqual(eng.state["cache"], {})

    def test_no_cache_response_always_revalidates(self):
        ev = [
            origin("o1", 0, etag='"v1"',
                   headers={"cache-control": "no-cache"}),
            req("r1", 1),
            req("r2", 2),   # 有条目但必须重验证 -> 304
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[0]["verdict"], MISS)
        self.assertEqual(res[1]["verdict"], REVALIDATED)
        codes = [t["code"] for t in res[1]["trace"]]
        self.assertIn("STALE_NO_CACHE", codes)

    def test_private_shared_vs_private_mode(self):
        ev_shared = [
            origin("o1", 0, headers={"cache-control": "private, max-age=100"}),
            req("r1", 1),
        ]
        _, res = run_events({"cache_mode": "shared"}, ev_shared)
        self.assertEqual(res[0]["verdict"], UNCACHEABLE)

        _, res = run_events({"cache_mode": "private"}, copy.deepcopy(ev_shared))
        self.assertEqual(res[0]["verdict"], MISS)

    def test_no_explicit_not_cacheable_by_default(self):
        ev = [origin("o1", 0), req("r1", 1), req("r2", 2)]
        _, res = run_events({}, ev)
        self.assertEqual(res[0]["verdict"], UNCACHEABLE)
        self.assertEqual(res[1]["verdict"], UNCACHEABLE)

    def test_heuristic_caching(self):
        # 资源 Last-Modified=0，当前 100，启发式 10% = 10s
        ev = [
            origin("o1", 100, last_modified="0"),
            req("r1", 100),
            req("r2", 105),   # age=5 <= 10 HIT
            req("r3", 111),   # 陈旧重验证
        ]
        cfg = {"heuristic_cache": True, "heuristic_ratio": 0.1}
        _, res = run_events(cfg, ev)
        self.assertEqual([r["verdict"] for r in res], [MISS, HIT, REVALIDATED])


class RevalidationTests(unittest.TestCase):
    def test_etag_change_returns_full_response(self):
        ev = [
            origin("o1", 0, body="v1", etag='"v1"',
                   headers={"cache-control": "max-age=10"}),
            req("r1", 1),
            {"id": "c1", "type": "origin_change", "at": 20,
             "url": "/a", "body": "v2-content", "etag": '"v2"'},
            req("r2", 21),   # 陈旧 -> ETag 不匹配 -> 200 新内容
        ]
        eng, res = run_events({}, ev)
        self.assertEqual(res[0]["verdict"], MISS)
        self.assertEqual(res[1]["verdict"], REFRESHED)
        self.assertEqual(res[1]["response_body"], "v2-content")
        self.assertEqual(res[1]["status"], 200)

    def test_last_modified_304_then_200(self):
        ev = [
            origin("o1", 0, last_modified="0",
                   headers={"cache-control": "max-age=10"}),
            req("r1", 1),
            req("r2", 20),                     # IMS=0 < LM? LM=0 未改 -> 304
            {"id": "c1", "type": "origin_change", "at": 30,
             "url": "/a", "body": "new", "last_modified": "30"},
            req("r3", 31),                     # IMS=0 < 30 -> 200
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], REVALIDATED)
        self.assertEqual(res[2]["verdict"], REFRESHED)
        self.assertEqual(res[2]["response_body"], "new")

    def test_client_conditional_gets_304_directly(self):
        ev = [
            origin("o1", 0, body="x", etag='"v1"',
                   headers={"cache-control": "max-age=1000"}),
            req("r1", 1),
            req("r2", 2, headers={"If-None-Match": '"v1"'}),  # 直通 304
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], NOT_MODIFIED)
        self.assertEqual(res[1]["status"], 304)

    def test_weak_etag_match(self):
        from cache_sim.engine import _etag_weak_equal
        self.assertTrue(_etag_weak_equal('W/"x"', '"x"'))
        self.assertTrue(_etag_weak_equal('"x"', 'W/"x"'))
        self.assertFalse(_etag_weak_equal('"x"', '"y"'))


class FailurePathTests(unittest.TestCase):
    def test_must_revalidate_offline_504(self):
        ev = [
            origin("o1", 0, etag='"e"',
                   headers={"cache-control": "max-age=10, must-revalidate"}),
            req("r1", 1),
            {"id": "n1", "type": "network", "at": 20, "up": False},
            req("r2", 21),   # 陈旧 + must-revalidate + 断网 -> 504
        ]
        eng, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], ERROR)
        self.assertEqual(res[1]["status"], 504)
        self.assertEqual(eng.state["counters"]["errors"], 1)
        self.assertEqual(eng.state["counters"]["origin_fetches"], 1)

    def test_stale_offline_served_with_warning(self):
        ev = [
            origin("o1", 0, body="old", etag='"e"',
                   headers={"cache-control": "max-age=10"}),
            req("r1", 1),
            {"id": "n1", "type": "network", "at": 20, "up": False},
            req("r2", 21),   # 普通陈旧 -> 断网继续提供
        ]
        eng, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], STALE)
        self.assertEqual(res[1]["response_body"], "old")
        self.assertIn("warning", res[1]["response_headers"])
        self.assertEqual(eng.state["counters"]["stale_served"], 1)

    def test_no_cache_offline_504(self):
        ev = [
            origin("o1", 0, etag='"e"',
                   headers={"cache-control": "no-cache"}),
            req("r1", 1),
            {"id": "n1", "type": "network", "at": 5, "up": False},
            req("r2", 6),
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["status"], 504)

    def test_offline_no_cache_502_online_after_reconnect(self):
        ev = [
            origin("o1", 0, headers={"cache-control": "no-store"}),
            {"id": "n1", "type": "network", "at": 1, "up": False},
            req("r1", 2),                 # 无缓存断网 -> 504
            {"id": "n2", "type": "network", "at": 3, "up": True},
            req("r2", 4),                 # 回源成功但 no-store
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[0]["status"], 504)
        self.assertEqual(res[1]["verdict"], UNCACHEABLE)

    def test_proxy_revalidate_shared_offline_504(self):
        ev = [
            origin("o1", 0, etag='"e"',
                   headers={"cache-control": "max-age=10, proxy-revalidate"}),
            req("r1", 1),
            {"id": "n1", "type": "network", "at": 20, "up": False},
            req("r2", 21),
        ]
        _, res = run_events({"cache_mode": "shared"}, ev)
        self.assertEqual(res[1]["status"], 504)

    def test_proxy_revalidate_ignored_private_mode(self):
        ev = [
            origin("o1", 0, etag='"e"',
                   headers={"cache-control": "max-age=10, proxy-revalidate"}),
            req("r1", 1),
            {"id": "n1", "type": "network", "at": 20, "up": False},
            req("r2", 21),
        ]
        _, res = run_events({"cache_mode": "private"}, ev)
        self.assertEqual(res[1]["verdict"], STALE)

    def test_origin_missing_502(self):
        _, res = run_events({}, [req("r1", 1, "/missing")])
        self.assertEqual(res[0]["verdict"], ERROR)
        self.assertEqual(res[0]["status"], 502)


class RequestCcTests(unittest.TestCase):
    def _setup(self):
        return [
            origin("o1", 0, etag='"e"',
                   headers={"cache-control": "max-age=100"}),
            req("r1", 1),
        ]

    def test_request_no_cache_forces_revalidation(self):
        ev = self._setup() + [req("r2", 2, headers={"Cache-Control": "no-cache"})]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], REVALIDATED)

    def test_request_no_store(self):
        ev = [origin("o1", 0, headers={"cache-control": "max-age=100"}),
              req("r1", 1, headers={"Cache-Control": "no-store"}),
              req("r2", 2)]
        _, res = run_events({}, ev)
        self.assertEqual(res[0]["verdict"], UNCACHEABLE)
        self.assertEqual(res[1]["verdict"], MISS)

    def test_max_stale_tolerates_stale(self):
        ev = self._setup() + [
            req("r2", 105, headers={"Cache-Control": "max-stale=10"})]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], HIT)

    def test_max_age_zero_forces_revalidate(self):
        ev = self._setup() + [
            req("r2", 5, headers={"Cache-Control": "max-age=0"})]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], REVALIDATED)

    def test_min_fresh(self):
        # age=90, ttl=100，剩 10；min-fresh=20 不满足 -> 重验证
        ev = self._setup() + [
            req("r2", 90, headers={"Cache-Control": "min-fresh=20"})]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], REVALIDATED)

    def test_only_if_cached_offline_hit(self):
        ev = self._setup() + [
            {"id": "n1", "type": "network", "at": 20, "up": False},
            req("r2", 21, headers={"Cache-Control": "only-if-cached"})]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], HIT)
        self.assertTrue(res[1]["response_headers"]["age"] != "")

    def test_only_if_cached_without_entry_504(self):
        ev = [{"id": "n1", "type": "network", "at": 0, "up": False},
              req("r1", 1, headers={"Cache-Control": "only-if-cached"})]
        _, res = run_events({}, ev)
        self.assertEqual(res[0]["status"], 504)


class VaryTests(unittest.TestCase):
    def test_vary_two_variants(self):
        ev = [
            origin("o1", 0, body="gzip" * 10,
                   headers={"cache-control": "max-age=100", "vary": "Accept-Encoding",
                            "x-variant": "gzip"},
                   etag='"g"'),
            # 两个请求头不同的初始变体
            {"id": "r1", "type": "request", "at": 1, "url": "/a",
             "headers": {"Accept-Encoding": "gzip"}},
            {"id": "r2", "type": "request", "at": 2, "url": "/a",
             "headers": {"Accept-Encoding": "identity"}},
            # 各自命中自己的变体
            {"id": "r3", "type": "request", "at": 3, "url": "/a",
             "headers": {"Accept-Encoding": "gzip"}},
            {"id": "r4", "type": "request", "at": 4, "url": "/a",
             "headers": {"Accept-Encoding": "identity"}},
        ]
        eng, res = run_events({}, ev)
        self.assertEqual([r["verdict"] for r in res],
                         [MISS, MISS, HIT, HIT])
        bucket = eng.state["cache"]["GET /a"]
        self.assertEqual(bucket["vary"], ["accept-encoding"])
        self.assertEqual(len(bucket["variants"]), 2)

    def test_vary_star_uncacheable(self):
        ev = [origin("o1", 0, headers={"cache-control": "max-age=100", "vary": "*"}),
              req("r1", 1)]
        _, res = run_events({}, ev)
        self.assertEqual(res[0]["verdict"], UNCACHEABLE)


class ClearAndControlTests(unittest.TestCase):
    def test_clear_all_and_clear_url(self):
        ev = [
            origin("o1", 0, url="/a", headers={"cache-control": "max-age=100"}),
            origin("o2", 0, url="/b", headers={"cache-control": "max-age=100"}),
            req("ra1", 1, "/a"), req("rb1", 1, "/b"),
            {"id": "x1", "type": "clear", "at": 2, "url": "/a"},
            req("ra2", 3, "/a"),   # MISS
            req("rb2", 3, "/b"),   # HIT
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[2]["verdict"], MISS)
        self.assertEqual(res[3]["verdict"], HIT)

        ev += [{"id": "x2", "type": "clear", "at": 4}]
        eng, res = run_events({}, ev)
        self.assertEqual(eng.state["cache"], {})

    def test_virtual_time_only_advances(self):
        eng = Engine()
        eng.apply_event({"type": "network", "at": 10, "up": True})
        with self.assertRaises(ValueError):
            eng.apply_event(req("r1", 5))

    def test_bytes_counters(self):
        ev = [
            origin("o1", 0, body="x" * 1000,
                   headers={"cache-control": "max-age=100"}),
            req("r1", 1),
            req("r2", 2),
        ]
        eng, res = run_events({}, ev)
        c = eng.state["counters"]
        # 回源只发生一次，但向客户端服务了两次
        self.assertGreater(c["bytes_served"], c["bytes_from_origin"])
        self.assertGreater(c["bytes_from_origin"], 1000)


def origin_b64(eid, at, url="/bin", data=b"", **kw):
    import base64
    ev = {"id": eid, "type": "origin", "at": at, "url": url,
          "body_base64": base64.b64encode(data).decode(), **kw}
    return ev


def rreq(eid, at, url="/f", rng=None, if_range=None, extra=None):
    headers = dict(extra or {})
    if rng:
        headers["Range"] = rng
    if if_range is not None:
        headers["If-Range"] = if_range
    return {"id": eid, "type": "request", "at": at, "url": url,
            "headers": headers}


def body_of(result):
    return from_b64(result["response_body_b64"])


class RangeParserTests(unittest.TestCase):
    def test_parse_single_suffix_open(self):
        self.assertEqual(parse_range_spec("bytes=0-99")[0], "range")
        self.assertEqual(parse_range_spec("bytes=10-")[0], "range")
        self.assertEqual(parse_range_spec("bytes=-50")[2], None)
        self.assertEqual(parse_range_spec("bytes=-50")[3], 50)

    def test_parse_multi_and_invalid(self):
        self.assertEqual(parse_range_spec("bytes=0-1,2-3")[0], "multi")
        self.assertEqual(parse_range_spec("bytes=5-1")[0], "invalid")
        self.assertEqual(parse_range_spec("bytes=abc")[0], "invalid")

    def test_resolve_clamped_and_unsatisfiable(self):
        self.assertEqual(resolve_range(parse_range_spec("bytes=5-100"), 10),
                         (5, 9))
        self.assertIsNone(resolve_range(parse_range_spec("bytes=20-30"), 10))
        self.assertEqual(resolve_range(parse_range_spec("bytes=-4"), 10), (6, 9))
        self.assertIsNone(resolve_range(parse_range_spec("bytes=0-0"), 0))


class RangeCacheReviewFixesTests(unittest.TestCase):
    """复核确认的四个范围缓存正确性缺陷的回归测试。"""

    def test_fix1_out_of_range_with_mismatched_if_range_returns_full_200(self):
        # 新鲜完整缓存；越界 Range + 不匹配的 If-Range：必须忽略 Range 返回完整 200，
        # 不能先用缓存长度判定 416
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v1"',
                   headers={"cache-control": "max-age=1000"}),
            req("r0", 1, "/f"),
            rreq("r1", 2, rng="bytes=500-600", if_range='"old"'),
        ]
        eng, res = run_events({}, ev)
        self.assertEqual(res[1]["status"], 200)
        self.assertEqual(res[1]["verdict"], REFRESHED)
        self.assertEqual(body_of(res[1]), b"0123456789")
        codes = [t["code"] for t in res[1]["trace"]]
        self.assertIn("IF_RANGE_MISMATCH", codes)
        self.assertNotIn("RANGE_416", codes)
        self.assertTrue(res[1]["range"]["served_full_fallback"])
        self.assertTrue(res[1]["from_origin"])

    def test_fix1_control_out_of_range_without_if_range_is_416(self):
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v1"',
                   headers={"cache-control": "max-age=1000"}),
            req("r0", 1, "/f"),
            rreq("r1", 2, rng="bytes=500-600"),
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["status"], 416)
        self.assertEqual(res[1]["verdict"], UNSATISFIABLE)
        self.assertFalse(res[1]["from_origin"])

    def test_fix1_out_of_range_with_matching_if_range_is_416(self):
        # If-Range 匹配：仍按区间处理，越界 -> 416
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v1"',
                   headers={"cache-control": "max-age=1000"}),
            req("r0", 1, "/f"),
            rreq("r1", 2, rng="bytes=500-600", if_range='"v1"'),
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["status"], 416)

    def test_fix2_first_206_covering_full_representation_promoted(self):
        # 单个 206 已覆盖完整表示（bytes=0-9/10）：缓存升级为完整条目，
        # 后续无 Range 的 GET 必须返回完整 200 与全部正文
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v"',
                   headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, rng="bytes=0-9"),
            {"id": "r2", "type": "request", "at": 2, "url": "/f"},
        ]
        eng, res = run_events({}, ev)
        self.assertEqual(res[0]["status"], 206)
        self.assertEqual(res[1]["status"], 200)
        self.assertEqual(res[1]["verdict"], HIT)
        self.assertEqual(body_of(res[1]), b"0123456789")
        variant = eng.state["cache"]["GET /f"]["variants"][0]
        self.assertFalse(variant["partial"])
        self.assertEqual(variant["body"], b"0123456789")
        self.assertNotIn("content-range", res[1]["response_headers"])

    def test_fix3_merge_to_full_removes_content_range_and_sets_length(self):
        # 相邻片段合并成完整表示后：200 响应不得残留 Content-Range，
        # Content-Length 必须是完整长度而非最后片段长度
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v"',
                   headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, rng="bytes=0-4"),
            rreq("r2", 2, rng="bytes=5-9"),
            {"id": "r3", "type": "request", "at": 3, "url": "/f"},
        ]
        eng, res = run_events({}, ev)
        # 合并发生在 r2：r2 的 206 仍带 Content-Range
        self.assertEqual(res[0]["response_headers"]["content-range"], "bytes 0-4/10")
        # r3 是拼满后的完整 GET
        full = res[2]
        self.assertEqual(full["status"], 200)
        self.assertNotIn("content-range", full["response_headers"])
        self.assertEqual(full["response_headers"]["content-length"], "10")
        self.assertEqual(body_of(full), b"0123456789")
        variant = eng.state["cache"]["GET /f"]["variants"][0]
        self.assertFalse(variant["partial"])
        self.assertEqual(variant["headers"]["content-length"], "10")
        self.assertNotIn("content-range", variant["headers"])

    def test_fix4_gap_fill_records_actual_origin_bytes(self):
        ev = [
            origin("o1", 0, url="/f", body="0123456789ABCDEFGHIJ", etag='"v"',
                   headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, rng="bytes=0-4"),
            rreq("r2", 2, rng="bytes=10-14"),
        ]
        eng, res = run_events({}, ev)
        fill = res[1]
        self.assertEqual(fill["verdict"], RANGE_FILL)
        # 顶层结果与 range 明细都必须记录本次实际回源字节（不再为 0）
        self.assertGreater(fill["origin_bytes"], 0)
        self.assertEqual(fill["range"]["origin_bytes"], fill["origin_bytes"])
        # 已缓存 [0,4]，本次只回源缺口 [10,14] 共 5 字节正文
        self.assertEqual(fill["range"]["origin_body_bytes"], 5)
        self.assertEqual(fill["range"]["fetched"], [[10, 14]])
        # 累计计数器同步
        self.assertEqual(eng.state["counters"]["range_bytes_from_origin"], 10)

    def test_fix4_range_hit_has_zero_origin_bytes(self):
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v"',
                   headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, rng="bytes=0-4"),
            rreq("r2", 2, rng="bytes=0-4"),
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], RANGE_HIT)
        self.assertEqual(res[1]["origin_bytes"], 0)
        self.assertNotIn("origin_bytes", res[1].get("range", {}))


class RangeCacheFlowTests(unittest.TestCase):
    def test_hit_fill_416_and_gap_accounting(self):
        data = bytes(range(256))
        ev = [
            origin_b64("o", 0, data=data, etag='"v1"',
                       headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, url="/bin", rng="bytes=0-99"),
            rreq("r2", 2, url="/bin", rng="bytes=0-99"),
            rreq("r3", 3, url="/bin", rng="bytes=50-149"),
            rreq("r4", 4, url="/bin", rng="bytes=300-400"),
        ]
        eng, res = run_events({}, ev)
        self.assertEqual([r["verdict"] for r in res],
                         [RANGE_FILL, RANGE_HIT, RANGE_FILL, UNSATISFIABLE])
        self.assertEqual(body_of(res[0]), data[0:100])
        self.assertEqual(body_of(res[2]), data[50:150])
        c = eng.state["counters"]
        self.assertEqual(c["range_hits"], 1)
        # 回源正文 = 100（首次）+ 50（缺口 100-149）
        self.assertEqual(c["range_bytes_from_origin"], 150)

    def test_if_range_match_allows_fill_mismatch_forces_full(self):
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v1"',
                   headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, rng="bytes=0-3"),
            rreq("r2", 2, rng="bytes=4-7", if_range='"v1"'),
            rreq("r3", 3, rng="bytes=8-9", if_range='"old"'),
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], RANGE_FILL)
        self.assertEqual(res[2]["verdict"], REFRESHED)
        self.assertEqual(res[2]["status"], 200)
        self.assertEqual(body_of(res[2]), b"0123456789")

    def test_validator_change_discards_segments(self):
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v1"',
                   headers={"cache-control": "max-age=5"}),
            rreq("r1", 1, rng="bytes=0-4"),
            {"id": "ch", "type": "origin_change", "at": 10, "url": "/f",
             "body": "XXXXXXXXXX", "etag": '"v2"'},
            rreq("r2", 11, rng="bytes=0-4"),
        ]
        eng, res = run_events({}, ev)
        self.assertEqual(res[1]["status"], 200)
        self.assertEqual(res[1]["verdict"], REFRESHED)
        self.assertEqual(body_of(res[1]), b"X" * 10)
        self.assertGreaterEqual(eng.state["counters"]["segments_invalidated"], 1)

    def test_stale_covered_range_revalidates_to_206(self):
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v1"',
                   headers={"cache-control": "max-age=5"}),
            rreq("r1", 1, rng="bytes=0-9"),
            rreq("r2", 20, rng="bytes=2-7"),
        ]
        _, res = run_events({}, ev)
        self.assertEqual(res[1]["verdict"], REVALIDATED)
        self.assertEqual(res[1]["status"], 206)
        self.assertEqual(body_of(res[1]), b"234567")

    def test_weak_etag_segments_not_stored(self):
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='W/"v1"',
                   headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, rng="bytes=0-4"),
            rreq("r2", 2, rng="bytes=0-4"),
        ]
        eng, res = run_events({}, ev)
        self.assertTrue(res[1]["from_origin"])  # 未存片段，再次回源
        self.assertNotIn("GET /f", eng.state["cache"])

    def test_range_cache_disabled_passes_through(self):
        ev = [
            origin("o1", 0, url="/f", body="0123456789", etag='"v"',
                   headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, rng="bytes=0-4"),
            rreq("r2", 2, rng="bytes=0-4"),
        ]
        eng, res = run_events({"range_cache": False}, ev)
        self.assertTrue(all(r["from_origin"] for r in res))
        self.assertNotIn("GET /f", eng.state["cache"])


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.store = Store(self.tmp.name)

    def tearDown(self):
        self.store.close()
        os.unlink(self.tmp.name)

    def test_persist_and_run_until(self):
        sc = self.store.create_scenario("s1")
        sid = sc["id"]
        events = [
            {"id": "o1", "type": "origin", "at": 0, "url": "/a",
             "headers": {"cache-control": "max-age=100"}},
            req("r1", 10, "/a"),
            req("r2", 20, "/a"),
            {"id": "o2", "type": "origin", "at": 25, "url": "/b",
             "headers": {"cache-control": "no-store"}},
            req("r3", 30, "/b"),
        ]
        self.store.add_events(sid, events)

        part = self.store.run(sid, until=15)
        self.assertEqual(part["events_run"], 2)   # o1 + r1
        verdicts = [r["verdict"] for r in part["results"] if r["type"] == "request"]
        self.assertEqual(verdicts, [MISS])

        rest = self.store.run(sid)
        verdicts = [r["verdict"] for r in rest["results"] if r["type"] == "request"]
        self.assertEqual(verdicts, [HIT, UNCACHEABLE])

        # 重新打开数据库，状态仍在
        self.store.close()
        store2 = Store(self.tmp.name)
        state = store2.get_state(sid)
        self.assertEqual(state["counters"]["requests"], 3)
        store2.close()
        self.store = Store(self.tmp.name)

    def test_reset(self):
        sc = self.store.create_scenario("s2")
        sid = sc["id"]
        self.store.add_event(sid, origin("o1", 0))
        self.store.add_event(sid, req("r1", 1))
        self.store.run(sid)
        state = self.store.reset(sid)
        self.assertEqual(state["counters"]["requests"], 0)
        self.assertEqual(state["cache"], {})
        out = self.store.run(sid)
        self.assertEqual(out["events_run"], 2)

    def test_clone_and_compare(self):
        sc = self.store.create_scenario("base")
        sid = sc["id"]
        self.store.add_events(sid, [
            origin("o1", 0),
            req("r1", 1), req("r2", 2), req("r3", 3),
        ])
        # A：默认不缓存无显式头的响应
        self.store.run(sid)
        # B：复制后注入缺省 max-age
        clone = self.store.clone_scenario(sid, "with-default-cc",
                                          {"default_cc": {"max-age": 60}})
        self.store.run(clone["id"])

        ca = self.store.get_state(sid)["counters"]
        cb = self.store.get_state(clone["id"])["counters"]
        self.assertEqual(ca["origin_fetches"], 3)
        self.assertEqual(cb["origin_fetches"], 1)
        self.assertGreater(ca["bytes_from_origin"], cb["bytes_from_origin"])

    def test_range_segments_persist_across_restart(self):
        import base64
        sc = self.store.create_scenario("range-persist")
        sid = sc["id"]
        data = bytes(range(20))
        self.store.add_events(sid, [
            origin_b64("o", 0, data=data, etag='"v"',
                       headers={"cache-control": "max-age=1000"}),
            rreq("r1", 1, url="/bin", rng="bytes=0-9"),
        ])
        out = self.store.run(sid)
        self.assertEqual([r["verdict"] for r in out["results"] if r["type"] == "request"],
                         [RANGE_FILL])

        # 重启：新 Store 打开同一库文件，分段状态必须恢复且可继续命中/补齐
        self.store.close()
        store2 = Store(self.tmp.name)
        state = store2.get_state(sid)
        variant = state["cache"]["GET /bin"]["variants"][0]
        self.assertTrue(variant["partial"])
        self.assertEqual(variant["length"], 20)
        seg = variant["segments"][0]
        self.assertEqual([seg["start"], seg["end"]], [0, 9])
        self.assertEqual(seg["body"], data[0:10])  # bytes 经 base64 无损往返
        self.assertEqual(state["counters"]["range_bytes_from_origin"], 10)

        store2.add_event(sid, rreq("r2", 2, url="/bin", rng="bytes=0-9"))
        out = store2.run(sid)
        verdicts = [r["verdict"] for r in out["results"] if r["type"] == "request"]
        self.assertEqual(verdicts, [RANGE_HIT])
        store2.close()
        self.store = Store(self.tmp.name)

    def test_bad_event(self):
        sc = self.store.create_scenario("s3")
        with self.assertRaises(ValueError):
            self.store.add_event(sc["id"], {"type": "bogus", "at": 1})
        with self.assertRaises(ValueError):
            self.store.add_event(sc["id"], {"type": "request", "url": "/"})

    def test_state_json_serializable_with_bytes_body(self):
        # body 给 bytes 也必须能落到 JSON 状态里
        sc = self.store.create_scenario("bytes")
        sid = sc["id"]
        self.store.add_event(sid, {"id": "o", "type": "origin", "at": 0,
                                   "url": "/a", "body": b"abc",
                                   "headers": {"cache-control": "max-age=10"}})
        self.store.add_event(sid, req("r", 1, "/a"))
        out = self.store.run(sid)
        self.assertEqual(out["results"][1]["verdict"], MISS)
        # 再跑一步（触发状态保存 + 重新加载）不报错
        self.store.add_event(sid, req("r2", 2, "/a"))
        self.store.run(sid)

    def test_delete_scenario_cascades(self):
        sc = self.store.create_scenario("s4")
        sid = sc["id"]
        self.store.add_event(sid, origin("o1", 0))
        self.store.run(sid)
        self.assertTrue(self.store.delete_scenario(sid))
        self.assertIsNone(self.store.get_state(sid))
        with self.assertRaises(KeyError):
            self.store.get_scenario(sid)


if __name__ == "__main__":
    unittest.main(verbosity=2)
