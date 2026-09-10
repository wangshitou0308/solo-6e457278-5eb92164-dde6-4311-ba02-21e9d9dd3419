"""HTTP 缓存仿真引擎（纯逻辑，不涉及 IO；时间为虚拟时钟）。

支持的缓存语义：
  * 响应指令：max-age / s-maxage / no-cache / no-store / private /
    must-revalidate / proxy-revalidate / Expires / ETag / Last-Modified / Vary
  * 请求指令：no-cache / no-store / max-age / max-stale / min-fresh / only-if-cached
  * 条件请求：If-None-Match / If-Modified-Since 与 304 合并
  * 共享 / 私有缓存模式（s-maxage、private 仅在共享模式生效）
  * 启发式过期（Last-Modified 年龄比例）
  * 断网失败路径：must-revalidate -> 504；普通陈旧响应允许继续使用
"""

from __future__ import annotations

import copy
import json
from email.utils import parsedate
from calendar import timegm

DEFAULT_CONFIG = {
    # shared：共享缓存（代理），private 指令不可缓存、s-maxage 生效
    # private：浏览器类私有缓存，private 可缓存、忽略 s-maxage
    "cache_mode": "shared",
    # 无显式新鲜度时，是否按 Last-Modified 年龄比例做启发式缓存
    "heuristic_cache": False,
    "heuristic_ratio": 0.1,
    "heuristic_min": 0,
    "heuristic_max": 86400,
    # 可缓存的源站状态码
    "cacheable_statuses": [200, 203, 301, 304, 404, 410],
    # 既无 Cache-Control/Expires，也无 ETag/Last-Modified 时是否缓存
    "cache_without_explicit": False,
    # 响应缺少 Cache-Control 时注入的缺省指令，例如 {"max-age": 60}
    # 用于“复制场景后调整规则”做 A/B 对比
    "default_cc": None,
    # 是否遵循请求中的 Cache-Control 指令
    "respect_request_cc": True,
}

# 判定结果枚举
HIT = "HIT"                        # 新鲜缓存直接命中
MISS = "MISS"                      # 回源获得完整响应并写入缓存
REVALIDATED = "REVALIDATED"        # 条件重验证得到 304，继续使用缓存副本
REFRESHED = "REFRESHED"            # 重验证/过期回源得到完整新响应，缓存被更新
NOT_MODIFIED = "NOT_MODIFIED"      # 客户端自带条件请求，源站直接 304
UNCACHEABLE = "UNCACHEABLE"        # 回源成功但响应不允许缓存
STALE = "STALE"                    # 断网时使用了陈旧副本
ERROR = "ERROR"                    # 断网且无法满足（504/502）


def new_state() -> dict:
    """构造一份空的仿真执行状态。"""
    return {
        "time": 0.0,
        "network_up": True,
        "origins": {},          # url -> 源站当前资源定义
        "cache": {},            # "METHOD url" -> {"vary": [...], "variants": [entry, ...]}
        "counters": {
            "requests": 0,
            "hits": 0,
            "origin_fetches": 0,
            "revalidations": 0,
            "not_modified": 0,
            "stale_served": 0,
            "errors": 0,
            "bytes_served": 0,
            "bytes_from_origin": 0,
        },
        "results": [],
        "executed_event_ids": [],
    }


# ---------------------------------------------------------------- 基础工具

def norm_headers(headers) -> dict:
    """请求/响应头统一成小写键；值统一为 str。"""
    out = {}
    if not headers:
        return out
    items = headers.items() if isinstance(headers, dict) else headers
    for k, v in items:
        out[str(k).strip().lower()] = str(v)
    return out


def parse_cc(value: str) -> dict:
    """解析 Cache-Control 头，返回 {token: True} 或 {token: int}。"""
    result = {}
    if value is None:
        return result
    for part in str(value).split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            token, _, raw = part.partition("=")
            token = token.strip().lower()
            raw = raw.strip().strip('"')
            try:
                result[token] = int(raw)
            except ValueError:
                result[token] = raw
        else:
            result[part.lower()] = True
    return result


def parse_time_value(value, default=None):
    """头里的时间值：数字即虚拟时间戳；字符串支持数字或 HTTP 日期。"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    try:
        return float(s)
    except ValueError:
        pass
    parsed = parsedate(s)
    if parsed is None:
        return default
    return float(timegm(parsed))


def wire_size(status: int, headers: dict, body) -> int:
    """仿真估算一次 HTTP 响应在线传输的字节数（状态行 + 响应头 + 响应体）。"""
    n = len(f"HTTP/1.1 {status}\r\n")
    for k, v in headers.items():
        n += len(f"{k}: {v}\r\n")
    n += 2
    if body is not None:
        n += len(body.encode("utf-8")) if isinstance(body, str) else len(body)
    return n


def norm_body(body):
    # 状态需要 JSON 持久化，body 统一为字符串
    if body is None:
        return ""
    if isinstance(body, bytes):
        return body.decode("utf-8", "replace")
    if isinstance(body, str):
        return body
    return json.dumps(body, ensure_ascii=False)


def _variant_key(selected: dict) -> str:
    return json.dumps(selected, sort_keys=True, ensure_ascii=False)


def _etag_values(header: str):
    if not header:
        return []
    return [x.strip() for x in header.split(",") if x.strip()]


def _etag_weak_equal(a: str, b: str) -> bool:
    """弱比较：剥掉 W/ 前缀后相等即可（RFC 7232）。"""
    if not a or not b:
        return False
    na = a.strip().upper().removeprefix("W/")
    nb = b.strip().upper().removeprefix("W/")
    return na == nb


# ---------------------------------------------------------------- 引擎

class Engine:
    def __init__(self, config: dict | None = None):
        self.config = copy.deepcopy(DEFAULT_CONFIG)
        if config:
            self.config.update(copy.deepcopy(config))
        self.state = new_state()

    def load_state(self, state: dict):
        self.state = copy.deepcopy(state)

    # ---- 事件入口 --------------------------------------------------------

    def apply_event(self, event: dict) -> dict:
        """按顺序执行单个事件，返回该事件的判定结果文档。"""
        etype = event.get("type")
        at = float(event.get("at", self.state["time"]))
        if at < self.state["time"]:
            raise ValueError(
                f"事件时间 {at} 早于当前虚拟时钟 {self.state['time']}，虚拟时间只能前进"
            )
        self.state["time"] = at

        dispatch = {
            "origin": self._apply_origin,
            "origin_change": self._apply_origin_change,
            "network": self._apply_network,
            "clear": self._apply_clear,
            "request": self._apply_request,
        }
        if etype not in dispatch:
            raise ValueError(f"未知事件类型: {etype!r}（可选 {sorted(dispatch)}）")
        result = dispatch[etype](event, at)
        result = {"event_id": event.get("id"), **result}
        self.state["results"].append(result)
        if event.get("id") is not None:
            self.state["executed_event_ids"].append(event["id"])
        return result

    # ---- 源站定义 / 变更 -------------------------------------------------

    def _origin_def(self, event: dict):
        url = event.get("url")
        if not url:
            raise ValueError("origin 事件需要 url 字段")
        headers = norm_headers(event.get("headers"))
        if event.get("etag") is not None and "etag" not in headers:
            headers["etag"] = str(event["etag"])
        if event.get("last_modified") is not None and "last-modified" not in headers:
            headers["last-modified"] = str(event["last_modified"])
        return url, {
            "status": int(event.get("status", 200)),
            "headers": headers,
            "body": norm_body(event.get("body", "")),
        }

    def _apply_origin(self, event, at):
        url, d = self._origin_def(event)
        self.state["origins"][url] = d
        return {"type": "origin", "at": at, "url": url, "summary": self._resource_summary(d)}

    def _apply_origin_change(self, event, at):
        url = event.get("url")
        if not url or url not in self.state["origins"]:
            raise ValueError(f"origin_change 需要先用 origin 事件定义资源: {url!r}")
        cur = self.state["origins"][url]
        before = self._resource_summary(cur)
        if event.get("body") is not None:
            cur["body"] = norm_body(event["body"])
        if event.get("status") is not None:
            cur["status"] = int(event["status"])
        if event.get("headers"):
            cur["headers"].update(norm_headers(event["headers"]))
        if event.get("remove_headers"):
            for h in event["remove_headers"]:
                cur["headers"].pop(str(h).lower(), None)
        if event.get("etag") is not None:
            cur["headers"]["etag"] = str(event["etag"])
        if event.get("last_modified") is not None:
            cur["headers"]["last-modified"] = str(event["last_modified"])
        return {"type": "origin_change", "at": at, "url": url,
                "before": before, "after": self._resource_summary(cur)}

    @staticmethod
    def _resource_summary(d):
        body = d.get("body", "")
        return {
            "status": d["status"],
            "etag": d["headers"].get("etag"),
            "last_modified": d["headers"].get("last-modified"),
            "cache_control": d["headers"].get("cache-control"),
            "body_bytes": len(body.encode("utf-8")) if isinstance(body, str) else len(body),
        }

    # ---- 网络 / 清除 -----------------------------------------------------

    def _apply_network(self, event, at):
        up = bool(event.get("up", True))
        self.state["network_up"] = up
        return {"type": "network", "at": at, "up": up}

    def _apply_clear(self, event, at):
        url = event.get("url")
        method = str(event.get("method", "GET")).upper()
        removed = 0
        if url:
            bucket = self.state["cache"].pop(f"{method} {url}", None)
            if bucket:
                removed = len(bucket["variants"])
        else:
            removed = sum(len(b["variants"]) for b in self.state["cache"].values())
            self.state["cache"].clear()
        return {"type": "clear", "at": at, "url": url, "removed_variants": removed}

    # ---- 请求处理（核心） ------------------------------------------------

    def _apply_request(self, event, at):
        counters = self.state["counters"]
        counters["requests"] += 1

        url = event.get("url")
        method = str(event.get("method", "GET")).upper()
        if not url:
            raise ValueError("request 事件需要 url 字段")
        req_headers = norm_headers(event.get("headers"))
        if self.config["respect_request_cc"]:
            req_cc = parse_cc(req_headers.get("cache-control", ""))
        else:
            req_cc = {}
        req_no_store = bool(req_cc.get("no-store"))
        client_conditional = "if-none-match" in req_headers or "if-modified-since" in req_headers
        cache_key = f"{method} {url}"
        trace = []

        def log(code, detail):
            trace.append({"step": len(trace) + 1, "code": code, "detail": detail})

        log("REQUEST", f"{method} {url}")
        if req_cc:
            log("REQUEST_CC", f"请求 Cache-Control: {dict(req_cc)}")
        if client_conditional:
            log("CLIENT_CONDITIONAL", "客户端自带条件请求，将转发到源站")

        # only-if-cached：只允许使用缓存（含陈旧），无缓存则 504，绝不回源
        if req_cc.get("only-if-cached"):
            bucket = self.state["cache"].get(cache_key)
            entry = self._select_variant(bucket, req_headers, trace) if bucket and not req_no_store else None
            if entry is not None:
                age = self._entry_age(entry, at)
                stale = not self._is_fresh(entry, age)
                served = self._entry_response(entry, age, at, stale=stale)
                log("ONLY_IF_CACHED",
                    f"only-if-cached：直接返回缓存副本（age={age:.0f}s，{'陈旧' if stale else '新鲜'}）")
                verdict = HIT
                return self._done(counters, served, verdict, at, url, method,
                                  req_headers, trace, entry, origin_used=False)
            log("ONLY_IF_CACHED_FAIL", "only-if-cached 但无缓存变体，返回 504（不回源）")
            served = self._gateway_error(504, "Gateway Timeout (only-if-cached, no cached entry)")
            return self._done(counters, served, ERROR, at, url, method,
                              req_headers, trace, None, origin_used=False)

        # ---- 路径 A：缓存查找与新鲜度判断 ----
        bucket = self.state["cache"].get(cache_key)
        entry = None
        if bucket and not req_no_store:
            entry = self._select_variant(bucket, req_headers, trace)
            if entry is None:
                log("VARY_MISMATCH", "存在缓存但 Vary 选择的请求头不匹配任何变体")

        # 客户端自带条件请求（If-None-Match / If-Modified-Since）：不做新鲜度短路，
        # 一律转发源站；entry 保留用于合并 304 元数据
        if entry is None and not client_conditional:
            log("CACHE_MISS", "没有可用缓存条目（或请求 no-store / Vary 不匹配）")

        if entry is not None and not client_conditional:
            age = self._entry_age(entry, at)
            log("CACHE_ENTRY",
                f"age={age:.0f}s ttl={self._fmt_ttl(entry)} no_cache={entry['no_cache']} "
                f"must_revalidate={entry['must_revalidate']}")
            decision = self._freshness_decision(entry, age, req_cc, trace)
            if decision == "fresh":
                served = self._entry_response(entry, age, at)
                return self._done(counters, served, HIT, at, url, method,
                                  req_headers, trace, entry, origin_used=False)
            # stale-ok / stale-must：在线回源重验证，断网在路径 B 分流

        # ---- 路径 B：回源（MISS / 重验证 / 客户端条件请求） ----
        if not self.state["network_up"]:
            if entry is not None and not self._must_not_serve_stale(entry, req_cc):
                age = self._entry_age(entry, at)
                served = self._entry_response(entry, age, at, stale=True)
                log("NETWORK_DOWN_STALE", "网络断开，使用陈旧缓存副本（带 Warning: 110）")
                return self._done(counters, served, STALE, at, url, method,
                                  req_headers, trace, entry, origin_used=False)
            log("NETWORK_DOWN_FAIL", "网络断开且无可用/不允许使用的缓存副本，返回 504")
            served = self._gateway_error(504, "Gateway Timeout (simulated network down)")
            return self._done(counters, served, ERROR, at, url, method,
                              req_headers, trace, entry, origin_used=False)

        # 构造发往源站的请求头；重验证时附加缓存验证器
        forwarded = dict(req_headers)
        conditional_sent = False
        if entry is not None and not client_conditional:
            age = self._entry_age(entry, at)
            if entry["headers"].get("etag"):
                forwarded["if-none-match"] = entry["headers"]["etag"]
                conditional_sent = True
                log("REVALIDATE", f"If-None-Match: {forwarded['if-none-match']} (age={age:.0f}s)")
            elif entry["headers"].get("last-modified"):
                forwarded["if-modified-since"] = entry["headers"]["last-modified"]
                conditional_sent = True
                log("REVALIDATE", f"If-Modified-Since: {forwarded['if-modified-since']} (age={age:.0f}s)")
            else:
                log("REVALIDATE_NONE", "条目没有 ETag/Last-Modified，只能完整回源")

        origin = self.state["origins"].get(url)
        counters["origin_fetches"] += 1
        if conditional_sent:
            counters["revalidations"] += 1
        if origin is None:
            log("ORIGIN_MISSING", f"源站未定义资源 {url}，仿真返回 502")
            served = self._gateway_error(502, "Bad Gateway (origin resource not defined in scenario)")
            return self._done(counters, served, ERROR, at, url, method,
                              req_headers, trace, entry, origin_used=True,
                              origin_status=502, origin_resp=served)

        origin_status, origin_headers, origin_body = self._fetch_origin(
            origin, forwarded, at, trace
        )
        origin_resp = {"status": origin_status, "headers": origin_headers, "body": origin_body}
        counters["bytes_from_origin"] += wire_size(origin_status, origin_headers, origin_body)

        # ---- 304 ----
        if origin_status == 304:
            counters["not_modified"] += 1
            if client_conditional:
                # 客户端自带条件请求：把 304 原样交给客户端；顺带刷新已有条目元数据
                if entry is not None:
                    self._merge_304(entry, origin_headers, at, trace)
                log("CLIENT_NOT_MODIFIED", "源站 304：透传给客户端")
                return self._done(counters, origin_resp, NOT_MODIFIED, at, url, method,
                                  req_headers, trace, entry, origin_used=True,
                                  origin_status=304, origin_resp=origin_resp)
            if entry is not None:
                self._merge_304(entry, origin_headers, at, trace)
                age = self._entry_age(entry, at)
                served = self._entry_response(entry, age, at)
                log("NOT_MODIFIED", "源站 304：合并校验元数据，继续使用缓存副本")
                return self._done(counters, served, REVALIDATED, at, url, method,
                                  req_headers, trace, entry, origin_used=True,
                                  origin_status=304, origin_resp=origin_resp)
            log("CLIENT_NOT_MODIFIED", "无缓存条目却收到 304，原样透传")
            return self._done(counters, origin_resp, NOT_MODIFIED, at, url, method,
                              req_headers, trace, None, origin_used=True,
                              origin_status=304, origin_resp=origin_resp)

        # ---- 完整响应：尝试写缓存 ----
        why_not = None
        stored_entry = None
        if method == "GET" and not req_no_store:
            store_info = self._try_store(cache_key, origin_resp, req_headers, at, trace)
            if store_info["stored"]:
                stored_entry = store_info["entry"]
            else:
                why_not = store_info["reason"]
                if entry is not None:
                    self._invalidate_variant(cache_key, entry)
                    log("INVALIDATE", f"新响应不可缓存（{why_not}），旧缓存条目已失效")
        else:
            why_not = "HEAD 等非 GET 请求不写缓存" if method != "GET" else "请求 no-store，不写缓存"
            log("NO_STORE", why_not)

        served = origin_resp
        if stored_entry is not None:
            if entry is not None:
                verdict = REFRESHED
                log("REFRESHED", "重验证返回完整新响应，缓存条目已替换")
            else:
                verdict = MISS
                log("MISS_STORED", "回源完整响应已写入缓存")
        else:
            verdict = UNCACHEABLE
            log("UNCACHEABLE", why_not)

        # 结果中附带执行后仍存在的缓存条目摘要
        after_entry = stored_entry
        if after_entry is None and entry is not None and self._variant_exists(cache_key, entry):
            after_entry = entry
        return self._done(counters, served, verdict, at, url, method,
                          req_headers, trace, after_entry,
                          origin_used=True, origin_status=origin_status,
                          origin_resp=origin_resp)

    # ---- 变体选择 / 新鲜度 ------------------------------------------------

    @staticmethod
    def _trace(trace, code, detail):
        trace.append({"step": len(trace) + 1, "code": code, "detail": detail})

    def _select_variant(self, bucket, req_headers, trace):
        if not bucket:
            return None
        variants = bucket["variants"]
        vary = bucket.get("vary") or []
        if not vary:
            return variants[0] if variants else None
        selected = {h: req_headers.get(h, "") for h in vary}
        key = _variant_key(selected)
        for v in variants:
            if v["variant_key"] == key:
                return v
        return None

    def _entry_age(self, entry, now):
        return now - entry["stored_at"] + entry.get("init_age", 0)

    def _is_fresh(self, entry, age) -> bool:
        if entry["no_cache"] or entry["ttl"] is None:
            return False
        return age <= entry["ttl"]

    @staticmethod
    def _fmt_ttl(entry):
        return "None" if entry["ttl"] is None else f"{entry['ttl']:.1f}s"

    def _freshness_decision(self, entry, age, req_cc, trace):
        """返回 fresh / stale-ok / stale-must。"""
        if entry["no_cache"]:
            self._trace(trace, "STALE_NO_CACHE", "响应带 no-cache：每次使用前必须重验证")
            return "stale-must"
        if req_cc.get("no-cache"):
            self._trace(trace, "REQ_NO_CACHE", "请求带 no-cache：强制重验证")
            return "stale-must"

        ttl = entry["ttl"]
        if ttl is not None and age <= ttl:
            if isinstance(req_cc.get("max-age"), int) and age > req_cc["max-age"]:
                self._trace(trace, "REQ_MAX_AGE", f"请求 max-age={req_cc['max-age']}s 小于当前 age={age:.0f}s")
                return "stale-ok"
            if isinstance(req_cc.get("min-fresh"), int) and age + req_cc["min-fresh"] > ttl:
                self._trace(trace, "REQ_MIN_FRESH", f"请求 min-fresh={req_cc['min-fresh']}s，剩余新鲜度不足")
                return "stale-ok"
            self._trace(trace, "FRESH", f"age={age:.0f}s <= ttl={ttl:.0f}s，缓存新鲜")
            return "fresh"

        # 已陈旧
        ms = req_cc.get("max-stale")
        if ms is True:
            self._trace(trace, "REQ_MAX_STALE", "请求 max-stale 无值：接受任意陈旧度")
            return "fresh"
        if isinstance(ms, int) and ttl is not None and age - ttl <= ms:
            self._trace(trace, "REQ_MAX_STALE", f"陈旧 {age - ttl:.0f}s 在 max-stale={ms}s 容忍范围内")
            return "fresh"
        if entry["must_revalidate"]:
            who = "proxy-revalidate" if entry.get("rev_by") == "proxy" else "must-revalidate"
            self._trace(trace, "STALE_MUST", f"缓存已陈旧且 {who}：必须重验证，断网必须返回 504")
            return "stale-must"
        if ttl is None:
            self._trace(trace, "STALE", "缓存没有可用新鲜寿命（启发式关闭且无显式新鲜度），按陈旧处理")
        else:
            self._trace(trace, "STALE", f"age={age:.0f}s > ttl={ttl:.1f}s，缓存已陈旧（在线将尝试重验证）")
        return "stale-ok"

    def _must_not_serve_stale(self, entry, req_cc) -> bool:
        """断网时该条目是否禁止返回陈旧副本。"""
        if entry["no_cache"] or entry["must_revalidate"]:
            return True
        if req_cc.get("no-cache"):
            return True
        return False

    # ---- 源站模拟 --------------------------------------------------------

    def _fetch_origin(self, origin, fwd_headers, at, trace):
        headers = copy.deepcopy(origin["headers"])
        status = origin["status"]
        body = origin["body"]

        inm = fwd_headers.get("if-none-match")
        ims = fwd_headers.get("if-modified-since")
        etag = headers.get("etag")
        lm = headers.get("last-modified")

        unchanged = False
        if inm is not None and etag:
            unchanged = any(_etag_weak_equal(tag, etag) for tag in _etag_values(inm))
            self._trace(trace, "ORIGIN_VALIDATE",
                f"源站比对 ETag：{etag!r} vs If-None-Match {inm!r} -> {'匹配' if unchanged else '不匹配'}")
        elif ims is not None and lm is not None:
            ims_t = parse_time_value(ims)
            lm_t = parse_time_value(lm)
            unchanged = ims_t is not None and lm_t is not None and ims_t >= lm_t
            self._trace(trace, "ORIGIN_VALIDATE",
                f"源站比对时间：Last-Modified={lm} vs If-Modified-Since={ims} -> "
                f"{'未修改' if unchanged else '已修改'}")

        if unchanged and status == 200:
            # 304 只携带校验器/缓存相关头
            keep = {h: headers[h] for h in
                    ("etag", "last-modified", "cache-control", "expires", "vary", "date")
                    if h in headers}
            return 304, keep, ""
        return status, headers, body

    # ---- 304 合并 / 写缓存 -----------------------------------------------

    def _merge_304(self, entry, resp304, at, trace):
        merged = dict(entry["headers"])
        merged.update(resp304)  # 304 携带的头更新/补充缓存条目
        entry["headers"] = merged
        entry["stored_at"] = at
        entry["init_age"] = 0
        cc = parse_cc(merged.get("cache-control", ""))
        entry["ttl"] = self._compute_ttl(merged, cc, at)
        entry["no_cache"] = "no-cache" in cc
        entry["must_revalidate"] = self._must_revalidate(cc)
        entry["rev_by"] = "proxy" if "proxy-revalidate" in cc else "must"
        self._trace(trace, "MERGE_304", "304 响应头已合并进缓存条目，age 归零")

    def _must_revalidate(self, cc) -> bool:
        if "must-revalidate" in cc:
            return True
        if self.config["cache_mode"] == "shared" and "proxy-revalidate" in cc:
            return True
        return False

    def _compute_ttl(self, headers, cc, at):
        """按 s-maxage > max-age > Expires > 启发式顺序计算新鲜寿命。"""
        if self.config["cache_mode"] == "shared" and isinstance(cc.get("s-maxage"), int):
            return float(cc["s-maxage"])
        if isinstance(cc.get("max-age"), int):
            return float(cc["max-age"])
        if "expires" in headers:
            exp = parse_time_value(headers["expires"])
            if exp is None:
                return 0.0
            date = parse_time_value(headers.get("date"), at)
            return max(0.0, exp - date)
        lm = headers.get("last-modified")
        if self.config["heuristic_cache"] and lm:
            lm_t = parse_time_value(lm)
            if lm_t is not None:
                ttl = (at - lm_t) * self.config["heuristic_ratio"]
                ttl = max(self.config["heuristic_min"], min(ttl, self.config["heuristic_max"]))
                return max(0.0, ttl)
        return None

    def _try_store(self, cache_key, resp, req_headers, at, trace):
        status, headers = resp["status"], resp["headers"]
        cc = parse_cc(headers.get("cache-control", ""))

        # 无 Cache-Control 时注入仿真缺省指令（供 A/B 对比）
        if "cache-control" not in headers and self.config["default_cc"]:
            cc = {str(k).lower(): (int(v) if isinstance(v, (int, float)) else v)
                  for k, v in self.config["default_cc"].items()}
            headers = dict(headers)
            headers["cache-control"] = ", ".join(
                str(k) if v is True else f"{k}={v}" for k, v in cc.items()
            )
            resp["headers"] = headers
            trace.append({"step": len(trace) + 1, "code": "DEFAULT_CC",
                          "detail": f"响应无 Cache-Control，注入仿真缺省指令 {dict(cc)}"})

        if status not in self.config["cacheable_statuses"]:
            return {"stored": False,
                    "reason": f"状态码 {status} 不在可缓存列表 {self.config['cacheable_statuses']}"}
        if "no-store" in cc:
            return {"stored": False, "reason": "响应 Cache-Control: no-store，禁止缓存"}
        if self.config["cache_mode"] == "shared" and "private" in cc:
            return {"stored": False, "reason": "共享缓存模式下响应 private，不允许缓存"}
        vary_raw = headers.get("vary")
        if vary_raw and vary_raw.strip() == "*":
            return {"stored": False, "reason": "Vary: * 表示不受限变体，不缓存"}

        ttl = self._compute_ttl(headers, cc, at)
        has_validator = "etag" in headers or "last-modified" in headers
        explicit_fresh = ttl is not None or "no-cache" in cc
        if not explicit_fresh and not has_validator:
            if not (self.config["cache_without_explicit"] or self.config["default_cc"]):
                return {"stored": False,
                        "reason": "无 Cache-Control/Expires 显式新鲜度，也无 ETag/Last-Modified "
                                  "验证器，默认不可缓存"}
            if ttl is None:
                ttl = 0.0  # 存但立即陈旧，下次使用需重验证

        vary_headers = ([h.strip().lower() for h in vary_raw.split(",") if h.strip()]
                        if vary_raw else [])
        selected = {h: req_headers.get(h, "") for h in vary_headers}
        age_raw = str(headers.get("age", ""))
        entry = {
            "variant_key": _variant_key(selected),
            "status": status,
            "headers": copy.deepcopy(headers),
            "body": resp["body"],
            "stored_at": at,
            "init_age": int(age_raw) if age_raw.isdigit() else 0,
            "ttl": ttl,
            "no_cache": "no-cache" in cc,
            "must_revalidate": self._must_revalidate(cc),
            "rev_by": "proxy" if "proxy-revalidate" in cc else "must",
        }
        bucket = self.state["cache"].setdefault(
            cache_key, {"vary": vary_headers, "variants": []})
        bucket["vary"] = vary_headers
        bucket["variants"] = [v for v in bucket["variants"]
                              if v["variant_key"] != entry["variant_key"]]
        bucket["variants"].append(entry)
        trace.append({"step": len(trace) + 1, "code": "STORE",
                      "detail": f"写入缓存：ttl={entry['ttl']}s vary={vary_headers or '无'} "
                                f"no_cache={entry['no_cache']} must_revalidate={entry['must_revalidate']}"})
        return {"stored": True, "reason": None, "entry": entry}

    def _invalidate_variant(self, cache_key, entry):
        bucket = self.state["cache"].get(cache_key)
        if not bucket:
            return
        bucket["variants"] = [v for v in bucket["variants"]
                              if v["variant_key"] != entry["variant_key"]]
        if not bucket["variants"]:
            self.state["cache"].pop(cache_key, None)

    def _variant_exists(self, cache_key, entry):
        if entry is None:
            return False
        bucket = self.state["cache"].get(cache_key)
        return bool(bucket and any(v["variant_key"] == entry["variant_key"]
                                   for v in bucket["variants"]))

    # ---- 组装响应 / 结果文档 ----------------------------------------------

    def _entry_response(self, entry, age, at, stale=False):
        headers = copy.deepcopy(entry["headers"])
        headers["age"] = str(int(max(0, age)))
        if stale:
            headers["warning"] = '110 cache-sim "Response is stale (network disconnected)"'
        return {"status": entry["status"], "headers": headers, "body": entry["body"]}

    @staticmethod
    def _gateway_error(status, message):
        return {"status": status,
                "headers": {"content-type": "text/plain; charset=utf-8"},
                "body": message}

    def _done(self, counters, served, verdict, at, url, method, req_headers,
              trace, entry, origin_used, origin_status=None, origin_resp=None):
        counters["bytes_served"] += wire_size(
            served["status"], served["headers"], served.get("body"))
        if verdict in (HIT, REVALIDATED, STALE):
            counters["hits"] += 1
        if verdict == STALE:
            counters["stale_served"] += 1
        if verdict == ERROR:
            counters["errors"] += 1
        return {
            "type": "request",
            "at": at,
            "url": url,
            "method": method,
            "request_headers": req_headers,
            "verdict": verdict,
            "status": served["status"],
            "from_origin": origin_used,
            "origin_status": origin_status,
            "response_headers": served["headers"],
            "response_body": served.get("body", ""),
            "bytes_served": wire_size(
                served["status"], served["headers"], served.get("body")),
            "origin_bytes": (wire_size(origin_resp["status"], origin_resp["headers"],
                                       origin_resp.get("body"))
                             if origin_resp is not None else 0),
            "cache_entry_after": self._entry_summary(entry) if entry else None,
            "network_up": self.state["network_up"],
            "trace": trace,
            "counters": copy.deepcopy(counters),
        }

    @staticmethod
    def _entry_summary(entry):
        if entry is None:
            return None
        return {
            "status": entry["status"],
            "ttl": entry["ttl"],
            "no_cache": entry["no_cache"],
            "must_revalidate": entry["must_revalidate"],
            "stored_at": entry["stored_at"],
            "etag": entry["headers"].get("etag"),
            "last_modified": entry["headers"].get("last-modified"),
            "vary": entry["headers"].get("vary"),
        }
