"""HTTP 缓存仿真引擎（纯逻辑，不涉及 IO；时间为虚拟时钟）。

支持的缓存语义：
  * 响应指令：max-age / s-maxage / no-cache / no-store / private /
    must-revalidate / proxy-revalidate / Expires / ETag / Last-Modified / Vary
  * 请求指令：no-cache / no-store / max-age / max-stale / min-fresh / only-if-cached
  * 条件请求：If-None-Match / If-Modified-Since 与 304 合并
  * 字节范围：单区间 Range / If-Range / 206 / 416，分段缓存（部分响应）、
    相邻/重叠片段合并（强 ETag 或 Last-Modified 一致才允许）、缺口回源补齐、
    验证器变化后丢弃旧片段
  * 共享 / 私有缓存模式（s-maxage、private 仅在共享模式生效）
  * 启发式过期（Last-Modified 年龄比例）
  * 断网失败路径：must-revalidate -> 504；普通陈旧响应允许继续使用
  * 源站故障仿真：响应延迟（delay，虚拟秒）、连接失败（fail）与 5xx 状态；
    5xx 不失效已有缓存条目
  * 陈旧容错：stale-while-revalidate（窗口内直接返回陈旧副本，同时调度后台
    重验证作业）、stale-if-error（回源 5xx 时按秒数窗口回退陈旧副本；
    连接失败路径沿用既有断网规则，若声明了 stale-if-error 则受其窗口约束）
  * 请求合并：同一虚拟时刻、同一缓存键 + Vary 变体只执行一次重验证，
    其余请求挂接在途作业（返回陈旧副本）或复用结算结果；待处理作业随虚拟
    时钟推进结算，并随状态持久化到 SQLite，重启后继续
"""

from __future__ import annotations

import base64
import copy
import json
from email.utils import parsedate
from calendar import timegm

DEFAULT_CONFIG = {
    # shared：共享缓存（代理），private 指令不可缓存、s-maxage 生效
    # private：浏览器类私有缓存，private 可缓存、忽略 s-maxage
    "cache_mode": "shared",
    # 是否启用分段（字节范围）缓存：关闭时不保存/合 206 部分响应
    "range_cache": True,
    # 无显式新鲜度时，是否按 Last-Modified 年龄比例做启发式缓存
    "heuristic_cache": False,
    "heuristic_ratio": 0.1,
    "heuristic_min": 0,
    "heuristic_max": 86400,
    # 可缓存的源站状态码（206 部分响应默认可缓存）
    "cacheable_statuses": [200, 203, 206, 301, 304, 404, 410],
    # 既无 Cache-Control/Expires，也无 ETag/Last-Modified 时是否缓存
    "cache_without_explicit": False,
    # 响应缺少 Cache-Control 时注入的缺省指令，例如 {"max-age": 60}
    # 用于“复制场景后调整规则”做 A/B 对比
    "default_cc": None,
    # 是否遵循请求中的 Cache-Control 指令
    "respect_request_cc": True,
}

# 判定结果枚举
HIT = "HIT"                        # 新鲜缓存直接命中（完整 200）
MISS = "MISS"                      # 回源获得完整响应并写入缓存
REVALIDATED = "REVALIDATED"        # 条件重验证得到 304，继续使用缓存副本
REFRESHED = "REFRESHED"            # 重验证/过期回源得到完整新响应，缓存被更新
NOT_MODIFIED = "NOT_MODIFIED"      # 客户端自带条件请求，源站直接 304
UNCACHEABLE = "UNCACHEABLE"        # 回源成功但响应不允许缓存
STALE = "STALE"                    # 断网时使用了陈旧副本
ERROR = "ERROR"                    # 断网且无法满足（504/502）或源站 5xx 透传
RANGE_HIT = "RANGE_HIT"            # 请求区间全部被已缓存片段覆盖，直接 206
RANGE_FILL = "RANGE_FILL"          # 区间有缺口，回源补齐后返回 206（含首次 206）
UNSATISFIABLE = "UNSATISFIABLE"    # 范围不可满足，416
STALE_WHILE_REVALIDATE = "STALE_WHILE_REVALIDATE"  # SWR 窗口内返回陈旧副本，后台重验证
STALE_IF_ERROR = "STALE_IF_ERROR"  # 回源失败（5xx），按 stale-if-error 回退陈旧副本

# 状态 JSON 中 bytes 的编解码标记（状态可整体落 SQLite，重启后恢复）
BYTES_KEY = "__bytes_b64__"


def to_b64(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def from_b64(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"), validate=True)


def _json_default(o):
    if isinstance(o, (bytes, bytearray)):
        return {BYTES_KEY: to_b64(bytes(o))}
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _json_hook(d):
    if set(d.keys()) == {BYTES_KEY}:
        return from_b64(d[BYTES_KEY])
    return d


def to_json(obj) -> str:
    """状态/事件落库用的 JSON：bytes 以 base64 标记无损保存。"""
    return json.dumps(obj, ensure_ascii=False, default=_json_default)


def from_json(s):
    return json.loads(s, object_hook=_json_hook)


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
            # 字节范围相关
            "range_requests": 0,            # 携带单区间 Range 的 GET 请求数
            "range_hits": 0,                # 完全由缓存片段满足的 206 次数
            "range_fills": 0,               # 回源补缺口的 206 次数
            "unsatisfiable": 0,             # 416 次数
            "range_fetches": 0,             # 回源 Range 子请求次数
            "range_bytes_from_origin": 0,   # 回源拿到的 206 响应体字节
            "segments_merged": 0,           # 片段合并次数
            "segments_invalidated": 0,      # 验证器变化丢弃片段的次数
            # 陈旧容错与请求合并
            "background_revalidations": 0,  # 调度过的后台重验证作业数
            "jobs_settled": 0,              # 已结算的后台作业数
            "jobs_failed": 0,               # 结算时失败（断网/源站故障/5xx）的作业数
            "coalesced_requests": 0,        # 挂接到在途作业的请求数
            "stale_while_revalidate": 0,    # SWR 窗口内直接返回陈旧副本的次数
            "stale_if_error": 0,            # 源站 5xx 后按 stale-if-error 回退的次数
            "origin_fetches_saved": 0,      # 因请求合并节省的回源次数
            "origin_errors": 0,             # 源站连接失败 / 5xx 次数
        },
        "results": [],
        "executed_event_ids": [],
        "jobs": [],             # 待结算的后台重验证作业（随虚拟时钟推进）
        "job_seq": 0,           # 作业 id 序号
        "job_log": [],          # 最近结算的作业结果（最多保留 50 条）
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


def body_len(body) -> int:
    if body is None:
        return 0
    if isinstance(body, str):
        return len(body.encode("utf-8"))
    return len(body)


def wire_size(status: int, headers: dict, body) -> int:
    """仿真估算一次 HTTP 响应在线传输的字节数（状态行 + 响应头 + 响应体）。"""
    n = len(f"HTTP/1.1 {status}\r\n")
    for k, v in headers.items():
        n += len(f"{k}: {v}\r\n")
    n += 2
    n += body_len(body)
    return n


def norm_body(body=None, body_base64=None) -> bytes:
    """事件中的资源体统一成 bytes：优先 body_base64（二进制），否则 UTF-8 文本。"""
    if body_base64 is not None:
        try:
            return from_b64(str(body_base64))
        except Exception as e:
            raise ValueError(f"body_base64 不是合法的 Base64: {e}") from e
    if body is None:
        return b""
    if isinstance(body, (bytes, bytearray)):
        return bytes(body)
    if isinstance(body, str):
        return body.encode("utf-8")
    return json.dumps(body, ensure_ascii=False).encode("utf-8")


def _variant_key(selected: dict) -> str:
    return json.dumps(selected, sort_keys=True, ensure_ascii=False)


def _etag_values(header: str):
    if not header:
        return []
    return [x.strip() for x in header.split(",") if x.strip()]


def _is_strong_etag(tag: str | None) -> bool:
    return bool(tag) and not tag.strip().upper().startswith("W/")


def _etag_weak_equal(a: str, b: str) -> bool:
    """弱比较：剥掉 W/ 前缀后相等即可（RFC 7232）。"""
    if not a or not b:
        return False
    na = a.strip().upper().removeprefix("W/")
    nb = b.strip().upper().removeprefix("W/")
    return na == nb


def _etag_strong_equal(a: str | None, b: str | None) -> bool:
    """强比较：两边都必须是强 ETag 且逐字相等（RFC 7232，片段合并要求）。"""
    if not _is_strong_etag(a) or not _is_strong_etag(b):
        return False
    return a.strip() == b.strip()


def _lm_equal(a: str | None, b: str | None) -> bool:
    ta, tb = parse_time_value(a), parse_time_value(b)
    return ta is not None and tb is not None and ta == tb


def same_representation(h1: dict, h2: dict) -> tuple[bool, str]:
    """确认两份表示是否同一版本：优先强 ETag，其次 Last-Modified。

    返回 (是否一致, 依据说明)。无法确认（弱 ETag / 都缺失）一律按不一致处理。
    """
    e1, e2 = h1.get("etag"), h2.get("etag")
    if _is_strong_etag(e1) and _is_strong_etag(e2):
        ok = _etag_strong_equal(e1, e2)
        return ok, f"强 ETag {e1} {'==' if ok else '!='} {e2}"
    l1, l2 = h1.get("last-modified"), h2.get("last-modified")
    if l1 and l2:
        ok = _lm_equal(l1, l2)
        return ok, f"Last-Modified {l1} {'==' if ok else '!='} {l2}"
    have = []
    if e1 or e2:
        have.append("ETag 为弱验证器，不能用于片段合并")
    if not (l1 and l2):
        have.append("缺少 Last-Modified")
    return False, "无法确认表示一致（" + "；".join(have) + "）"


# ---------------------------------------------------------------- Range 解析

def parse_range_spec(header: str):
    """语法解析单区间 Range（不依赖资源长度）。

    返回 ("range", start, end, suffix)：start/end 可能为 None（open-ended），
    suffix 为 bytes=-N 形式的 N；("multi", None) 多区间（不支持，按普通 GET 转发）；
    ("invalid", None) 非法头（RFC 要求忽略，按普通 GET 处理）。
    """
    s = str(header).strip()
    if not s.lower().startswith("bytes="):
        return ("invalid", None)
    spec = s[len("bytes="):].strip()
    if "," in spec:
        return ("multi", None)
    if spec.count("-") != 1:
        return ("invalid", None)
    a, b = spec.split("-")
    a, b = a.strip(), b.strip()
    if a == "" and b == "":
        return ("invalid", None)
    try:
        if a == "":  # bytes=-N：最后 N 字节
            return ("range", None, None, int(b))
        start = int(a)
        end = int(b) if b else None
        if start < 0 or (end is not None and end < start):
            return ("invalid", None)
        return ("range", start, end, None)
    except ValueError:
        return ("invalid", None)


def resolve_range(spec, length: int):
    """把语法区间解析成闭区间 [first, last]；不可满足返回 None。"""
    _, start, end, suffix = spec
    if suffix is not None:
        if suffix <= 0 or length == 0:
            return None
        n = min(suffix, length)
        return length - n, length - 1
    first = start
    last = length - 1 if end is None else min(end, length - 1)
    if length == 0 or first >= length or first > last:
        return None
    return first, last


def parse_content_range(value: str):
    """解析 Content-Range: bytes start-end/total，返回 (start, end, total)。"""
    try:
        s = str(value).strip().lower().removeprefix("bytes").strip()
        rng, _, total = s.partition("/")
        a, b = rng.split("-")
        return int(a), int(b), int(total)
    except Exception:
        return None


# ---------------------------------------------------------------- 引擎

class Engine:
    def __init__(self, config: dict | None = None):
        self.config = copy.deepcopy(DEFAULT_CONFIG)
        if config:
            self.config.update(copy.deepcopy(config))
        self.state = new_state()

    def load_state(self, state: dict):
        # 兼容旧版本状态：补齐新增计数器/条目字段
        state = copy.deepcopy(state)
        defaults = new_state()
        for k, v in defaults["counters"].items():
            state.setdefault("counters", {}).setdefault(k, v)
        state.setdefault("origins", {})
        state.setdefault("cache", {})
        state.setdefault("results", [])
        state.setdefault("executed_event_ids", [])
        state.setdefault("jobs", [])
        state.setdefault("job_seq", 0)
        state.setdefault("job_log", [])
        self.state = state

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
        # 虚拟时钟推进到 at：先结算所有到期的后台重验证作业，再处理本事件
        self._settle_due_jobs(at)

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

    def _accept_ranges(self, event, headers) -> str:
        """归一化源站是否支持范围：'bytes' 或 'none'。默认支持（静态源站语义）。"""
        val = event.get("accept_ranges")
        if val is None:
            val = headers.get("accept-ranges")
        if val is None:
            return "bytes"
        if isinstance(val, bool):
            return "bytes" if val else "none"
        return "bytes" if str(val).strip().lower() != "none" else "none"

    def _origin_def(self, event: dict):
        url = event.get("url")
        if not url:
            raise ValueError("origin 事件需要 url 字段")
        headers = norm_headers(event.get("headers"))
        if event.get("etag") is not None and "etag" not in headers:
            headers["etag"] = str(event["etag"])
        if event.get("last_modified") is not None and "last-modified" not in headers:
            headers["last-modified"] = str(event["last_modified"])
        ar = self._accept_ranges(event, headers)
        if "accept-ranges" not in headers:
            headers["accept-ranges"] = ar
        delay = event.get("delay", 0)
        try:
            delay = float(delay or 0)
        except (TypeError, ValueError):
            raise ValueError(f"delay 必须是数字秒数: {delay!r}")
        if delay < 0:
            raise ValueError("delay 不能为负")
        return url, {
            "status": int(event.get("status", 200)),
            "headers": headers,
            "body": norm_body(event.get("body", ""), event.get("body_base64")),
            "accept_ranges": ar,
            # 故障仿真：响应延迟（虚拟秒）/ 连接失败
            "delay": delay,
            "fail": bool(event.get("fail", False)),
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
        if event.get("body") is not None or event.get("body_base64") is not None:
            cur["body"] = norm_body(event.get("body"), event.get("body_base64"))
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
        if event.get("accept_ranges") is not None:
            cur["accept_ranges"] = ("bytes" if event["accept_ranges"]
                                    else "none") if isinstance(
                event["accept_ranges"], bool) else str(event["accept_ranges"]).strip().lower()
            cur["headers"]["accept-ranges"] = cur["accept_ranges"]
        if event.get("delay") is not None:
            d = float(event["delay"])
            if d < 0:
                raise ValueError("delay 不能为负")
            cur["delay"] = d
        if event.get("fail") is not None:
            cur["fail"] = bool(event["fail"])
        return {"type": "origin_change", "at": at, "url": url,
                "before": before, "after": self._resource_summary(cur)}

    @staticmethod
    def _resource_summary(d):
        return {
            "status": d["status"],
            "etag": d["headers"].get("etag"),
            "last_modified": d["headers"].get("last-modified"),
            "cache_control": d["headers"].get("cache-control"),
            "accept_ranges": d.get("accept_ranges",
                                   d["headers"].get("accept-ranges")),
            "delay": d.get("delay", 0),
            "fail": bool(d.get("fail", False)),
            "body_bytes": body_len(d.get("body", b"")),
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

        # Range 语法解析（只支持单区间；多区间/非法头按普通 GET 处理）
        range_spec = None
        if method == "GET" and "range" in req_headers:
            parsed = parse_range_spec(req_headers["range"])
            if parsed[0] == "range":
                range_spec = parsed
                counters["range_requests"] += 1
                log("RANGE_REQUEST", f"单区间 Range: {req_headers['range']}")
            elif parsed[0] == "multi":
                log("RANGE_MULTI_UNSUPPORTED",
                    f"多区间 Range {req_headers['range']} 超出仿真范围，按完整 GET 转发")
            else:
                log("RANGE_PARSE_IGNORE",
                    f"非法 Range {req_headers['range']}，RFC 要求忽略，按完整 GET 处理")
        want_range = range_spec is not None
        if_range = req_headers.get("if-range")
        if if_range is not None:
            log("IF_RANGE", f"请求带 If-Range: {if_range}")
        if client_conditional:
            log("CLIENT_CONDITIONAL", "客户端自带条件请求，将转发到源站")

        # only-if-cached：只允许使用缓存（含陈旧），无缓存则 504，绝不回源
        if req_cc.get("only-if-cached"):
            bucket = self.state["cache"].get(cache_key)
            entry = self._select_variant(bucket, req_headers, trace) if bucket and not req_no_store else None
            if entry is not None:
                age = self._entry_age(entry, at)
                stale = not self._is_fresh(entry, age)
                if want_range:
                    served, rinfo = self._serve_cached_range(
                        entry, range_spec, age, at, stale=stale)
                    if served is not None:
                        log("ONLY_IF_CACHED",
                            f"only-if-cached：区间已被缓存片段覆盖，直接返回 206（age={age:.0f}s）")
                        return self._done(counters, served, RANGE_HIT, at, url, method,
                                          req_headers, trace, entry, origin_used=False,
                                          range_info=rinfo)
                    log("ONLY_IF_CACHED_FAIL",
                        "only-if-cached 且缓存片段存在缺口，返回 504（不回源）")
                    served = self._gateway_error(
                        504, "Gateway Timeout (only-if-cached, range not fully cached)")
                    return self._done(counters, served, ERROR, at, url, method,
                                      req_headers, trace, entry, origin_used=False)
                served = self._entry_response(entry, age, at, stale=stale)
                log("ONLY_IF_CACHED",
                    f"only-if-cached：直接返回缓存副本（age={age:.0f}s，{'陈旧' if stale else '新鲜'}）")
                return self._done(counters, served, HIT, at, url, method,
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

        if entry is None and not client_conditional:
            log("CACHE_MISS", "没有可用缓存条目（或请求 no-store / Vary 不匹配）")

        # range_plan：新鲜区间命中在此直接返回；其余情况交给路径 B
        # 取值：None / ("validate-covered", ...) / ("gaps", ...)
        range_plan = None
        force_full_get = False     # If-Range 明确不匹配：丢弃 Range，完整回源
        full_get = False  # 完整 GET 命中仅含片段的条目：补齐后返回完整 200
        if entry is not None and not client_conditional:
            age = self._entry_age(entry, at)
            log("CACHE_ENTRY",
                f"age={age:.0f}s ttl={self._fmt_ttl(entry)} no_cache={entry['no_cache']} "
                f"must_revalidate={entry['must_revalidate']}"
                + (f" partial={entry.get('partial', False)} "
                   f"segments={self._segments_view(entry)}"
                   if (want_range or entry.get("partial")) else ""))
            decision = self._freshness_decision(entry, age, req_cc, trace)

            # 完整 GET 命中“只有部分片段”的条目：把请求当作 [0, length-1] 区间
            if not want_range and entry.get("partial"):
                want_range = True
                interval = (0, entry["length"] - 1)
                covered, gaps = self._coverage(entry, *interval)
                rinfo_base = {"requested": None, "full_get": True,
                              "first": 0, "last": entry["length"] - 1,
                              "length": entry["length"], "covered": covered, "gaps": gaps,
                              "segments_before": self._segments_view(entry)}
                if not gaps and decision == "fresh":
                    # 片段已拼满（理论上 partial 已升级，留作防御）
                    served = self._entry_response(entry, age, at)
                    log("PARTIAL_COMPLETE_HIT", "片段已覆盖完整表示，直接返回 200")
                    return self._done(counters, served, HIT, at, url, method,
                                      req_headers, trace, entry, origin_used=False,
                                      range_info={**rinfo_base, "served_from": "cache"})
                range_plan = ("gaps", entry, interval, gaps, rinfo_base,
                              decision != "fresh", age, True)
                log("PARTIAL_FULL_GET",
                    f"完整 GET 但缓存只有片段，缺口 {gaps}，回源补齐后返回完整 200"
                    if gaps else "完整 GET，片段覆盖完整表示但需重验证")
            elif want_range:
                # 先判定 If-Range：不匹配时（即便区间越界）按 RFC 7233 忽略 Range，
                # 走完整 GET 回源，绝不能先由缓存生成 416
                ir = None
                if if_range is not None:
                    ir = self._if_range_matches(if_range, entry["headers"])
                    if ir is False:
                        log("IF_RANGE_MISMATCH",
                            f"If-Range {if_range} 与缓存验证器不一致，"
                            "忽略 Range（不判定区间是否越界）回源请求完整表示")
                    elif ir:
                        log("IF_RANGE_MATCH",
                            f"If-Range {if_range} 与缓存验证器一致，按区间处理")
                    else:
                        log("IF_RANGE_UNKNOWN", "缓存无可用验证器，If-Range 交源站判定")

                if ir is False:
                    # 明确不匹配：直接走完整 GET 路径（剥离 Range）
                    force_full_get = True
                    want_range = False
                else:
                    interval = resolve_range(range_spec, entry["length"])
                    if interval is None:
                        # 相对已知表示长度不可满足：缓存可直接生成 416
                        served = self._range_416(entry["length"])
                        counters["unsatisfiable"] += 1
                        log("RANGE_416",
                            f"请求区间相对缓存长度 {entry['length']} 不可满足，返回 416（不回源）")
                        return self._done(counters, served, UNSATISFIABLE, at, url, method,
                                          req_headers, trace, entry, origin_used=False,
                                          range_info={"requested": req_headers["range"],
                                                      "length": entry["length"],
                                                      "served_from": "cache"})

                    covered, gaps = self._coverage(entry, *interval)
                    rinfo_base = {"requested": req_headers["range"], "first": interval[0],
                                  "last": interval[1], "length": entry["length"],
                                  "covered": covered, "gaps": gaps,
                                  "segments_before": self._segments_view(entry)}
                    if not gaps:
                        if decision == "fresh":
                            served, rinfo = self._serve_cached_range(
                                entry, range_spec, age, at)
                            rinfo = {**rinfo_base, **rinfo, "served_from": "cache"}
                            log("RANGE_COVERED",
                                f"区间 [{interval[0]},{interval[1]}] 全部被缓存片段覆盖，"
                                "直接返回 206（命中区间=%s）" % covered)
                            return self._done(counters, served, RANGE_HIT, at, url, method,
                                              req_headers, trace, entry, origin_used=False,
                                              range_info=rinfo)
                        # 已陈旧但区间完整覆盖：需要条件重验证
                        range_plan = ("validate-covered", entry, interval, rinfo_base, age)
                    else:
                        need_validate = decision != "fresh"
                        # 重新选取条目：覆盖区间的判定过程中条目可能已被此前事件替换
                        entry = self._find_variant(cache_key, req_headers) or entry
                        range_plan = ("gaps", entry, interval, gaps, rinfo_base,
                                      need_validate, age, False)
                        if need_validate:
                            log("RANGE_GAPS_STALE",
                                f"区间缺口 {gaps} 且缓存需重验证，"
                                "将带 If-Range 回源补齐（验证器变化则源站返回完整 200）")
                        else:
                            log("RANGE_GAPS", f"区间缺口 {gaps}，只需回源补齐缺失字节")
            elif decision == "fresh":
                served = self._entry_response(entry, age, at)
                return self._done(counters, served, HIT, at, url, method,
                                  req_headers, trace, entry, origin_used=False)
            elif self._swr_allows(entry, age, decision, req_cc):
                # stale-while-revalidate 窗口内：直接返回陈旧副本，
                # 同一缓存键 + Vary 变体只调度/挂接一个后台重验证作业
                return self._serve_stale_while_revalidate(
                    counters, entry, age, at, url, method, req_headers, trace,
                    cache_key)
            # stale-ok / stale-must：在线回源重验证，断网在路径 B 分流

        # ---- 路径 B：回源（MISS / 重验证 / 区间补齐 / 客户端条件请求） ----
        origin = self.state["origins"].get(url)
        # 连接失败分流：整体断网，或源站定义了 fail=true（按该源站连接失败处理）
        conn_fail = None
        if not self.state["network_up"]:
            conn_fail = "网络断开"
        elif origin is not None and origin.get("fail"):
            conn_fail = "源站连接失败（fail=true）"
            log("ORIGIN_CONN_FAIL", conn_fail + "，走连接失败容错路径")

        if conn_fail is not None:
            if want_range and range_plan is not None and range_plan[0] == "validate-covered":
                _, entry, interval, rinfo_base, _ = range_plan
                age = self._entry_age(entry, at)
                if self._conn_fail_fallback(entry, age, req_cc):
                    served, rinfo = self._serve_cached_range(
                        entry, range_spec, age, at, stale=True, interval=interval)
                    rinfo = {**rinfo_base, **rinfo, "served_from": "cache-stale"}
                    log("NETWORK_DOWN_STALE",
                        f"{conn_fail}，使用陈旧缓存片段返回 206（带 Warning: 110）")
                    return self._done(counters, served, STALE, at, url, method,
                                      req_headers, trace, entry, origin_used=False,
                                      range_info=rinfo)
                log("NETWORK_DOWN_FAIL",
                    f"{conn_fail}，区间虽被陈旧片段覆盖但 must-revalidate/no-cache 禁止兜底"
                    "或已超出 stale-if-error 窗口，返回 504")
                served = self._gateway_error(504, "Gateway Timeout (stale range must revalidate)")
                return self._done(counters, served, ERROR, at, url, method,
                                  req_headers, trace, entry, origin_used=False)
            if want_range and range_plan is not None:
                # gaps（含完整 GET 只有片段）：缺字节断网无法补齐
                plan_entry = range_plan[1]
                if range_plan[7]:
                    # 完整 GET 上的陈旧片段：允许 STALE 兜底（返回不完整内容，trace 标注）
                    age = self._entry_age(plan_entry, at)
                    if self._conn_fail_fallback(plan_entry, age, req_cc):
                        served = self._entry_response(plan_entry, age, at, stale=True)
                        log("NETWORK_DOWN_STALE",
                            f"{conn_fail}，完整 GET 仅有部分片段，按陈旧副本兜底"
                            "（内容不完整，已带 Warning: 110）")
                        return self._done(counters, served, STALE, at, url, method,
                                          req_headers, trace, plan_entry, origin_used=False,
                                          range_info={"full_get": True, "incomplete": True,
                                                      "gaps": range_plan[3]})
                log("NETWORK_DOWN_FAIL",
                    f"{conn_fail}且请求区间未被缓存片段完整覆盖（存在缺口），返回 504")
                served = self._gateway_error(504, "Gateway Timeout (range gaps, network down)")
                return self._done(counters, served, ERROR, at, url, method,
                                  req_headers, trace, plan_entry, origin_used=False)
            if entry is not None:
                age = self._entry_age(entry, at)
                if self._conn_fail_fallback(entry, age, req_cc):
                    served = self._entry_response(entry, age, at, stale=True)
                    log("NETWORK_DOWN_STALE", f"{conn_fail}，使用陈旧缓存副本（带 Warning: 110）")
                    return self._done(counters, served, STALE, at, url, method,
                                      req_headers, trace, entry, origin_used=False)
            log("NETWORK_DOWN_FAIL",
                f"{conn_fail}且无可用/不允许使用的缓存副本（或超出 stale-if-error 窗口），返回 504")
            served = self._gateway_error(504, "Gateway Timeout (simulated network down)")
            return self._done(counters, served, ERROR, at, url, method,
                              req_headers, trace, entry, origin_used=False)

        # ---- 在途 SWR 作业：不能/不应直接返回陈旧副本的请求挂接并等待 ----
        # 走到这里的请求未命中 SWR 分支（no-cache/must-revalidate/显式新鲜度约束
        # 或窗口外），若同一缓存键 + Vary 变体已有在途重验证作业，则等待并复用
        # 其结果，保证同一虚拟时刻只重验证一次。Range 与 If-Range 路径沿用现有规则。
        if (entry is not None and not client_conditional
                and not want_range and not force_full_get):
            pending_job = self._find_pending_job(cache_key, entry["variant_key"])
            if pending_job is not None:
                waited = self._wait_for_job(counters, pending_job, at, url,
                                            method, req_headers, req_cc, trace,
                                            cache_key)
                if waited is not None:
                    return waited
                # 作业结果不可用（条目已失效等）：按当前缓存状态继续常规路径
                entry = self._find_variant(cache_key, req_headers)

        # ---- B-1：分段缓存的缺口补齐 / 陈旧区间重验证 ----
        if want_range and range_plan is not None:
            return self._range_origin_path(
                range_plan, origin, url, method, at, req_headers, req_cc,
                req_no_store, if_range, trace, counters, cache_key)

        # 构造发往源站的请求头；重验证时附加缓存验证器
        forwarded = dict(req_headers)

        # 客户端 If-Range 与缓存验证器明确不符：完整回源（剥离 Range/If-Range）
        strip_range = force_full_get
        if strip_range:
            forwarded.pop("range", None)
            forwarded.pop("if-range", None)

        conditional_sent = False
        if entry is not None and not client_conditional and not strip_range:
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
            origin, forwarded, at, trace)
        origin_resp = {"status": origin_status, "headers": origin_headers, "body": origin_body}
        self._count_origin_response(counters, origin_status, origin_headers, origin_body,
                                    range_hdr=forwarded.get("range"))
        # 源站响应延迟：响应在 at+delay 才到达，缓存条目的寿命从到达时刻起算
        delay = self._origin_delay(origin)
        eff_at = at + delay

        # ---- 5xx：源站错误不失效已有缓存；stale-if-error 窗口内回退陈旧副本 ----
        if origin_status >= 500:
            counters["origin_errors"] += 1
            log("ORIGIN_5XX", f"源站返回 {origin_status}（5xx 不使已有缓存条目失效）")
            if entry is not None and not client_conditional:
                age = self._entry_age(entry, at)
                if self._sie_allows(entry, age, req_cc):
                    counters["stale_if_error"] += 1
                    excess = age - (entry["ttl"] or 0.0)
                    served = self._entry_response(
                        entry, age, at, stale=True,
                        warning='110 cache-sim "Response is stale (stale-if-error: origin 5xx)"')
                    log("STALE_IF_ERROR",
                        f"陈旧 {excess:.0f}s 在 stale-if-error={entry['sie']:.0f}s 窗口内，"
                        f"回退陈旧副本（源站 {origin_status}，带 Warning: 110）")
                    return self._done(
                        counters, served, STALE_IF_ERROR, at, url, method,
                        req_headers, trace, entry, origin_used=True,
                        origin_status=origin_status, origin_resp=origin_resp,
                        stale_info={"kind": "stale-if-error",
                                    "window": entry["sie"], "excess": excess,
                                    "origin_status": origin_status})
                log("NO_STALE_FALLBACK",
                    "无 stale-if-error 窗口（或 must-revalidate/no-cache 禁止），透传源站错误")
            return self._done(counters, origin_resp, ERROR, at, url, method,
                              req_headers, trace, entry, origin_used=True,
                              origin_status=origin_status, origin_resp=origin_resp)

        # ---- 304 ----
        if origin_status == 304:
            counters["not_modified"] += 1
            if client_conditional:
                if entry is not None:
                    self._merge_304(entry, origin_headers, eff_at, trace)
                log("CLIENT_NOT_MODIFIED", "源站 304：透传给客户端")
                return self._done(counters, origin_resp, NOT_MODIFIED, at, url, method,
                                  req_headers, trace, entry, origin_used=True,
                                  origin_status=304, origin_resp=origin_resp)
            if entry is not None and not entry.get("partial"):
                self._merge_304(entry, origin_headers, eff_at, trace)
                age = self._entry_age(entry, at)
                served = self._entry_response(entry, age, at)
                log("NOT_MODIFIED", "源站 304：合并校验元数据，继续使用缓存副本")
                return self._done(counters, served, REVALIDATED, at, url, method,
                                  req_headers, trace, entry, origin_used=True,
                                  origin_status=304, origin_resp=origin_resp,
                                  origin_delay=delay)
            if entry is not None and entry.get("partial") and "range" not in forwarded:
                # 缓存里只有部分片段，非区间 GET 不能凭 304 返回空 200：
                # 去掉条件头完整回源一次
                log("PARTIAL_304_FULL",
                    "源站 304 但缓存仅有部分片段且请求是完整 GET：去掉条件头再取完整表示")
                fwd2 = {k: v for k, v in forwarded.items()
                        if k not in ("if-none-match", "if-modified-since")}
                counters["origin_fetches"] += 1
                st2, hd2, bd2 = self._fetch_origin(origin, fwd2, at, trace)
                self._count_origin_response(counters, st2, hd2, bd2)
                full_resp = {"status": st2, "headers": hd2, "body": bd2}
                if st2 == 200:
                    return self._full_replaces_for_range(
                        cache_key, full_resp, entry, url, method, eff_at, req_headers,
                        req_no_store, trace, counters,
                        reason="部分片段 + 完整 GET：验证器未变，补齐为完整表示")
            log("CLIENT_NOT_MODIFIED", "无（完整）缓存条目却收到 304，原样透传")
            return self._done(counters, origin_resp, NOT_MODIFIED, at, url, method,
                              req_headers, trace, entry, origin_used=True,
                              origin_status=304, origin_resp=origin_resp)

        # ---- 206：分段缓存路径（无缓存条目 / 客户端条件请求 / range_cache 关闭） ----
        if origin_status == 206 and want_range:
            return self._handle_origin_206(
                cache_key, origin_resp, origin, url, method, eff_at, req_headers,
                req_no_store, trace, counters, entry,
                client_conditional=client_conditional)

        # ---- 416：范围不可满足（无缓存长度可依，由源站判定） ----
        if origin_status == 416 and want_range:
            counters["unsatisfiable"] += 1
            log("RANGE_416", "源站返回 416：范围不可满足")
            rinfo = {"requested": req_headers.get("range"), "served_from": "origin-416"}
            return self._done(counters, origin_resp, UNSATISFIABLE, at, url, method,
                              req_headers, trace, entry, origin_used=True,
                              origin_status=416, origin_resp=origin_resp,
                              range_info=rinfo)

        # ---- 完整响应：尝试写缓存 ----
        why_not = None
        stored_entry = None
        old_partial = None
        if method == "GET" and not req_no_store:
            # 若同一变体此前只缓存了部分片段，记录完整响应与其验证器的关系
            cur = self._find_variant(cache_key, req_headers)
            old_partial = cur if cur is not None and cur.get("partial") else None
            if old_partial is not None:
                ok, basis = same_representation(old_partial["headers"], origin_headers)
                if ok:
                    log("PARTIAL_SUPERSEDED",
                        f"完整 200 与片段验证器一致（{basis}），分段缓存被完整表示替代")
                else:
                    counters["segments_invalidated"] += 1
                    log("INVALIDATE_SEGMENTS",
                        f"完整 200 的验证器已变化（{basis}），丢弃旧片段")
            store_info = self._try_store(cache_key, origin_resp, req_headers, eff_at, trace)
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
            if entry is not None or old_partial is not None:
                verdict = REFRESHED
                log("REFRESHED", "回源返回完整新响应，缓存条目已替换")
            else:
                verdict = MISS
                log("MISS_STORED", "回源完整响应已写入缓存")
        else:
            verdict = UNCACHEABLE
            log("UNCACHEABLE", why_not)

        after_entry = stored_entry
        if after_entry is None and entry is not None and self._variant_exists(cache_key, entry):
            after_entry = entry
        range_info = None
        if "range" in req_headers:
            # 源站忽略了 Range（不支持/If-Range 不符），返回完整 200
            range_info = {"requested": req_headers["range"],
                          "served_full_fallback": True,
                          "reason": ("If-Range 不匹配" if strip_range else "源站未按 Range 返回 206"),
                          "segments_after": self._segments_view(after_entry)}
        return self._done(counters, served, verdict, at, url, method,
                          req_headers, trace, after_entry,
                          origin_used=True, origin_status=origin_status,
                          origin_resp=origin_resp, range_info=range_info,
                          origin_delay=delay)

    # ---- 字节范围：回源补齐 / 重验证 -------------------------------------

    def _range_origin_path(self, plan, origin, url, method, at, req_headers,
                           req_cc, req_no_store, if_range, trace, counters, cache_key):
        kind_p = plan[0]
        if origin is None:
            self._trace(trace, "ORIGIN_MISSING", f"源站未定义资源 {url}，仿真返回 502")
            served = self._gateway_error(502, "Bad Gateway (origin resource not defined in scenario)")
            return self._done(counters, served, ERROR, at, url, method,
                              req_headers, trace, plan[1], origin_used=True,
                              origin_status=502, origin_resp=served)
        delay = self._origin_delay(origin)
        eff_at = at + delay

        if kind_p == "validate-covered":
            _, entry, interval, rinfo_base, age0 = plan
            # 区间完整覆盖但缓存陈旧：发条件请求（不带 Range）
            fwd = dict(req_headers)
            fwd.pop("range", None)
            if entry["headers"].get("etag"):
                fwd["if-none-match"] = entry["headers"]["etag"]
            elif entry["headers"].get("last-modified"):
                fwd["if-modified-since"] = entry["headers"]["last-modified"]
            self._trace(trace, "REVALIDATE",
                f"区间已覆盖但缓存陈旧，条件重验证 { {k: v for k, v in fwd.items() if k.startswith('if-') and k != 'if-range'} }")
            counters["origin_fetches"] += 1
            counters["revalidations"] += 1
            st, hd, bd = self._fetch_origin(origin, fwd, at, trace)
            self._count_origin_response(counters, st, hd, bd)
            if st == 304:
                counters["not_modified"] += 1
                self._merge_304(entry, hd, eff_at, trace)
                age = self._entry_age(entry, at)
                served, rinfo = self._serve_cached_range(entry, None, age, at,
                                                         interval=interval)
                rinfo = {**rinfo_base, **rinfo, "served_from": "cache-after-304"}
                self._trace(trace, "RANGE_REVALIDATED", "源站 304：片段仍有效，用缓存片段拼出 206")
                return self._done(counters, served, REVALIDATED, at, url, method,
                                  req_headers, trace, entry, origin_used=True,
                                  origin_status=304,
                                  origin_resp={"status": 304, "headers": hd, "body": bd},
                                  range_info=rinfo)
            if st == 200:
                return self._full_replaces_for_range(
                    cache_key, {"status": 200, "headers": hd, "body": bd},
                    entry, url, method, eff_at, req_headers, req_no_store, trace,
                    counters, reason="陈旧重验证返回完整 200（验证器变化）")
            if st >= 500:
                counters["origin_errors"] += 1
                age = self._entry_age(entry, at)
                if self._sie_allows(entry, age, req_cc):
                    counters["stale_if_error"] += 1
                    served, rinfo = self._serve_cached_range(
                        entry, None, age, at, stale=True, interval=interval,
                        warning='110 cache-sim "Response is stale (stale-if-error: origin 5xx)"')
                    rinfo = {**rinfo_base, **rinfo, "served_from": "cache-stale-if-error"}
                    self._trace(trace, "STALE_IF_ERROR",
                        f"源站 {st}，陈旧 {age - (entry['ttl'] or 0.0):.0f}s 在 "
                        f"stale-if-error={entry['sie']:.0f}s 窗口内，用缓存片段拼出 206")
                    return self._done(
                        counters, served, STALE_IF_ERROR, at, url, method,
                        req_headers, trace, entry, origin_used=True, origin_status=st,
                        origin_resp={"status": st, "headers": hd, "body": bd},
                        range_info=rinfo,
                        stale_info={"kind": "stale-if-error", "window": entry["sie"],
                                    "excess": age - (entry["ttl"] or 0.0),
                                    "origin_status": st})
                self._trace(trace, "ORIGIN_5XX",
                            f"源站 {st} 且无 stale-if-error 回退，返回 504")
                served = self._gateway_error(
                    504, f"Gateway Timeout (origin {st}, no stale-if-error fallback)")
                return self._done(counters, served, ERROR, at, url, method,
                                  req_headers, trace, entry, origin_used=True,
                                  origin_status=st,
                                  origin_resp={"status": st, "headers": hd, "body": bd})
            served = self._gateway_error(502, f"Unexpected origin status {st} for range revalidation")
            return self._done(counters, served, ERROR, at, url, method,
                              req_headers, trace, entry, origin_used=True)

        # kind == "gaps"
        _, entry, interval, gaps, rinfo_base, need_validate, age0, full_get = plan
        # 分段缓存开关关闭：直接转发客户端 Range，206 不落缓存
        if not self.config["range_cache"]:
            self._trace(trace, "RANGE_CACHE_DISABLED",
                "配置 range_cache=false：不补齐/合片段，Range 直接透传源站")
            fwd = dict(req_headers)
            counters["origin_fetches"] += 1
            st, hd, bd = self._fetch_origin(origin, fwd, at, trace)
            self._count_origin_response(counters, st, hd, bd, range_hdr=fwd.get("range"))
            served = {"status": st, "headers": hd, "body": bd}
            rinfo = {**rinfo_base, "served_from": "origin",
                     "stored": False, "segments_after": self._segments_view(entry)}
            verdict = RANGE_FILL if st == 206 else (UNSATISFIABLE if st == 416 else REFRESHED)
            if st == 416:
                counters["unsatisfiable"] += 1
            return self._done(counters, served, verdict, at, url, method,
                              req_headers, trace, entry, origin_used=True,
                              origin_status=st, origin_resp=served, range_info=rinfo)

        validator_header = None
        if need_validate:
            et = entry["headers"].get("etag")
            if _is_strong_etag(et):
                validator_header = ("if-range", et)
            elif entry["headers"].get("last-modified"):
                validator_header = ("if-range", entry["headers"]["last-modified"])

        # 逐段回源补齐（每个缺口是一个单区间请求）；同时累计本次实际回源字节
        fetched = []
        origin_bytes_actual = 0
        for gi, (gs, ge) in enumerate(gaps):
            fwd = {k: v for k, v in req_headers.items()
                   if k not in ("range", "if-range", "if-none-match", "if-modified-since")}
            fwd["range"] = f"bytes={gs}-{ge}"
            if validator_header is not None:
                fwd["if-range"] = validator_header[1]
            counters["origin_fetches"] += 1
            counters["range_fetches"] += 1
            self._trace(trace, "RANGE_FETCH",
                f"缺口 [{gs},{ge}]（{ge - gs + 1} 字节）回源"
                + (f"，附带 {validator_header[0]}: {validator_header[1]}"
                   if validator_header else ""))
            st, hd, bd = self._fetch_origin(origin, fwd, at, trace)
            wb = wire_size(st, hd, bd)
            origin_bytes_actual += wb
            self._count_origin_response(counters, st, hd, bd, range_hdr=fwd["range"])

            if st == 200:
                # If-Range 不匹配：源站忽略 Range，返回完整表示。丢弃旧片段
                return self._full_replaces_for_range(
                    cache_key, {"status": 200, "headers": hd, "body": bd},
                    entry, url, method, eff_at, req_headers, req_no_store, trace,
                    counters, reason="回源补缺口时验证器已变化（If-Range 不匹配）")
            if st == 416:
                counters["unsatisfiable"] += 1
                served = {"status": 416, "headers": hd, "body": bd}
                self._trace(trace, "RANGE_416", "补齐缺口时源站返回 416")
                return self._done(counters, served, UNSATISFIABLE, at, url, method,
                                  req_headers, trace, entry, origin_used=True,
                                  origin_status=416, origin_resp=served,
                                  range_info={**rinfo_base, "served_from": "origin-416"})
            if st >= 500:
                counters["origin_errors"] += 1
                self._trace(trace, "ORIGIN_5XX",
                    f"补齐缺口时源站返回 {st}（5xx 不失效旧片段），无法补齐，返回 504")
                served = self._gateway_error(
                    504, f"Gateway Timeout (origin {st} during range fill)")
                return self._done(counters, served, ERROR, at, url, method,
                                  req_headers, trace, entry, origin_used=True,
                                  origin_status=st,
                                  origin_resp={"status": st, "headers": hd, "body": bd})
            if st != 206:
                served = self._gateway_error(502, f"Unexpected origin status {st} during range fill")
                return self._done(counters, served, ERROR, at, url, method,
                                  req_headers, trace, entry, origin_used=True)
            cr = parse_content_range(hd.get("content-range", ""))
            if cr is None:
                served = self._gateway_error(502, "206 缺少合法 Content-Range")
                return self._done(counters, served, ERROR, at, url, method,
                                  req_headers, trace, entry, origin_used=True)
            fetched.append((cr[0], cr[1], cr[2], bd, hd))

        # 合并片段（强验证器一致性已由 If-Range 路径保证，这里再确认一次）
        for fs, fe, flen, fb, fh in fetched:
            store_info = self._store_partial(
                cache_key,
                {"status": 206, "headers": fh,
                 "body": fb if len(fb) == fe - fs + 1 else fb[:fe - fs + 1]},
                (fs, fe, flen), req_headers, eff_at, trace, expected_entry=entry)
            if not store_info["stored"]:
                # 合并被拒绝（验证器不一致）：安全回退完整回源
                self._trace(trace, "RANGE_MERGE_REJECTED",
                    f"片段 [{fs},{fe}] 无法合并：{store_info['reason']}，回退完整回源")
                fwd = {k: v for k, v in req_headers.items()
                       if k not in ("range", "if-range")}
                counters["origin_fetches"] += 1
                st, hd, bd = self._fetch_origin(origin, fwd, at, trace)
                self._count_origin_response(counters, st, hd, bd)
                return self._full_replaces_for_range(
                    cache_key, {"status": st, "headers": hd, "body": bd},
                    entry, url, method, eff_at, req_headers, req_no_store, trace,
                    counters, reason=store_info["reason"])
            entry = store_info["entry"]

        counters["range_fills"] += 1
        age = self._entry_age(entry, at)
        covered, gaps_after = self._coverage(entry, *interval)
        fetched_ranges = [[fs, fe] for fs, fe, *_ in fetched]
        fetched_bytes = sum(fe - fs + 1 for fs, fe, *_ in fetched)
        segments_after = self._segments_view(entry)

        if full_get:
            # 完整 GET 命中部分片段：缺口补齐后应已拼满完整表示，返回 200
            served = self._entry_response(entry, age, at)
            rinfo = {**rinfo_base, "covered": covered, "gaps": gaps_after,
                     "fetched": fetched_ranges, "full_get": True,
                     "origin_bytes": origin_bytes_actual,
                     "origin_body_bytes": fetched_bytes,
                     "served_from": "cache+origin-fill",
                     "segments_after": segments_after}
            self._trace(trace, "RANGE_FILLED",
                f"完整 GET 缺口补齐完成，本次回源 {fetched_bytes} 正文节，"
                f"线传输估算 {origin_bytes_actual} 字节，合并后片段={segments_after}，返回完整 200")
            return self._done(counters, served, RANGE_FILL, at, url, method,
                              req_headers, trace, entry, origin_used=True,
                              origin_status=200, range_info=rinfo,
                              origin_bytes=origin_bytes_actual)

        served, rinfo = self._serve_cached_range(entry, None, age, at, interval=interval)
        rinfo = {**rinfo_base,
                 "covered": covered, "gaps": gaps_after,
                 "fetched": fetched_ranges,
                 "origin_bytes": origin_bytes_actual,
                 "origin_body_bytes": fetched_bytes,
                 "served_from": "cache+origin-fill",
                 "segments_after": segments_after,
                 **rinfo}
        self._trace(trace, "RANGE_FILLED",
            f"缺口补齐完成，本次回源 {fetched_bytes} 正文节，"
            f"线传输估算 {origin_bytes_actual} 字节，合并后片段={segments_after}")
        return self._done(counters, served, RANGE_FILL, at, url, method,
                          req_headers, trace, entry, origin_used=True,
                          range_info=rinfo, origin_bytes=origin_bytes_actual)

    def _full_replaces_for_range(self, cache_key, resp, entry, url, method, at,
                                 req_headers, req_no_store, trace, counters,
                                 reason):
        """区间请求路径上收到完整 200：按验证器关系丢弃旧片段，存完整表示，返回 200。"""
        old = self._find_variant(cache_key, req_headers) or entry
        if old is not None and old.get("partial"):
            ok, basis = same_representation(old["headers"], resp["headers"])
            if ok:
                self._trace(trace, "PARTIAL_SUPERSEDED", f"{reason}；验证器仍一致（{basis}），以完整表示替代片段")
            else:
                counters["segments_invalidated"] += 1
                self._trace(trace, "INVALIDATE_SEGMENTS",
                    f"{reason}；验证器变化（{basis}），旧片段全部丢弃")
        stored_entry = None
        why = None
        if method == "GET" and not req_no_store:
            info = self._try_store(cache_key, resp, req_headers, at, trace)
            if info["stored"]:
                stored_entry = info["entry"]
            else:
                why = info["reason"]
                if entry is not None:
                    self._invalidate_variant(cache_key, entry)
        served = resp
        if stored_entry is not None:
            verdict = REFRESHED
            self._trace(trace, "REFRESHED", "完整 200 已写入缓存（客户端得到完整表示，而非 206）")
        else:
            verdict = UNCACHEABLE
            self._trace(trace, "UNCACHEABLE", why or "完整响应不可缓存")
        rinfo = {"requested": req_headers.get("range"),
                 "served_full_fallback": True, "reason": reason,
                 "segments_after": self._segments_view(stored_entry)}
        return self._done(counters, served, verdict, at, url, method,
                          req_headers, trace, stored_entry or entry,
                          origin_used=True, origin_status=200,
                          origin_resp=resp, range_info=rinfo)

    def _handle_origin_206(self, cache_key, origin_resp, origin, url, method, at,
                           req_headers, req_no_store, trace, counters, entry,
                           client_conditional):
        """直接透传路径上源站返回的 206（无缓存条目 / 客户端条件 / 首次区间请求）。"""
        cr = parse_content_range(origin_resp["headers"].get("content-range", ""))
        stored_entry = None
        rinfo = {"requested": req_headers.get("range"), "served_from": "origin"}
        if cr is None:
            self._trace(trace, "RANGE_206_NO_CR", "206 缺少合法 Content-Range，不缓存")
        elif method != "GET" or req_no_store or not self.config["range_cache"]:
            if not self.config["range_cache"]:
                self._trace(trace, "RANGE_CACHE_DISABLED", "配置 range_cache=false：206 不写入分段缓存")
        else:
            info = self._store_partial(cache_key, origin_resp, cr,
                                       req_headers, at, trace)
            if info["stored"]:
                stored_entry = info["entry"]
                counters["range_fills"] += 1
                rinfo["stored"] = True
                rinfo["segments_after"] = self._segments_view(stored_entry)
            else:
                self._trace(trace, "RANGE_UNCACHEABLE", f"206 未缓存：{info['reason']}")
                rinfo["stored"] = False
                rinfo["reason"] = info["reason"]
        served = origin_resp
        verdict = RANGE_FILL
        self._trace(trace, "RANGE_SERVED_206", "源站返回 206 部分内容")
        return self._done(counters, served, verdict, at, url, method,
                          req_headers, trace, stored_entry, origin_used=True,
                          origin_status=206, origin_resp=origin_resp,
                          range_info=rinfo)

    # ---- 片段数据结构 / 合并 / 覆盖 --------------------------------------

    @staticmethod
    def _segments_view(entry):
        if entry is None:
            return []
        if not entry.get("partial"):
            return [[0, entry.get("length", body_len(entry.get("body"))) - 1]] \
                if entry.get("length") else []
        return [[s["start"], s["end"]] for s in entry.get("segments", [])]

    @staticmethod
    def _slice_segments(segments, first, last) -> bytes:
        buf = bytearray()
        for seg in sorted(segments, key=lambda s: s["start"]):
            if seg["end"] < first or seg["start"] > last:
                continue
            a = max(first, seg["start"])
            b = min(last, seg["end"])
            buf += seg["body"][a - seg["start"]:b - seg["start"] + 1]
        return bytes(buf)

    def _coverage(self, entry, first, last):
        """返回请求区间内已覆盖的子区间列表与缺口列表（均为闭区间）。"""
        if not entry.get("partial"):
            if entry.get("length", 0) >= last + 1:
                return [[first, last]], []
            return [], [[first, last]]
        covered, gaps = [], []
        cursor = first
        for seg in sorted(entry.get("segments", []), key=lambda s: s["start"]):
            s, e = max(seg["start"], first), min(seg["end"], last)
            if s > e:
                continue
            if s > cursor:
                gaps.append([cursor, s - 1])
            covered.append([s, e])
            cursor = max(cursor, e + 1)
        if cursor <= last:
            gaps.append([cursor, last])
        return covered, gaps

    def _merge_segments(self, existing, new_start, new_end, new_body, length,
                        headers, at, trace):
        """合并相邻/重叠片段。返回 (segments, complete, merged_count)。

        合并前提：新片段与缓存条目指向同一表示（调用方需先用强 ETag/Last-Modified
        确认），本函数只做区间代数。
        """
        segs = list(existing) + [{
            "start": new_start, "end": new_end, "body": bytes(new_body)}]
        segs.sort(key=lambda s: (s["start"], s["end"]))
        merged = []
        merges = 0
        for seg in segs:
            if merged and seg["start"] <= merged[-1]["end"] + 1:
                last_seg = merged[-1]
                if seg["start"] <= last_seg["end"] + 1 and seg["end"] <= last_seg["end"]:
                    # 完全落在已有片段内（重叠）
                    overlap = seg["start"] <= last_seg["end"]
                    if overlap:
                        merges += 1
                    continue
                overlap = seg["start"] <= last_seg["end"]
                offset = max(0, last_seg["end"] + 1 - seg["start"])
                last_seg["body"] += seg["body"][offset:]
                last_seg["end"] = seg["end"]
                merges += 1
                log_kind = "重叠" if overlap else "相邻"
                trace.append({"step": len(trace) + 1, "code": "MERGE_SEGMENTS",
                              "detail": f"{log_kind}片段合并：[{seg['start']},{seg['end']}] "
                                        f"并入 [{last_seg['start']},{last_seg['end']}]"})
            else:
                merged.append(dict(seg))
        complete = bool(merged) and merged[0]["start"] == 0 and \
            merged[-1]["end"] == length - 1 and len(merged) == 1
        return merged, complete, merges

    # ---- 206 存储 --------------------------------------------------------

    def _store_partial(self, cache_key, resp, cr, req_headers, at, trace,
                       expected_entry=None):
        """保存 206 部分响应；与同变体片段做验证器确认后合并。"""
        status, headers = resp["status"], resp["headers"]
        cc = parse_cc(headers.get("cache-control", ""))

        # 与 _try_store 相同的可缓存性门槛
        if "no-store" in cc:
            return {"stored": False, "reason": "响应 Cache-Control: no-store，禁止缓存"}
        if self.config["cache_mode"] == "shared" and "private" in cc:
            return {"stored": False, "reason": "共享缓存模式下响应 private，不允许缓存"}
        if headers.get("vary", "").strip() == "*":
            return {"stored": False, "reason": "Vary: * 表示不受限变体，不缓存"}
        if status not in self.config["cacheable_statuses"]:
            return {"stored": False,
                    "reason": f"状态码 {status} 不在可缓存列表 {self.config['cacheable_statuses']}"}

        # 片段合并必须能确认表示身份：强 ETag 或 Last-Modified
        strong = _is_strong_etag(headers.get("etag"))
        if not strong and not headers.get("last-modified"):
            return {"stored": False,
                    "reason": "206 分段缓存需要强 ETag 或 Last-Modified 验证器"}

        fs, fe, flen = cr
        body = resp["body"]
        if flen <= 0 or not (0 <= fs <= fe < flen):
            return {"stored": False, "reason": f"Content-Range 非法: {headers.get('content-range')}"}
        if body_len(body) < fe - fs + 1:
            return {"stored": False, "reason": "206 响应体短于 Content-Range 声明的区间"}
        body = bytes(body)[:fe - fs + 1]

        vary_raw = headers.get("vary")
        vary_headers = ([h.strip().lower() for h in vary_raw.split(",") if h.strip()]
                        if vary_raw else [])
        selected = {h: req_headers.get(h, "") for h in vary_headers}
        vkey = _variant_key(selected)

        bucket = self.state["cache"].setdefault(
            cache_key, {"vary": vary_headers, "variants": []})
        bucket["vary"] = vary_headers or bucket.get("vary", [])
        existing = next((v for v in bucket["variants"] if v["variant_key"] == vkey), None)

        # 已有完整表示：验证器一致则无需降级为片段
        if existing is not None and not existing.get("partial"):
            ok, basis = same_representation(existing["headers"], headers)
            if ok:
                return {"stored": False, "reason": f"已有完整表示且验证器一致（{basis}）",
                        "entry": existing}
            # 表示变了：旧完整条目失效，用新片段替换
            self.state["counters"]["segments_invalidated"] += 1
            trace.append({"step": len(trace) + 1, "code": "INVALIDATE_SEGMENTS",
                          "detail": f"新片段验证器变化（{basis}），丢弃旧完整条目"})
            bucket["variants"] = [v for v in bucket["variants"] if v["variant_key"] != vkey]
            existing = None

        if existing is not None:
            ok, basis = same_representation(existing["headers"], headers)
            if not ok:
                self.state["counters"]["segments_invalidated"] += 1
                trace.append({"step": len(trace) + 1, "code": "INVALIDATE_SEGMENTS",
                              "detail": f"验证器变化（{basis}），丢弃旧片段 "
                                        f"{self._segments_view(existing)}"})
                bucket["variants"] = [v for v in bucket["variants"]
                                      if v["variant_key"] != vkey]
                existing = None

        ttl = self._compute_ttl(headers, cc, at)
        age_raw = str(headers.get("age", ""))
        if existing is None:
            entry = {
                "variant_key": vkey,
                "status": 206,
                "headers": copy.deepcopy(headers),
                "stored_at": at,
                "init_age": int(age_raw) if age_raw.isdigit() else 0,
                "ttl": ttl,
                "no_cache": "no-cache" in cc,
                "must_revalidate": self._must_revalidate(cc),
                "rev_by": "proxy" if "proxy-revalidate" in cc else "must",
                "swr": self._cc_seconds(cc, "stale-while-revalidate"),
                "sie": self._cc_seconds(cc, "stale-if-error"),
                "partial": True,
                "length": flen,
                "segments": [{"start": fs, "end": fe, "body": body}],
                "body": b"",
            }
            bucket["variants"].append(entry)
            trace.append({"step": len(trace) + 1, "code": "RANGE_STORE",
                          "detail": f"写入分段缓存：[{fs},{fe}]/{flen}（{fe - fs + 1} 字节），"
                                    f"ttl={'None' if ttl is None else f'{ttl:.0f}s'} 验证器="
                                    f"{headers.get('etag') or headers.get('last-modified')}"})
            # 单个 206 已覆盖完整表示：直接升级为完整条目（后续无 Range 的 GET 可返回 200）
            if fs == 0 and fe == flen - 1:
                self._promote_complete(entry, merged_segments=entry["segments"], at=at,
                                       trace=trace)
            return {"stored": True, "entry": entry, "reason": None}

        # 与既有片段合并（验证器已确认一致）
        merged, complete, merges = self._merge_segments(
            existing["segments"], fs, fe, body, flen, headers, at, trace)
        self.state["counters"]["segments_merged"] += max(1, merges)
        existing["segments"] = merged
        existing["length"] = flen
        # 206 是部分响应，不构成对完整表示的重验证：新鲜度窗口沿用首个片段的
        # stored_at/ttl，不因后续补缺口而“续命”；只更新元数据头（ETag/LM 等）
        merged_headers = dict(existing["headers"])
        merged_headers.update(headers)
        existing["headers"] = merged_headers
        new_cc = parse_cc(merged_headers.get("cache-control", ""))
        if "no-cache" in new_cc:
            existing["no_cache"] = True
        if self._must_revalidate(new_cc):
            existing["must_revalidate"] = True
        new_swr = self._cc_seconds(new_cc, "stale-while-revalidate")
        if new_swr is not None:
            existing["swr"] = new_swr
        new_sie = self._cc_seconds(new_cc, "stale-if-error")
        if new_sie is not None:
            existing["sie"] = new_sie
        ttl_txt = "None" if existing["ttl"] is None else f"{existing['ttl']:.0f}s"
        trace.append({"step": len(trace) + 1, "code": "RANGE_MERGE_FRESHNESS",
                      "detail": "片段合并沿用原新鲜度窗口（206 不延长寿命），"
                                f"stored_at={existing['stored_at']:.0f} ttl={ttl_txt}"})

        if complete:
            self._promote_complete(existing, merged_segments=merged, at=at, trace=trace)
        return {"stored": True, "entry": existing, "reason": None}

    @staticmethod
    def _promote_complete(entry, merged_segments, at, trace):
        """片段已覆盖完整表示：升级为完整 200 条目。

        必须清掉部分响应专属的 Content-Range，并把 Content-Length 更新为完整长度，
        否则后续完整 GET 会错误地带着区间头返回。
        """
        flen = entry["length"]
        full = Engine._slice_segments(merged_segments, 0, flen - 1)
        entry["partial"] = False
        entry["body"] = full
        entry["status"] = 200
        entry["segments"] = merged_segments
        headers = dict(entry["headers"])
        removed_cr = headers.pop("content-range", None)
        headers["content-length"] = str(flen)
        entry["headers"] = headers
        trace.append({"step": len(trace) + 1, "code": "MERGE_COMPLETE",
                      "detail": f"片段已拼满 [0,{flen - 1}]，升级为完整表示（{flen} 字节）；"
                                + (f"移除残留 Content-Range（{removed_cr}），"
                                   if removed_cr else "")
                                + f"Content-Length 更新为 {flen}"})

    def _find_variant(self, cache_key, req_headers):
        bucket = self.state["cache"].get(cache_key)
        return self._select_variant(bucket, req_headers, []) if bucket else None

    # ---- 206 / 416 响应组装 ----------------------------------------------

    def _serve_cached_range(self, entry, range_spec, age, at, stale=False,
                            interval=None, warning=None):
        """用缓存条目（完整或片段）拼出 206；缺片段返回 (None, info)。"""
        if interval is None:
            interval = resolve_range(range_spec, entry["length"])
        first, last = interval
        covered, gaps = self._coverage(entry, first, last)
        if gaps:
            return None, {"first": first, "last": last, "covered": covered, "gaps": gaps}
        if entry.get("partial"):
            data = self._slice_segments(entry["segments"], first, last)
        else:
            data = entry["body"][first:last + 1]
        headers = copy.deepcopy(entry["headers"])
        headers["content-range"] = f"bytes {first}-{last}/{entry['length']}"
        headers["accept-ranges"] = "bytes"
        headers["content-length"] = str(last - first + 1)
        headers["age"] = str(int(max(0, age)))
        if stale:
            headers["warning"] = warning or \
                '110 cache-sim "Response is stale (network disconnected)"'
        served = {"status": 206, "headers": headers, "body": data}
        info = {"first": first, "last": last, "length": entry["length"],
                "covered": covered, "gaps": [], "served_bytes": last - first + 1,
                "segments_after": self._segments_view(entry)}
        return served, info

    def _range_416(self, length):
        return {"status": 416,
                "headers": {"content-range": f"bytes */{length}",
                            "accept-ranges": "bytes",
                            "content-type": "text/plain; charset=utf-8"},
                "body": b"Requested Range Not Satisfiable"}

    # ---- If-Range --------------------------------------------------------

    def _if_range_matches(self, value, headers):
        """客户端 If-Range 是否与缓存验证器一致。True/False/None（无法判断）。"""
        v = str(value).strip()
        if '"' in v:
            et = headers.get("etag")
            # RFC 7233：弱 ETag 不能用于 If-Range
            if not _is_strong_etag(v) or not _is_strong_etag(et):
                return False
            return _etag_strong_equal(v, et)
        # 当作 HTTP 日期
        t1, t2 = parse_time_value(v), parse_time_value(headers.get("last-modified"))
        if t1 is None or t2 is None:
            return None
        return t1 == t2

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

    # ---- 陈旧容错（stale-while-revalidate / stale-if-error） --------------

    @staticmethod
    def _cc_seconds(cc, key):
        """从解析后的 Cache-Control 取秒数指令；未声明返回 None（区别于显式 0）。"""
        v = cc.get(key)
        return float(v) if isinstance(v, int) else None

    def _swr_allows(self, entry, age, decision, req_cc) -> bool:
        """是否可在 stale-while-revalidate 窗口内直接返回陈旧副本 + 后台重验证。"""
        if decision != "stale-ok":
            # fresh 不需要；stale-must（must-revalidate / 请求 no-cache）禁止
            return False
        swr = entry.get("swr")
        if swr is None:
            return False
        # 请求显式新鲜度约束（max-age/min-fresh）优先，不用 SWR 糊弄客户端
        if isinstance(req_cc.get("max-age"), int) or isinstance(req_cc.get("min-fresh"), int):
            return False
        excess = age - (entry["ttl"] or 0.0)
        return excess <= swr

    def _sie_allows(self, entry, age, req_cc) -> bool:
        """源站出错（5xx）时是否允许按 stale-if-error 窗口回退陈旧副本。"""
        if self._must_not_serve_stale(entry, req_cc):
            return False
        sie = entry.get("sie")
        if sie is None:
            return False
        return age - (entry["ttl"] or 0.0) <= sie

    def _conn_fail_fallback(self, entry, age, req_cc) -> bool:
        """连接失败（断网 / 源站 fail）时能否回退陈旧副本。

        未声明 stale-if-error 时沿用既有行为（允许，除非 must-revalidate 等）；
        声明了则受秒数窗口约束。
        """
        if self._must_not_serve_stale(entry, req_cc):
            return False
        sie = entry.get("sie")
        if sie is None:
            return True
        return age - (entry["ttl"] or 0.0) <= sie

    def _serve_stale_while_revalidate(self, counters, entry, age, at, url, method,
                                      req_headers, trace, cache_key):
        """SWR 窗口内直接返回陈旧副本；调度或挂接后台重验证作业。"""
        swr = entry["swr"]
        excess = age - (entry["ttl"] or 0.0)
        job = self._find_pending_job(cache_key, entry["variant_key"])
        coalesced = job is not None
        if coalesced:
            job["attached"] += 1
            counters["coalesced_requests"] += 1
            counters["origin_fetches_saved"] += 1
            self._trace(trace, "SWR_COALESCED",
                        f"在途重验证作业 {job['id']} 已存在（将于 t={job['finish_at']:.0f} 结算），"
                        f"本请求挂接为第 {job['attached']} 个跟随者，合并节省一次回源")
        else:
            job = self._schedule_job(cache_key, entry, url, method, req_headers,
                                     at, trace)
        counters["stale_while_revalidate"] += 1
        served = self._entry_response(
            entry, age, at, stale=True,
            warning='110 cache-sim "Response is stale (stale-while-revalidate)"')
        self._trace(trace, "STALE_WHILE_REVALIDATE",
                    f"陈旧 {excess:.0f}s 在 stale-while-revalidate={swr:.0f}s 窗口内，"
                    "直接返回陈旧副本（Warning: 110）；后台重验证"
                    + ("复用在途作业" if coalesced else "已调度"))
        stale_info = {"kind": "stale-while-revalidate", "window": swr,
                      "excess": excess, "job_id": job["id"],
                      "coalesced": coalesced, "job_finish_at": job["finish_at"]}
        return self._done(counters, served, STALE_WHILE_REVALIDATE, at, url,
                          method, req_headers, trace, entry, origin_used=False,
                          stale_info=stale_info)

    # ---- 后台重验证作业（请求合并 + 虚拟时钟推进 + 持久化） -----------------

    def _find_pending_job(self, cache_key, variant_key):
        for j in self.state.get("jobs", []):
            if j["cache_key"] == cache_key and j["variant_key"] == variant_key:
                return j
        return None

    def _schedule_job(self, cache_key, entry, url, method, req_headers, at, trace):
        origin = self.state["origins"].get(url)
        delay = self._origin_delay(origin) if origin else 0.0
        self.state["job_seq"] = self.state.get("job_seq", 0) + 1
        job = {
            "id": f"job-{self.state['job_seq']}",
            "kind": "revalidate",
            "cache_key": cache_key,
            "variant_key": entry["variant_key"],
            "url": url,
            "method": method,
            "req_headers": dict(req_headers),
            "started_at": at,
            "finish_at": at + delay,   # 源站延迟决定作业何时结算
            "attached": 0,
        }
        self.state["jobs"].append(job)
        self.state["counters"]["background_revalidations"] += 1
        self._trace(trace, "SWR_SCHEDULE",
                    f"调度后台重验证作业 {job['id']}（键 {cache_key}，变体 "
                    f"{entry['variant_key']}）：源站延迟 {delay:.0f}s，"
                    f"将于 t={job['finish_at']:.0f} 由虚拟时钟结算")
        return job

    def _settle_due_jobs(self, up_to):
        """虚拟时钟推进到 up_to：结算所有 finish_at <= up_to 的作业。"""
        jobs = self.state.get("jobs", [])
        if not jobs:
            return
        due = [j for j in jobs if j["finish_at"] <= up_to]
        if not due:
            return
        self.state["jobs"] = [j for j in jobs if j["finish_at"] > up_to]
        for job in sorted(due, key=lambda j: (j["finish_at"], j["id"])):
            self._settle_job(job)

    def _settle_job(self, job):
        """结算一个后台重验证作业：按当前条目验证器发条件请求并应用结果。

        时间基准统一：作业在 started_at 发出重验证请求，finish_at
        （= started_at + 调度时的源站延迟）同时是响应到达、结果生效与
        作业结算的时刻，后续请求按虚拟时钟顺序看到一致状态。
        验证器变化 / 条目被清除时绝不复用不合规旧内容：
        304 合并进当前条目，200 经 _try_store 全量替换，5xx/故障保留陈旧条目。
        返回结算结果文档（含 failure 失败类别，供等待该作业的请求分流）。
        """
        counters = self.state["counters"]
        counters["jobs_settled"] += 1
        at = job["finish_at"]
        trace = []
        self._trace(trace, "JOB_SETTLE",
                    f"后台重验证作业 {job['id']} 于 t={at:.0f} 结算"
                    f"（调度于 t={job['started_at']:.0f}，挂接请求 {job['attached']} 个）")
        entry = None
        bucket = self.state["cache"].get(job["cache_key"])
        if bucket:
            entry = next((v for v in bucket["variants"]
                          if v["variant_key"] == job["variant_key"]), None)

        failure = None
        origin_status = None
        if entry is None:
            outcome, detail = "discarded", "缓存条目已不存在（被清除或替换），作业结果丢弃"
            self._trace(trace, "JOB_DISCARDED", detail)
        elif not self.state["network_up"]:
            outcome, detail = "failed", "结算时网络断开，保留陈旧条目"
            failure = "network-down"
            counters["jobs_failed"] += 1
            counters["origin_errors"] += 1
            self._trace(trace, "JOB_FAILED", detail)
        else:
            origin = self.state["origins"].get(job["url"])
            if origin is None:
                outcome, detail = "failed", "源站未定义资源，保留陈旧条目"
                failure = "origin-missing"
                counters["jobs_failed"] += 1
                self._trace(trace, "JOB_FAILED", detail)
            elif origin.get("fail"):
                outcome, detail = "failed", "源站连接失败（fail=true），保留陈旧条目"
                failure = "origin-fail"
                counters["jobs_failed"] += 1
                counters["origin_errors"] += 1
                self._trace(trace, "JOB_FAILED", detail)
            else:
                eff_delay = job["finish_at"] - job["started_at"]
                if eff_delay > 0:
                    self._trace(trace, "ORIGIN_DELAY",
                                f"重验证请求于 t={job['started_at']:.0f} 发出，"
                                f"源站延迟 {eff_delay:.0f}s，响应于 t={at:.0f} 到达"
                                "（到达即结算，延迟只计一次）")
                fwd = {}
                if entry["headers"].get("etag"):
                    fwd["if-none-match"] = entry["headers"]["etag"]
                elif entry["headers"].get("last-modified"):
                    fwd["if-modified-since"] = entry["headers"]["last-modified"]
                counters["origin_fetches"] += 1
                counters["revalidations"] += 1
                # delay=0：延迟已在调度时计入 finish_at，此处不再重复计
                st, hd, bd = self._fetch_origin(origin, fwd, at, trace, delay=0)
                self._count_origin_response(counters, st, hd, bd)
                origin_status = st
                if st == 304:
                    counters["not_modified"] += 1
                    self._merge_304(entry, hd, at, trace)
                    outcome, detail = "revalidated", "源站 304：合并校验元数据，age 归零"
                elif st >= 500:
                    counters["jobs_failed"] += 1
                    counters["origin_errors"] += 1
                    outcome, detail = "failed", f"源站返回 {st}，保留陈旧条目（5xx 不失效缓存）"
                    failure = "5xx"
                    self._trace(trace, "JOB_FAILED", detail)
                else:
                    info = self._try_store(job["cache_key"],
                                           {"status": st, "headers": hd, "body": bd},
                                           job.get("req_headers", {}), at, trace)
                    if info["stored"]:
                        outcome, detail = "refreshed", f"源站 {st}：缓存条目已更新"
                    else:
                        self._invalidate_variant(job["cache_key"], entry)
                        outcome, detail = ("invalidated",
                                           f"新响应不可缓存（{info['reason']}），旧条目已失效")
        doc = {"type": "job", "event_id": None, "job_id": job["id"], "at": at,
               "url": job["url"], "cache_key": job["cache_key"],
               "outcome": outcome, "detail": detail, "failure": failure,
               "origin_status": origin_status,
               "attached": job["attached"], "trace": trace}
        self.state["results"].append(doc)
        log = self.state.setdefault("job_log", [])
        log.append({k: doc[k] for k in
                    ("job_id", "at", "url", "outcome", "detail", "attached")})
        del log[:-50]
        return doc

    def _wait_for_job(self, counters, job, at, url, method, req_headers, req_cc,
                      trace, cache_key):
        """挂接并等待在途重验证作业：立即结算（结果时间戳仍为 finish_at），
        复用其合规结果，绝不返回不合规旧内容；作业失败时沿用既有容错规则。
        返回 None 表示作业结果不可用（如条目已失效），调用方回退常规回源路径。
        """
        wait = max(0.0, job["finish_at"] - at)
        self._trace(trace, "JOB_WAIT",
                    f"请求不允许直接返回陈旧副本（no-cache/must-revalidate），"
                    f"挂接在途作业 {job['id']} 并等待 {wait:.0f}s"
                    f"（作业于 t={job['finish_at']:.0f} 结算），只重验证一次")
        job["attached"] += 1
        counters["coalesced_requests"] += 1
        counters["origin_fetches_saved"] += 1
        self.state["jobs"] = [j for j in self.state["jobs"] if j["id"] != job["id"]]
        doc = self._settle_job(job)
        wait_info = {"job_id": job["id"], "finish_at": job["finish_at"],
                     "wait_seconds": wait}
        outcome = doc["outcome"]
        cur = self._find_variant(cache_key, req_headers)
        if outcome in ("revalidated", "refreshed") and cur is not None:
            age = self._entry_age(cur, at)
            served = self._entry_response(cur, age, at)
            self._trace(trace, "JOB_RESULT_REUSED",
                        f"作业 {job['id']} 结算（{outcome}），复用结果返回缓存副本")
            return self._done(counters, served, HIT, at, url, method, req_headers,
                              trace, cur, origin_used=False, job_wait=wait_info)
        if outcome == "failed" and cur is not None:
            age = self._entry_age(cur, at)
            failure = doc.get("failure")
            if failure in ("network-down", "origin-fail"):
                # 连接失败语义：沿用断网容错规则（声明 SIE 则受其窗口约束）
                if self._conn_fail_fallback(cur, age, req_cc):
                    served = self._entry_response(cur, age, at, stale=True)
                    self._trace(trace, "JOB_FAILED_STALE",
                                f"作业 {job['id']} 失败（{doc['detail']}），"
                                "按连接失败容错规则回退陈旧副本")
                    return self._done(counters, served, STALE, at, url, method,
                                      req_headers, trace, cur, origin_used=False,
                                      job_wait=wait_info)
            elif failure == "5xx" and self._sie_allows(cur, age, req_cc):
                counters["stale_if_error"] += 1
                excess = age - (cur["ttl"] or 0.0)
                served = self._entry_response(
                    cur, age, at, stale=True,
                    warning='110 cache-sim "Response is stale (stale-if-error: origin 5xx)"')
                self._trace(trace, "STALE_IF_ERROR",
                            f"作业 {job['id']} 遇源站 5xx，陈旧 {excess:.0f}s 在 "
                            f"stale-if-error={cur['sie']:.0f}s 窗口内，回退陈旧副本")
                return self._done(counters, served, STALE_IF_ERROR, at, url,
                                  method, req_headers, trace, cur,
                                  origin_used=False,
                                  stale_info={"kind": "stale-if-error",
                                              "window": cur["sie"],
                                              "excess": excess,
                                              "origin_status": doc.get("origin_status")},
                                  job_wait=wait_info)
            status = 502 if failure == "origin-missing" else 504
            self._trace(trace, "JOB_FAILED_NO_FALLBACK",
                        f"作业 {job['id']} 失败（{doc['detail']}），且 "
                        "must-revalidate/no-cache 禁止陈旧兜底，"
                        f"返回 {status}")
            served = self._gateway_error(
                status, f"{'Bad Gateway' if status == 502 else 'Gateway Timeout'}"
                        " (revalidation job failed, stale forbidden)")
            return self._done(counters, served, ERROR, at, url, method,
                              req_headers, trace, cur, origin_used=False,
                              job_wait=wait_info)
        # invalidated / discarded / 变体缺失：结果不可用，回退常规回源路径
        self._trace(trace, "JOB_RESULT_UNUSABLE",
                    f"作业 {job['id']} 结果不可用（{doc['outcome']}: {doc['detail']}），"
                    "回退常规回源路径")
        return None

    # ---- 源站模拟 --------------------------------------------------------

    @staticmethod
    def _origin_delay(origin) -> float:
        """源站响应延迟（虚拟秒）；非法值按 0 处理。"""
        if not origin:
            return 0.0
        try:
            return max(0.0, float(origin.get("delay", 0) or 0))
        except (TypeError, ValueError):
            return 0.0

    def _fetch_origin(self, origin, fwd_headers, at, trace, delay=None):
        # delay=None：按源站当前定义计延迟；后台作业结算时传 0
        # （延迟已在调度时计入 finish_at，到达时刻即结算时刻）
        if delay is None:
            delay = self._origin_delay(origin)
        if delay > 0:
            self._trace(trace, "ORIGIN_DELAY",
                        f"源站响应延迟 {delay:.0f}s（虚拟时间），响应于 t={at + delay:.0f} 到达")
        headers = copy.deepcopy(origin["headers"])
        status = origin["status"]
        body = origin["body"]
        length = len(body)
        # 回源响应带上源站的 Accept-Ranges 能力
        headers["accept-ranges"] = origin.get("accept_ranges",
                                              headers.get("accept-ranges", "bytes"))
        if "content-length" not in headers:
            headers["content-length"] = str(length)

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
                    ("etag", "last-modified", "cache-control", "expires", "vary", "date",
                     "accept-ranges")
                    if h in headers}
            return 304, keep, b""

        # Range 处理（条件请求 304 优先于 Range）
        rng = fwd_headers.get("range")
        if rng is not None:
            spec = parse_range_spec(rng)
            if spec[0] == "range":
                if origin.get("accept_ranges", "bytes") != "bytes":
                    self._trace(trace, "RANGE_UNSUPPORTED",
                                f"源站 Accept-Ranges: none，忽略 Range {rng}，返回完整 200")
                    return status, headers, body

                # If-Range 判定
                ir = fwd_headers.get("if-range")
                if ir is not None:
                    match = self._origin_if_range_match(ir, etag, lm)
                    self._trace(trace, "ORIGIN_IF_RANGE",
                        f"If-Range {ir} vs 当前（ETag={etag}, Last-Modified={lm}）-> "
                        f"{'一致，按 Range 返回 206' if match else '不一致，忽略 Range 返回完整 200'}")
                    if not match:
                        return status, headers, body

                interval = resolve_range(spec, length)
                if interval is None:
                    h = {"content-range": f"bytes */{length}",
                         "accept-ranges": "bytes",
                         "content-type": "text/plain; charset=utf-8"}
                    self._trace(trace, "ORIGIN_416",
                                f"Range {rng} 超出表示长度 {length}，返回 416")
                    return 416, h, b"Requested Range Not Satisfiable"
                first, last = interval
                h = dict(headers)
                h["content-range"] = f"bytes {first}-{last}/{length}"
                h["content-length"] = str(last - first + 1)
                self._trace(trace, "ORIGIN_206",
                            f"源站按 Range 返回 206：[{first},{last}]/{length}，"
                            f"{last - first + 1} 字节")
                return 206, h, body[first:last + 1]
            if spec[0] == "multi":
                self._trace(trace, "RANGE_MULTI_UNSUPPORTED",
                            "源站仿真不支持多区间，返回完整 200")
            else:
                self._trace(trace, "RANGE_PARSE_IGNORE",
                            f"非法 Range {rng}，源站忽略并返回完整 200")
        return status, headers, body

    @staticmethod
    def _origin_if_range_match(value, etag, lm):
        v = str(value).strip()
        if '"' in v:
            # RFC 7233：If-Range 中的弱 ETag 无效 -> 视为不匹配
            return _is_strong_etag(v) and _is_strong_etag(etag) and \
                v.strip() == etag.strip()
        t1, t2 = parse_time_value(v), parse_time_value(lm)
        return t1 is not None and t2 is not None and t1 == t2

    def _count_origin_response(self, counters, status, headers, body, range_hdr=None):
        counters["bytes_from_origin"] += wire_size(status, headers, body)
        if status == 206:
            counters["range_bytes_from_origin"] += body_len(body)
            if range_hdr is None:
                counters["range_fetches"] += 1

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
        entry["swr"] = self._cc_seconds(cc, "stale-while-revalidate")
        entry["sie"] = self._cc_seconds(cc, "stale-if-error")
        self._trace(trace, "MERGE_304", "304 响应头已合并进缓存条目，age 归零（片段保留）")

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

    def _check_store_rules(self, status, headers, req_headers, at, trace):
        """完整/部分响应共用的可缓存性检查，返回 (ok, cc, work_headers, reason)。"""
        cc = parse_cc(headers.get("cache-control", ""))
        work_headers = headers
        if "cache-control" not in headers and self.config["default_cc"]:
            cc = {str(k).lower(): (int(v) if isinstance(v, (int, float)) else v)
                  for k, v in self.config["default_cc"].items()}
            work_headers = dict(headers)
            work_headers["cache-control"] = ", ".join(
                str(k) if v is True else f"{k}={v}" for k, v in cc.items())
            trace.append({"step": len(trace) + 1, "code": "DEFAULT_CC",
                          "detail": f"响应无 Cache-Control，注入仿真缺省指令 {dict(cc)}"})
        if status not in self.config["cacheable_statuses"]:
            return False, cc, work_headers, \
                f"状态码 {status} 不在可缓存列表 {self.config['cacheable_statuses']}"
        if "no-store" in cc:
            return False, cc, work_headers, "响应 Cache-Control: no-store，禁止缓存"
        if self.config["cache_mode"] == "shared" and "private" in cc:
            return False, cc, work_headers, "共享缓存模式下响应 private，不允许缓存"
        if work_headers.get("vary", "").strip() == "*":
            return False, cc, work_headers, "Vary: * 表示不受限变体，不缓存"
        return True, cc, work_headers, None

    def _try_store(self, cache_key, resp, req_headers, at, trace):
        status, headers0 = resp["status"], resp["headers"]
        ok, cc, headers, why = self._check_store_rules(
            status, headers0, req_headers, at, trace)
        if not ok:
            return {"stored": False, "reason": why}
        resp["headers"] = headers

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

        vary_raw = headers.get("vary")
        vary_headers = ([h.strip().lower() for h in vary_raw.split(",") if h.strip()]
                        if vary_raw else [])
        selected = {h: req_headers.get(h, "") for h in vary_headers}
        age_raw = str(headers.get("age", ""))
        body = resp["body"]
        entry = {
            "variant_key": _variant_key(selected),
            "status": status,
            "headers": copy.deepcopy(headers),
            "body": bytes(body),
            "stored_at": at,
            "init_age": int(age_raw) if age_raw.isdigit() else 0,
            "ttl": ttl,
            "no_cache": "no-cache" in cc,
            "must_revalidate": self._must_revalidate(cc),
            "rev_by": "proxy" if "proxy-revalidate" in cc else "must",
            "swr": self._cc_seconds(cc, "stale-while-revalidate"),
            "sie": self._cc_seconds(cc, "stale-if-error"),
            "partial": False,
            "length": body_len(body),
            "segments": [],
        }
        bucket = self.state["cache"].setdefault(
            cache_key, {"vary": vary_headers, "variants": []})
        bucket["vary"] = vary_headers
        bucket["variants"] = [v for v in bucket["variants"]
                              if v["variant_key"] != entry["variant_key"]]
        bucket["variants"].append(entry)
        trace.append({"step": len(trace) + 1, "code": "STORE",
                      "detail": f"写入缓存：ttl={entry['ttl']}s vary={vary_headers or '无'} "
                                f"no_cache={entry['no_cache']} must_revalidate={entry['must_revalidate']} "
                                f"length={entry['length']}"})
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

    def _entry_response(self, entry, age, at, stale=False, warning=None):
        headers = copy.deepcopy(entry["headers"])
        headers["age"] = str(int(max(0, age)))
        if stale:
            headers["warning"] = warning or \
                '110 cache-sim "Response is stale (network disconnected)"'
        return {"status": entry["status"], "headers": headers, "body": entry["body"]}

    @staticmethod
    def _gateway_error(status, message):
        return {"status": status,
                "headers": {"content-type": "text/plain; charset=utf-8"},
                "body": message.encode("utf-8") if isinstance(message, str) else message}

    def _done(self, counters, served, verdict, at, url, method, req_headers,
              trace, entry, origin_used, origin_status=None, origin_resp=None,
              range_info=None, origin_bytes=None, stale_info=None,
              origin_delay=None, job_wait=None):
        served_body = served.get("body", b"")
        counters["bytes_served"] += wire_size(
            served["status"], served["headers"], served_body)
        if verdict in (HIT, RANGE_HIT, REVALIDATED, STALE,
                       STALE_WHILE_REVALIDATE, STALE_IF_ERROR):
            counters["hits"] += 1
        if verdict in (STALE, STALE_WHILE_REVALIDATE, STALE_IF_ERROR):
            counters["stale_served"] += 1
        if verdict == ERROR:
            counters["errors"] += 1
        if verdict == RANGE_HIT:
            counters["range_hits"] += 1
        text = served_body.decode("utf-8", "replace") if isinstance(served_body, bytes) \
            else str(served_body)
        out = {
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
            # 文本预览（UTF-8 替换）；二进制内容请用 response_body_b64
            "response_body": text,
            "response_body_b64": to_b64(served_body)
            if isinstance(served_body, bytes) else to_b64(str(served_body).encode("utf-8")),
            "bytes_served": wire_size(
                served["status"], served["headers"], served_body),
            "origin_bytes": (origin_bytes if origin_bytes is not None
                             else (wire_size(origin_resp["status"], origin_resp["headers"],
                                             origin_resp.get("body"))
                                   if origin_resp is not None else 0)),
            "cache_entry_after": self._entry_summary(entry),
            "network_up": self.state["network_up"],
            "trace": trace,
            "counters": copy.deepcopy(counters),
        }
        if range_info is not None:
            out["range"] = range_info
        if stale_info is not None:
            out["stale_info"] = stale_info
        if origin_delay:
            out["origin_delay"] = origin_delay
        if job_wait is not None:
            out["job_wait"] = job_wait
        return out

    @staticmethod
    def _entry_summary(entry):
        if entry is None:
            return None
        partial = entry.get("partial", False)
        out = {
            "status": entry["status"],
            "ttl": entry["ttl"],
            "no_cache": entry["no_cache"],
            "must_revalidate": entry["must_revalidate"],
            "swr": entry.get("swr"),
            "sie": entry.get("sie"),
            "stored_at": entry["stored_at"],
            "etag": entry["headers"].get("etag"),
            "last_modified": entry["headers"].get("last-modified"),
            "vary": entry["headers"].get("vary"),
            "partial": partial,
            "length": entry.get("length"),
            "segments": Engine._segments_view(entry),
        }
        return out

