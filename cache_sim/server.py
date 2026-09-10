"""本地 HTTP 缓存仿真 API（标准库 http.server）。

启动：  python -m cache_sim [--host 127.0.0.1] [--port 8000] [--db cache_sim.db]
接口文档见 README.md。
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .store import Store

SID = r"[0-9a-zA-Z_-]{6,64}"

# 路由：(method regex, handler_name)
ROUTES = [
    ("GET",     r"^/health$",                                  "h_health"),
    # compare 必须排在 /scenarios/<sid> 之前，否则会被当作 sid
    ("POST",    r"^/scenarios/compare$",                       "h_compare"),
    ("GET",     r"^/scenarios/compare$",                       "h_compare"),
    ("POST",    r"^/scenarios$",                               "h_create_scenario"),
    ("GET",     r"^/scenarios$",                               "h_list_scenarios"),
    ("GET",     r"^/scenarios/(?P<sid>" + SID + r")$",         "h_get_scenario"),
    ("PATCH",   r"^/scenarios/(?P<sid>" + SID + r")$",         "h_update_scenario"),
    ("DELETE",  r"^/scenarios/(?P<sid>" + SID + r")$",         "h_delete_scenario"),
    ("POST",    r"^/scenarios/(?P<sid>" + SID + r")/clone$",   "h_clone_scenario"),
    ("POST",    r"^/scenarios/(?P<sid>" + SID + r")/events$",  "h_add_events"),
    ("GET",     r"^/scenarios/(?P<sid>" + SID + r")/events$",  "h_list_events"),
    ("DELETE",  r"^/scenarios/(?P<sid>" + SID + r")/events/(?P<eid>[0-9a-zA-Z_-]+)$",
                                                                 "h_delete_event"),
    ("POST",    r"^/scenarios/(?P<sid>" + SID + r")/run$",     "h_run"),
    ("POST",    r"^/scenarios/(?P<sid>" + SID + r")/reset$",   "h_reset"),
    ("GET",     r"^/scenarios/(?P<sid>" + SID + r")/snapshot$", "h_snapshot"),
    ("GET",     r"^/scenarios/(?P<sid>" + SID + r")/results$",  "h_results"),
]
COMPILED = [(m, re.compile(p), h) for m, p, h in ROUTES]


class Handler(BaseHTTPRequestHandler):
    server_version = "CacheSim/1.0"

    # ---- 基础收发 --------------------------------------------------------

    def _send(self, code: int, payload):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError(f"请求体不是合法 JSON: {e}")
        if not isinstance(data, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return data

    def _query(self) -> dict:
        return {k: v[-1] for k, v in parse_qs(urlparse(self.path).query).items()}

    def log_message(self, fmt, *args):
        # 简洁日志
        self.server.quiet or print(f"[{self.log_date_time_string()}] {fmt % args}")

    # ---- 路由分发 --------------------------------------------------------

    def _dispatch(self, method: str):
        path = urlparse(self.path).path.rstrip("/") or "/"
        if path == "/":
            return self._send(200, {
                "service": "http-cache-sim",
                "docs": "README.md",
                "endpoints": [f"{m} {p.pattern.replace(chr(92), '')}"
                              for m, p, _ in COMPILED],
            })
        for m, rx, handler_name in COMPILED:
            if m != method:
                continue
            match = rx.match(path)
            if match:
                try:
                    return getattr(self, handler_name)(**match.groupdict())
                except KeyError as e:
                    return self._send(404, {"error": "not_found", "message": str(e)})
                except ValueError as e:
                    return self._send(400, {"error": "bad_request", "message": str(e)})
        self._send(404, {"error": "not_found",
                         "message": f"无匹配路由: {method} {path}"})

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PATCH(self):
        self._dispatch("PATCH")

    def do_DELETE(self):
        self._dispatch("DELETE")

    # ---- 处理器 ----------------------------------------------------------

    @property
    def store(self) -> Store:
        return self.server.store

    def h_health(self):
        self._send(200, {"status": "ok", "offline": True})

    def h_create_scenario(self):
        data = self._read_json()
        sid = data.get("id")
        sc = self.store.create_scenario(
            name=data.get("name", "unnamed"),
            config=data.get("config"), sid=sid)
        events = data.get("events") or []
        added = []
        for ev in events:
            added.append(self.store.add_event(sc["id"], ev))
        self._send(201, {"scenario": sc, "events_added": len(added)})

    def h_list_scenarios(self):
        self._send(200, {"scenarios": self.store.list_scenarios()})

    def h_get_scenario(self, sid):
        self._send(200, {"scenario": self.store.get_scenario(sid)})

    def h_update_scenario(self, sid):
        data = self._read_json()
        sc = self.store.update_config(sid, data.get("config", {}), data.get("name"))
        self._send(200, {"scenario": sc})

    def h_delete_scenario(self, sid):
        ok = self.store.delete_scenario(sid)
        if not ok:
            self._send(404, {"error": "not_found", "message": f"场景不存在: {sid}"})
        else:
            self._send(200, {"deleted": sid})

    def h_clone_scenario(self, sid):
        data = self._read_json()
        clone = self.store.clone_scenario(
            sid, name=data.get("name"), config_patch=data.get("config"),
            clone_state=bool(data.get("clone_state", False)))
        self._send(201, {"clone": clone, "source": sid})

    def h_add_events(self, sid):
        self.store.get_scenario(sid)
        data = self._read_json()
        if "events" in data and "event" in data:
            raise ValueError("event（单个）与 events（批量）只能二选一")
        if "event" in data:
            rec = self.store.add_event(sid, data["event"])
            self._send(201, {"added": [rec]})
        else:
            events = data.get("events")
            if not isinstance(events, list) or not events:
                raise ValueError("需要 event（单个对象）或 events（非空数组）")
            recs = self.store.add_events(sid, events)
            self._send(201, {"added": recs})

    def h_list_events(self, sid):
        self.store.get_scenario(sid)
        self._send(200, {"scenario_id": sid, "events": self.store.list_events(sid)})

    def h_delete_event(self, sid, eid):
        ok = self.store.delete_event(sid, eid)
        if not ok:
            self._send(404, {"error": "not_found",
                             "message": f"事件 {eid} 不存在（删除/修改事件后需 reset 重新运行）"})
        else:
            self._send(200, {"deleted": eid})

    def h_run(self, sid):
        data = self._read_json()
        until = data.get("until", self._query().get("until"))
        if until is not None:
            try:
                until = float(until)
            except (TypeError, ValueError):
                raise ValueError("until 必须是数字时间戳")
        result = self.store.run(sid, until)
        self._send(200, result)

    def h_reset(self, sid):
        state = self.store.reset(sid)
        self._send(200, {"scenario_id": sid, "reset": True,
                         "state": self._snapshot_state(sid, state, include_results=False,
                                                       include_cache=True)})

    def h_results(self, sid):
        state = self.store.get_state(sid)
        if state is None:
            raise KeyError(f"场景 {sid} 尚未运行或已重置")
        self._send(200, {"scenario_id": sid, "results": state.get("results", [])})

    def h_snapshot(self, sid):
        q = self._query()
        state = self.store.get_state(sid)
        if state is None:
            state = self.store.reset(sid)
        include_results = q.get("results", "1") not in ("0", "false", "False")
        include_trace = q.get("trace", "1") not in ("0", "false", "False")
        self._send(200, self._snapshot_state(
            sid, state, include_results=include_results,
            include_trace=include_trace, include_cache=True))

    def h_compare(self):
        if self.command == "POST":
            data = self._read_json()
            ids = data.get("ids") or []
        else:
            ids = self._query().get("ids", "").split(",")
        ids = [i.strip() for i in ids if i.strip()]
        if len(ids) < 2:
            raise ValueError("compare 至少需要两个场景 id")
        items = []
        for sid in ids:
            sc = self.store.get_scenario(sid)
            state = self.store.get_state(sid)
            items.append({
                "scenario_id": sid,
                "name": sc["name"],
                "config": sc["config"],
                "virtual_time": state["time"] if state else 0,
                "executed": len(state.get("executed_event_ids", [])) if state else 0,
                "counters": state["counters"] if state else None,
                "verdicts": ([r.get("verdict") for r in state.get("results", [])
                              if r.get("type") == "request"]) if state else [],
            })
        base = items[0]
        rows = []
        for it in items[1:]:
            c0, c1 = base["counters"], it["counters"]
            rows.append({
                "a": base["scenario_id"], "b": it["scenario_id"],
                "delta_origin_fetches": c1["origin_fetches"] - c0["origin_fetches"],
                "delta_bytes_from_origin": c1["bytes_from_origin"] - c0["bytes_from_origin"],
                "delta_hits": c1["hits"] - c0["hits"],
                "delta_errors": c1["errors"] - c0["errors"],
                "delta_range_fills": c1.get("range_fills", 0) - c0.get("range_fills", 0),
                "delta_range_hits": c1.get("range_hits", 0) - c0.get("range_hits", 0),
                "delta_range_bytes_from_origin": (
                    c1.get("range_bytes_from_origin", 0)
                    - c0.get("range_bytes_from_origin", 0)),
                "same_verdict_sequence": base["verdicts"] == it["verdicts"],
            })
        self._send(200, {"items": items, "comparisons": rows})

    # ---- 快照 ------------------------------------------------------------

    def _snapshot_state(self, sid, state, include_results=True, include_trace=True,
                        include_cache=True):
        sc = self.store.get_scenario(sid)
        now = state["time"]
        cache_view = []
        if include_cache:
            for key, bucket in sorted(state["cache"].items()):
                variants = []
                for v in bucket["variants"]:
                    age = now - v["stored_at"] + v.get("init_age", 0)
                    ttl = v["ttl"]
                    fresh = (not v["no_cache"] and ttl is not None and age <= ttl)
                    partial = v.get("partial", False)
                    if partial:
                        segments = [[s["start"], s["end"]] for s in v.get("segments", [])]
                        cached_bytes = sum(e - s + 1 for s, e in segments)
                        length = v.get("length", 0)
                        body_bytes = cached_bytes
                    else:
                        body = v.get("body", b"")
                        segments = None
                        length = v.get("length",
                                      len(body) if isinstance(body, bytes) else len(str(body)))
                        cached_bytes = length
                        body_bytes = length
                    variants.append({
                        "variant_key": v["variant_key"],
                        "status": v["status"],
                        "age": age,
                        "ttl": ttl,
                        "fresh": fresh,
                        "no_cache": v["no_cache"],
                        "must_revalidate": v["must_revalidate"],
                        "etag": v["headers"].get("etag"),
                        "last_modified": v["headers"].get("last-modified"),
                        "accept_ranges": v["headers"].get("accept-ranges"),
                        "partial": partial,
                        "length": length,
                        "segments": segments,
                        "cached_bytes": cached_bytes,
                        "coverage_pct": round(cached_bytes / length, 4) if length else None,
                        "body_bytes": body_bytes,
                    })
                cache_view.append({"key": key, "vary": bucket.get("vary", []),
                                   "variants": variants})
        out = {
            "scenario_id": sid,
            "name": sc["name"],
            "config": sc["config"],
            "virtual_time": now,
            "network_up": state["network_up"],
            "origins": {url: self._origin_summary(d)
                        for url, d in sorted(state["origins"].items())},
            "cache": cache_view,
            "counters": state["counters"],
            "executed_event_ids": state["executed_event_ids"],
            "pending_events": self._pending_events(sid, state),
        }
        if include_results:
            results = state.get("results", [])
            if not include_trace:
                results = [{k: val for k, val in r.items() if k != "trace"} for r in results]
            out["results"] = results
        return out

    def _pending_events(self, sid, state):
        done = set(state.get("executed_event_ids", []))
        return [e for e in self.store.list_events(sid) if e.get("id") not in done]

    @staticmethod
    def _origin_summary(d):
        body = d.get("body", b"")
        n = len(body) if isinstance(body, (bytes, bytearray)) else len(str(body).encode("utf-8"))
        return {
            "status": d["status"],
            "etag": d["headers"].get("etag"),
            "last_modified": d["headers"].get("last-modified"),
            "cache_control": d["headers"].get("cache-control"),
            "accept_ranges": d.get("accept_ranges",
                                   d["headers"].get("accept-ranges")),
            "body_bytes": n,
        }


def build_server(host: str = "127.0.0.1", port: int = 8000,
                 db: str = "cache_sim.db", quiet: bool = False) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.store = Store(db)
    httpd.quiet = quiet
    return httpd


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="本地 HTTP 缓存仿真 API")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--db", default="cache_sim.db")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)
    httpd = build_server(args.host, args.port, args.db, args.quiet)
    print(f"缓存仿真 API 已启动: http://{args.host}:{args.port}  (db={args.db}, Ctrl+C 退出)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n关闭中…")
    finally:
        httpd.server_close()
        httpd.store.close()


if __name__ == "__main__":
    main()
