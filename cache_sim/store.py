"""SQLite 持久化层：场景、事件序列、仿真执行状态。"""

from __future__ import annotations

import copy
import json
import sqlite3
import threading
import uuid

from .engine import DEFAULT_CONFIG, Engine, new_state, norm_body

VALID_EVENT_TYPES = {"origin", "origin_change", "network", "clear", "request"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS scenarios (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    config      TEXT NOT NULL,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id  TEXT NOT NULL REFERENCES scenarios(id) ON DELETE CASCADE,
    event_id     TEXT,
    type         TEXT NOT NULL,
    at           REAL,
    payload      TEXT NOT NULL,
    seq          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_scenario ON events(scenario_id, seq);
CREATE TABLE IF NOT EXISTS states (
    scenario_id  TEXT PRIMARY KEY REFERENCES scenarios(id) ON DELETE CASCADE,
    state        TEXT NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id  TEXT NOT NULL REFERENCES scenarios(id) ON DELETE CASCADE,
    until        REAL,
    events_run   INTEGER NOT NULL,
    wall_time    REAL NOT NULL
);
"""


class Store:
    def __init__(self, path: str = "cache_sim.db"):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self):
        with self._lock:
            self._conn.close()

    # ---- 场景 ------------------------------------------------------------

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex[:12]

    def create_scenario(self, name: str = "unnamed", config: dict | None = None,
                        sid: str | None = None) -> dict:
        import time
        cfg = copy.deepcopy(DEFAULT_CONFIG)
        if config:
            cfg.update(copy.deepcopy(config))
        sid = sid or self.new_id()
        now = time.time()
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO scenarios(id, name, config, created_at, updated_at) "
                    "VALUES (?,?,?,?,?)",
                    (sid, name, json.dumps(cfg, ensure_ascii=False), now, now))
                self._conn.commit()
            except sqlite3.IntegrityError as e:
                raise ValueError(f"场景 ID 已存在: {sid}") from e
        return {"id": sid, "name": name, "config": cfg,
                "created_at": now, "updated_at": now}

    def list_scenarios(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT s.*, (SELECT COUNT(*) FROM events e WHERE e.scenario_id=s.id) AS n_events "
                "FROM scenarios s ORDER BY s.created_at").fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["config"] = json.loads(d["config"])
            out.append(d)
        return out

    def get_scenario(self, sid: str) -> dict:
        with self._lock:
            r = self._conn.execute("SELECT * FROM scenarios WHERE id=?", (sid,)).fetchone()
        if r is None:
            raise KeyError(f"场景不存在: {sid}")
        d = dict(r)
        d["config"] = json.loads(d["config"])
        return d

    def update_config(self, sid: str, config_patch: dict, name: str | None = None) -> dict:
        import time
        sc = self.get_scenario(sid)
        cfg = sc["config"]
        cfg.update(copy.deepcopy(config_patch or {}))
        with self._lock:
            self._conn.execute(
                "UPDATE scenarios SET config=?, name=COALESCE(?, name), updated_at=? WHERE id=?",
                (json.dumps(cfg, ensure_ascii=False), name, time.time(), sid))
            self._conn.commit()
        return self.get_scenario(sid)

    def delete_scenario(self, sid: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM scenarios WHERE id=?", (sid,))
            self._conn.commit()
            return cur.rowcount > 0

    def clone_scenario(self, src_id: str, name: str | None = None,
                       config_patch: dict | None = None,
                       clone_state: bool = False) -> dict:
        """复制场景（含事件序列；默认不复制执行状态，便于从头对比规则）。"""
        src = self.get_scenario(src_id)
        cfg = copy.deepcopy(src["config"])
        if config_patch:
            cfg.update(copy.deepcopy(config_patch))
        new_id = self.new_id()
        import time
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO scenarios(id, name, config, created_at, updated_at) VALUES (?,?,?,?,?)",
                (new_id, name or f"{src['name']}-copy",
                 json.dumps(cfg, ensure_ascii=False), now, now))
            rows = self._conn.execute(
                "SELECT event_id, type, at, payload, seq FROM events "
                "WHERE scenario_id=? ORDER BY seq", (src_id,)).fetchall()
            for r in rows:
                self._conn.execute(
                    "INSERT INTO events(scenario_id, event_id, type, at, payload, seq) "
                    "VALUES (?,?,?,?,?,?)",
                    (new_id, r["event_id"], r["type"], r["at"], r["payload"], r["seq"]))
            if clone_state:
                sr = self._conn.execute(
                    "SELECT state FROM states WHERE scenario_id=?", (src_id,)).fetchone()
                if sr:
                    self._conn.execute(
                        "INSERT OR REPLACE INTO states(scenario_id, state, updated_at) VALUES (?,?,?)",
                        (new_id, sr["state"], now))
            self._conn.commit()
        return self.get_scenario(new_id)

    # ---- 事件 ------------------------------------------------------------

    def add_event(self, sid: str, event: dict, event_id: str | None = None) -> dict:
        self.get_scenario(sid)  # 存在性
        etype = event.get("type")
        if etype not in VALID_EVENT_TYPES:
            raise ValueError(f"非法事件类型 {etype!r}，可选 {sorted(VALID_EVENT_TYPES)}")
        if "at" not in event:
            raise ValueError("事件必须带 at（虚拟时间戳）")
        at = float(event["at"])
        payload = copy.deepcopy(event)
        # 事件载荷需 JSON 落库：把 body（可能是 bytes）归一化为字符串
        if etype in ("origin", "origin_change") and "body" in payload:
            payload["body"] = norm_body(payload["body"])
        eid = event_id or event.get("id") or uuid.uuid4().hex[:8]
        payload["id"] = eid
        with self._lock:
            seq = self._next_seq(sid)
            self._conn.execute(
                "INSERT INTO events(scenario_id, event_id, type, at, payload, seq) "
                "VALUES (?,?,?,?,?,?)",
                (sid, eid, etype, at, json.dumps(payload, ensure_ascii=False), seq))
            self._conn.commit()
        return {"id": eid, "seq": seq, "event": payload}

    def add_events(self, sid: str, events: list[dict]) -> list[dict]:
        out = []
        for ev in events:
            out.append(self.add_event(sid, ev))
        return out

    def _next_seq(self, sid) -> int:
        r = self._conn.execute(
            "SELECT COALESCE(MAX(seq), -1) + 1 AS s FROM events WHERE scenario_id=?",
            (sid,)).fetchone()
        return int(r["s"])

    def list_events(self, sid: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, type, at, payload, seq FROM events "
                "WHERE scenario_id=? ORDER BY seq", (sid,)).fetchall()
        return [{"seq": r["seq"], "id": r["event_id"], "type": r["type"],
                 "at": r["at"], **json.loads(r["payload"])} for r in rows]

    def delete_event(self, sid: str, event_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM events WHERE scenario_id=? AND event_id=?", (sid, event_id))
            self._conn.commit()
            return cur.rowcount > 0

    # ---- 执行状态 --------------------------------------------------------

    def get_state(self, sid: str) -> dict | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT state FROM states WHERE scenario_id=?", (sid,)).fetchone()
        return json.loads(r["state"]) if r else None

    def save_state(self, sid: str, state: dict):
        import time
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO states(scenario_id, state, updated_at) VALUES (?,?,?)",
                (sid, json.dumps(state, ensure_ascii=False), time.time()))
            self._conn.commit()

    def reset(self, sid: str) -> dict:
        """清空执行状态（保留场景配置与事件序列），时钟归零。"""
        self.get_scenario(sid)
        with self._lock:
            self._conn.execute("DELETE FROM states WHERE scenario_id=?", (sid,))
            self._conn.execute("DELETE FROM runs WHERE scenario_id=?", (sid,))
            self._conn.commit()
        state = new_state()
        self.save_state(sid, state)
        return state

    # ---- 运行 ------------------------------------------------------------

    def run(self, sid: str, until: float | None = None) -> dict:
        """从当前状态继续执行：按 (at, seq) 顺序运行未执行且 at <= until 的事件。"""
        import time
        sc = self.get_scenario(sid)
        engine = Engine(sc["config"])
        state = self.get_state(sid)
        if state is None:
            state = new_state()
        engine.load_state(state)

        done = set(state.get("executed_event_ids", []))
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, at, payload, seq FROM events WHERE scenario_id=? "
                "ORDER BY at, seq", (sid,)).fetchall()
        pending = []
        for r in rows:
            if r["event_id"] in done:
                continue
            if until is not None and float(r["at"]) > float(until):
                continue
            pending.append((r["seq"], json.loads(r["payload"])))

        results = []
        for _, payload in pending:
            results.append(engine.apply_event(payload))

        self.save_state(sid, engine.state)
        with self._lock:
            self._conn.execute(
                "INSERT INTO runs(scenario_id, until, events_run, wall_time) VALUES (?,?,?,?)",
                (sid, until, len(pending), time.time()))
            self._conn.commit()
        return {
            "scenario_id": sid,
            "until": until,
            "events_run": len(pending),
            "virtual_time": engine.state["time"],
            "network_up": engine.state["network_up"],
            "results": results,
            "counters": engine.state["counters"],
        }

    def list_runs(self, sid: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, until, events_run, wall_time FROM runs "
                "WHERE scenario_id=? ORDER BY id", (sid,)).fetchall()
        return [dict(r) for r in rows]
