# 本地 HTTP 缓存仿真 API

供后端工程师在**离线环境**验证 HTTP 缓存策略的仿真服务。输入源站响应、缓存模式与带虚拟时间戳的事件序列，
观察缓存的命中、回源、304 重验证、不可缓存与断网失败路径，并逐条给出**判定依据（trace）**。

仅依赖 Python 3.10+ 标准库（`http.server` + `sqlite3`），无需任何第三方包、无需联网。

## 快速开始

```bash
# 启动（默认 127.0.0.1:8000，SQLite 文件 cache_sim.db）
python -m cache_sim --port 8000 --db cache_sim.db

# 跑离线示例（直接用引擎 + SQLite，不需要起服务）
python examples/demo.py

# 端到端 HTTP 示例（自动起停服务）
python examples/demo.py --http

# 全部测试（77 个：引擎语义 + 持久化 + HTTP 集成）
python -m unittest discover -s tests -v
```

## 它仿真了什么

| 类别 | 支持项 |
|---|---|
| 响应 `Cache-Control` | `max-age`、`s-maxage`、`no-cache`、`no-store`、`private`、`must-revalidate`、`proxy-revalidate`、`stale-while-revalidate`、`stale-if-error` |
| 其他响应头 | `Expires`、`ETag`（含 `W/` 弱校验）、`Last-Modified`、`Vary`（含 `Vary: *`）、`Age`、`Date` |
| 请求 `Cache-Control` | `no-cache`、`no-store`、`max-age`、`max-stale[=s]`、`min-fresh`、`only-if-cached` |
| 条件请求 | `If-None-Match` / `If-Modified-Since`，源站返回 304 时合并头、age 归零 |
| 缓存模式 | `shared`（共享代理：`private` 不缓存、`s-maxage` 优先）/ `private`（浏览器：忽略 `s-maxage`） |
| 启发式缓存 | 无显式新鲜度时按 `Last-Modified` 年龄比例估算 TTL（默认关闭） |
| 源站故障 | `delay`（响应延迟，虚拟秒）、`fail`（连接失败）、5xx 状态；5xx **不失效**已有缓存条目 |
| 陈旧容错 | `stale-while-revalidate`：窗口内直接返回陈旧副本 + 后台重验证作业；`stale-if-error`：回源 5xx 时按窗口回退陈旧副本（连接失败路径沿用断网规则，声明了 SIE 则受其窗口约束） |
| 请求合并 | 同一虚拟时刻、同一缓存键 + Vary 变体只执行一次重验证；其余请求挂接在途作业（返回陈旧副本）或复用结算结果；待处理作业随虚拟时钟推进、持久化到 SQLite，重启后继续 |
| 失败路径 | 断网 + 陈旧：普通响应返回 `STALE`（带 `Warning: 110`）；`must-revalidate`/`no-cache` 返回 504；`only-if-cached` 不回源 |

## 判定结果（verdict）

| verdict | 含义 |
|---|---|
| `MISS` | 无缓存，回源拿到完整响应，**已写入缓存** |
| `HIT` | 缓存新鲜，直接返回（不回源） |
| `REVALIDATED` | 缓存陈旧/`no-cache`，发条件请求后源站返回 **304**，继续使用副本 |
| `REFRESHED` | 重验证/过期回源拿到**完整 200 新响应**，缓存被替换（如 ETag 变化） |
| `NOT_MODIFIED` | 客户端**自带**条件请求，源站直接返回 304，透传给客户端 |
| `UNCACHEABLE` | 回源成功但响应不允许/不足以缓存（`no-store`、`private`、无显式新鲜度等，trace 给出具体原因） |
| `STALE` | 断网/源站连接失败时使用了陈旧副本（响应附加 `Warning: 110`） |
| `STALE_WHILE_REVALIDATE` | 陈旧度在 `stale-while-revalidate` 窗口内：直接返回陈旧副本，同时调度（或挂接）后台重验证作业 |
| `STALE_IF_ERROR` | 回源得到 5xx，陈旧度在 `stale-if-error` 窗口内：回退陈旧副本（源站错误不透传给客户端） |
| `ERROR` | 无法满足请求：断网 504、源站未定义 502，或无陈旧回退时的源站 5xx 透传 |

每个 request 事件的返回里都有 `trace` 数组，逐步记录判定依据，例如：
`CACHE_MISS → STORE`、`CACHE_ENTRY → FRESH → HIT`、
`STALE → REVALIDATE(If-None-Match) → ORIGIN_VALIDATE(匹配) → NOT_MODIFIED → MERGE_304`、
`STALE_MUST → NETWORK_DOWN_FAIL(504)`。

## 事件类型

所有事件都带 `at`（虚拟时间戳，只能单调前进）和可选 `id`。事件按 `(at, 追加顺序)` 执行。

| type | 必填/常用字段 | 说明 |
|---|---|---|
| `origin` | `url`、`status?`、`headers?`、`body?`、`etag?`、`last_modified?`、`delay?`、`fail?` | 定义/覆盖源站资源；`delay` 为响应延迟（虚拟秒），`fail: true` 模拟连接失败，`status` 可设 5xx |
| `origin_change` | `url`、`body?`、`etag?`、`last_modified?`、`headers?`、`remove_headers?`、`status?`、`delay?`、`fail?` | 模拟源站内容/故障变更（需已 origin 定义） |
| `network` | `up: true/false` | 网络连通/断开；断开期间缓存不回源，走失败路径 |
| `clear` | `url?`、`method?` | 手动清缓存：带 url 清单个资源（含所有 Vary 变体），不带清全部 |
| `request` | `url`、`method?`(默认 GET)、`headers?` | 一次穿过缓存的客户端请求 |

时间值既可以是数字（虚拟时间戳，如 `120`），也可以是 HTTP 日期字符串
（如 `"Wed, 21 Oct 2015 07:28:00 GMT"`）；数字时间戳之间直接做差。

## 场景配置（config）

```json
{
  "cache_mode": "shared",          // shared(默认) | private
  "heuristic_cache": false,        // 无显式新鲜度时启用启发式缓存
  "heuristic_ratio": 0.1,          // 启发式 TTL = (now - Last-Modified) * ratio
  "heuristic_min": 0,
  "heuristic_max": 86400,
  "cacheable_statuses": [200, 203, 301, 304, 404, 410],
  "cache_without_explicit": false, // 连 ETag/Last-Modified 都没有时是否缓存
  "default_cc": null,              // 例如 {"max-age": 60}：给无 Cache-Control 的响应注入缺省指令（A/B 对比用）
  "respect_request_cc": true       // 是否遵循请求里的 Cache-Control
}
```

## HTTP 接口

所有请求/响应均为 JSON（`Content-Type: application/json`）。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/scenarios` | 创建场景，可同时带 `events` 一次性写入序列 |
| GET | `/scenarios` | 场景列表（含事件数） |
| GET | `/scenarios/{id}` | 场景详情（含 config） |
| PATCH | `/scenarios/{id}` | 修改 `name` / 合并 `config` |
| DELETE | `/scenarios/{id}` | 删除场景（事件与状态级联删除） |
| POST | `/scenarios/{id}/clone` | 复制场景（复制事件序列；默认不复制执行状态，便于从头对比） |
| POST | `/scenarios/{id}/events` | 追加事件：`{"event": {...}}` 或 `{"events": [...]}` |
| GET | `/scenarios/{id}/events` | 事件序列 |
| DELETE | `/scenarios/{id}/events/{eid}` | 删除事件（删改后请 `reset` 重跑） |
| POST | `/scenarios/{id}/run` | 运行；body `{"until": 100}` 只跑到指定虚拟时间，空 body 跑全部 |
| POST | `/scenarios/{id}/reset` | 清空执行状态、时钟归零（保留 config 与事件） |
| GET | `/scenarios/{id}/snapshot?trace=0&results=0` | 快照：时钟、网络、源站(含 delay/fail)、缓存变体(age/ttl/fresh/swr/sie)、计数器、待执行事件、待结算作业(pending_jobs)与作业日志(job_log) |
| GET | `/scenarios/{id}/results` | 已执行事件的完整结果（含 trace） |
| POST/GET | `/scenarios/compare` | 多场景对比（POST `{"ids":[...]}` 或 GET `?ids=a,b`）：回源次数/字节/命中差值与判定序列一致性 |

`run` 是**增量**的：从上次执行状态继续，只运行尚未执行且 `at <= until` 的事件；
状态保存在 SQLite 中，重启服务后可继续。计数器：
`requests / hits / origin_fetches / revalidations / not_modified / stale_served / errors / bytes_served / bytes_from_origin`，
以及陈旧容错与请求合并相关：
`background_revalidations`（调度的后台重验证作业数）、`jobs_settled` / `jobs_failed`（已结算/失败作业数）、
`coalesced_requests`（挂接在途作业的请求数）、`stale_while_revalidate` / `stale_if_error`（两类陈旧回退次数）、
`origin_fetches_saved`（合并节省的回源次数）、`origin_errors`（源站连接失败/5xx 次数）。

每次事件执行前，虚拟时钟先结算所有到期的后台重验证作业；作业结果以 `type: "job"`
的记录进入结果流（`outcome`: `revalidated` / `refreshed` / `failed` / `discarded` / `invalidated`），
`run` 的返回与 `/results` 中均可见。

## curl 示例

```bash
# 1) 创建场景（带完整事件序列）
curl -s -X POST localhost:8000/scenarios -H 'Content-Type: application/json' -d '{
  "name": "etag-demo",
  "events": [
    {"id":"o", "type":"origin", "at":0, "url":"/a", "body":"v1", "etag":"\"v1\"",
     "headers":{"cache-control":"max-age=10"}},
    {"id":"r1","type":"request","at":1,"url":"/a"},
    {"id":"r2","type":"request","at":5,"url":"/a"},
    {"id":"ch","type":"origin_change","at":20,"url":"/a","body":"v2","etag":"\"v2\""},
    {"id":"r3","type":"request","at":21,"url":"/a"}
  ]
}'

# 2) 跑到 t=5（MISS、HIT）
curl -s -X POST localhost:8000/scenarios/<id>/run -d '{"until":5}'

# 3) 继续跑完（ETag 变化 -> REFRESHED）
curl -s -X POST localhost:8000/scenarios/<id>/run -d '{}'

# 4) 看快照（对象老化情况）
curl -s 'localhost:8000/scenarios/<id>/snapshot?trace=0'

# 5) 复制场景，改规则（共享 -> 私有缓存）后重跑对比
curl -s -X POST localhost:8000/scenarios/<id>/clone \
  -H 'Content-Type: application/json' \
  -d '{"name":"private-mode","config":{"cache_mode":"private"}}'

curl -s -X POST localhost:8000/scenarios/<clone>/reset -d '{}'
curl -s -X POST localhost:8000/scenarios/<clone>/run   -d '{}'
curl -s 'localhost:8000/scenarios/compare?ids=<id>,<clone>'

# 6) 重置重跑
curl -s -X POST localhost:8000/scenarios/<id>/reset -d '{}'
```

## 断网 / 失败路径示例

```jsonc
// max-age=10, must-revalidate
{"type":"origin","at":0,"url":"/m","etag":"\"e\"",
 "headers":{"cache-control":"max-age=10, must-revalidate"}}
{"type":"request","at":1,"url":"/m"}                 // MISS
{"type":"network","at":20,"up":false}                // 断网
{"type":"request","at":21,"url":"/m"}                // ERROR 504（must-revalidate 禁止陈旧兜底）
// 对照：同样的时间线用 "max-age=10"（无 must-revalidate）
{"type":"request","at":21,"url":"/m"}                // STALE 200 + Warning: 110
{"type":"network","at":30,"up":true}                 // 恢复
{"type":"request","at":31,"url":"/m"}                // REVALIDATED 304
```

## 陈旧容错（SWR/SIE）与请求合并示例

```jsonc
// 源站：响应延迟 5s；max-age=10，SWR 窗口 30s，SIE 窗口 60s
{"type":"origin","at":0,"url":"/feed","body":"v1","etag":"\"f1\"","delay":5,
 "headers":{"cache-control":"max-age=10, stale-while-revalidate=30, stale-if-error=60"}}
{"type":"request","at":1,"url":"/feed"}    // MISS（响应 t=6 到达，寿命从到达时刻起算）
{"type":"request","at":20,"url":"/feed"}   // STALE_WHILE_REVALIDATE：直接返回陈旧副本，
                                           // 调度后台重验证作业（t=25 由虚拟时钟结算）
{"type":"request","at":21,"url":"/feed"}   // 挂接在途作业（coalesced），返回陈旧副本，
                                           // origin_fetches_saved +1
{"type":"request","at":30,"url":"/feed"}   // 作业已结算(304)，复用结果 -> HIT
{"type":"origin_change","at":35,"url":"/feed","status":503}   // 源站开始 5xx
{"type":"request","at":70,"url":"/feed"}   // 已出 SWR 窗口但在 SIE 窗口内
                                           // -> STALE_IF_ERROR 回退陈旧副本
{"type":"origin_change","at":80,"url":"/feed","status":200,"fail":true}
{"type":"request","at":81,"url":"/feed"}   // 连接失败路径：STALE（声明了 SIE 则受其窗口约束）
```

规则要点：

- **SWR 优先级**：`must-revalidate`/`no-cache`（响应或请求）、请求 `max-age`/`min-fresh`
  约束不满足时不走 SWR，仍同步重验证；Range 片段沿用既有规则（区间重验证遇 5xx 时
  同样可按 SIE 回退 206）。
- **请求合并**：同一虚拟时刻、同一缓存键 + Vary 变体最多一个在途重验证作业；
  后续请求挂接（`stale_info.coalesced=true`）或在作业结算后直接 HIT 复用结果。
- **作业持久化**：待结算作业随执行状态写入 SQLite，重启后虚拟时钟推进到
  `finish_at` 时继续结算；结算时条目已清除则丢弃（`discarded`），验证器变化则
  全量替换（`refreshed`），5xx/连接失败保留陈旧条目（`failed`）。
- **5xx 语义**：源站 5xx 不失效已有缓存条目；无 SIE 窗口时透传 5xx（verdict `ERROR`）。
- 快照 `pending_jobs` 查看在途作业，`job_log` 查看最近结算记录；`compare` 输出
  `delta_background_revalidations`、`delta_coalesced_requests`、`delta_stale_*`、
  `delta_origin_fetches_saved`、`delta_origin_errors` 等差值。

## A/B 对比工作流

1. 建好基础场景并跑完；
2. `clone` 出副本，在 `config` 里调整规则（如 `default_cc: {"max-age": 60}`、
   `cache_mode: "private"`、`heuristic_cache: true`）；
3. 对副本 `reset`（clone 默认不带执行状态）后 `run`；
4. `POST /scenarios/compare {"ids":[base, variant1, variant2]}` 直接得到
   `delta_origin_fetches`、`delta_bytes_from_origin`、`delta_hits`、`delta_errors`
   以及判定序列是否一致（`same_verdict_sequence`）。

## 数据存储

SQLite（默认 `cache_sim.db`）三张表：`scenarios`（配置）、`events`（事件序列）、
`states`（可恢复的完整执行状态 JSON），另有 `runs` 记录每次运行。
删除库文件即可完全清空；`:memory:` 或临时文件适合测试。

## 目录结构

```
cache_sim/
  engine.py     # 纯逻辑仿真引擎（虚拟时钟、Cache-Control/条件请求/Vary、trace）
  store.py      # SQLite 持久化：场景、事件、状态、增量运行、克隆
  server.py     # http.server HTTP API
  __main__.py   # python -m cache_sim 入口
examples/demo.py
tests/test_engine.py    # 73 个引擎/存储语义测试
tests/test_http_api.py  # 4 个 HTTP 端到端测试
```

## 仿真边界说明

- 这是**策略验证工具**而非真实 HTTP 栈：不建 TCP 连接、不做真传输，字节数按
  状态行 + 头 + body 长度估算，用于横向对比而非绝对性能数据。
- 源站是“定义式”的：用 `origin` 事件声明资源当前响应，条件请求是否 304 由当前
  ETag / Last-Modified 与请求头比较决定。
- 只缓存 GET 响应条目（方法参与缓存键）；客户端自带条件请求会被转发到源站。
- 事件按虚拟时间戳顺序处理，无真并发：前台回源（含 `delay`）在事件内同步完成，
  延迟只影响条目寿命起算时刻（`stored_at = at + delay`）；只有 SWR 后台重验证
  是异步作业，由后续事件的虚拟时钟推进结算。
