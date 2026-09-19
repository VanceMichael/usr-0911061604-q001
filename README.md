# 后厨影像证据索引服务

连锁餐饮门店后厨摄像头的关键片段向顾客公开前，先在这里登记**不可篡改的片段索引**。
门店网关上报带时间戳的事件，服务按（门店, 摄像头）维护哈希链索引与内容摘要，
支持断线重传、重复上报幂等、按时间范围复核查询；设备时钟漂移或片段缺失时
返回**可定位的错误**，绝不静默接受。

- 索引与校验摘要：**PostgreSQL**
- 异步通知：**Redis Streams**（消费组示例消费者随服务启动）
- 配置：全部通过**环境变量**
- 交付：`Dockerfile` + `docker-compose.yml` 一键起栈

## 快速开始

```bash
docker compose up --build -d     # 启动 api + postgres + redis
curl localhost:8080/healthz      # {"postgres":"up","redis":"up","status":"ok"}
./scripts/acceptance.sh          # 跑完整验收流
./scripts/acceptance.sh --restart  # 额外验证“重启后索引仍可查”
```

本地开发（无需 Docker 的单元测试；集成测试用内嵌 PG + miniredis）：

```bash
make test        # 单元测试
make test-it     # 集成测试：真实 PostgreSQL 17 + 内存 Redis 跑完整验收流
```

## 验收对照

| 验收要求 | 实现 | 验证方式 |
|---|---|---|
| 两次相同上报只产生一条记录 | `(store_id, camera_id, event_id)` 唯一约束 + 幂等返回原记录 | `acceptance.sh` 步骤 1–3：第二次返回 `200 duplicate` 与相同记录 id，库中仅 1 行 |
| 重启服务后仍可查到索引 | 索引落 PostgreSQL（compose `pgdata` 卷持久化），迁移幂等 | `acceptance.sh --restart`：`docker compose restart api` 后查询仍返回 3 条 |
| 失败重传有明确状态 | 每次上报（含被拒绝的）写入 `ingest_attempts` 审计 | `acceptance.sh` 步骤 7–9：`409 IDEMPOTENCY_CONFLICT` 等错误即时返回，并可在 `GET /attempts` 复查 |
| 时钟漂移/片段缺失不静默 | 校验拦截，返回可定位错误码与细节 | 步骤 4（`SEQUENCE_GAP` 含 `expected_seq`）、步骤 8（`CLOCK_DRIFT_FUTURE` 含服务端时间与容忍度） |

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/v1/events` | 上报片段事件。新记录 `201 accepted`；幂等命中 `200 duplicate`；冲突/缺失/漂移 `4xx` |
| `GET` | `/v1/stores/:store/cameras/:cam/events?from=&to=&after_seq=&limit=` | 按时间范围查询索引（`seq` 游标分页） |
| `GET` | `/v1/stores/:store/cameras/:cam/events/:event_id` | 按幂等键查单条 |
| `GET` | `/v1/stores/:store/cameras/:cam/chain/verify` | 回放哈希链，证明索引未被篡改 |
| `GET` | `/v1/stores/:store/cameras/:cam/attempts?limit=` | 上报尝试审计（含失败重传及原因） |
| `GET` | `/healthz` | 进程与依赖（PG/Redis）健康状态 |

### 上报示例

```json
POST /v1/events
{
  "event_id": "evt-0001",               // 设备生成的幂等键
  "store_id": "store-sh-001",
  "camera_id": "cam-kitchen-01",
  "seq": 1,                              // 摄像头维度从 1 连续递增
  "event_type": "clip.ready",
  "occurred_at": "2026-09-19T08:00:00Z", // 设备时钟，用于漂移校验
  "clip": {
    "start_ts": "2026-09-19T07:59:30Z",
    "end_ts":   "2026-09-19T08:00:00Z",
    "uri": "s3://clips/store-sh-001/cam-kitchen-01/evt-0001.mp4",
    "media_hash": "sha256:<64 hex>"      // 片段内容摘要，裸 hex 亦可
  }
}
```

`201` 响应携带完整索引记录（含 `payload_hash` / `prev_chain_hash` / `chain_hash`）与
`notified`（异步通知是否已投递到 Redis Stream）。

### 错误响应（可定位）

```json
HTTP 409
{
  "error": {
    "code": "SEQUENCE_GAP",
    "message": "片段缺失：期望序号 2，收到 3，请先补传缺失的 1 个片段",
    "details": {"store_id": "...", "camera_id": "...", "expected_seq": 2, "received_seq": 3, "missing": 1}
  },
  "request_id": "8f3d..."
}
```

| 错误码 | HTTP | 含义 |
|---|---|---|
| `VALIDATION_FAILED` | 400 | 字段缺失/格式非法（含 `media_hash` 非 64 位 hex） |
| `CLOCK_DRIFT_FUTURE` | 422 | `occurred_at` 超前服务端超过 `CLOCK_DRIFT_TOLERANCE`，设备需校时 |
| `EVENT_TOO_OLD` | 422 | 事件年龄超过 `MAX_BACKFILL_AGE` 回传窗口 |
| `SEQUENCE_GAP` | 409 | 中间缺片段，`details` 给出 `expected_seq`，补传后重发即可 |
| `SEQUENCE_REPLAYED` | 409 | 该序号已被其他事件占用（疑似设备序号重置或数据被改动） |
| `IDEMPOTENCY_CONFLICT` | 409 | 同一 `event_id` 携带不同内容摘要，拒绝覆盖 |
| `NOT_FOUND` | 404 | 查询对象不存在 |
| `INTERNAL` | 500 | 内部错误，凭 `request_id` 排查 |

## 设计要点

**幂等与断线重传。** 设备为每个事件生成 `event_id`；数据库对
`(store_id, camera_id, event_id)` 建唯一约束。重传命中时比对 `payload_hash`：
一致 → `200 duplicate` 返回原记录（不产生第二行）；不一致 → `409 IDEMPOTENCY_CONFLICT`。
写路径在事务内对 `camera_chains` 链头行 `SELECT ... FOR UPDATE`，
同一摄像头的写入串行化；并发重传最终由唯一约束兜底，仍按上述规则判定。

**片段完整性。** `seq` 必须等于 `last_seq + 1`：跳号返回 `SEQUENCE_GAP`（附期望序号），
门店网关补传缺口后重发即可；小于等于已确认序号且幂等键未见的，返回 `SEQUENCE_REPLAYED`。
缺口/占用都**不会**落库，也不会消耗序号。

**不可篡改索引。** 每条记录：
`payload_hash = SHA256("P1" ‖ canonical(event))`，
`chain_hash  = SHA256("C1" ‖ prev_chain_hash ‖ payload_hash)`，创世前驱为 64 个 `0`。
规范串字段顺序固定、时间统一 UTC RFC3339Nano，分隔符用 ASCII 单元分隔符。
任何历史记录被改动都会使后续链哈希失配；`GET /chain/verify` 全量回放并与
`camera_chains` 链头比对，输出首个断点序号。

**时钟校验。** `occurred_at` 超前服务端超过容忍度（默认 5m）→ `CLOCK_DRIFT_FUTURE`；
早于最大回补窗口（默认 168h）→ `EVENT_TOO_OLD`。两者都附服务端时间与阈值，便于门店定位。

**异步通知。** 索引落库后 `XADD` 到 `clip.events`（近似 MAXLEN 10 万）；响应中的
`notified=false` 表示本次通知投递失败（索引已安全落库，可由对账任务补偿）。
服务内置一个消费组示例消费者（`ENABLE_STREAM_CONSUMER=true`），收到通知打日志并 ACK，
下游（公开页渲染、投诉复核预取）可参照接入。

**审计。** 每次上报尝试（accepted / duplicate / rejected 含错误码与原因）写入
`ingest_attempts`，投诉复核时通过 `GET /attempts` 可重现“失败重传”的完整轨迹。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `HTTP_PORT` | `8080` | 服务端口 |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/kitchen?sslmode=disable` | PostgreSQL 连接串 |
| `REDIS_ADDR` / `REDIS_PASSWORD` / `REDIS_DB` | `localhost:6379` / 空 / `0` | Redis 连接 |
| `REDIS_STREAM` | `clip.events` | 通知 Stream 名 |
| `REDIS_CONSUMER_GROUP` | `clip-indexer` | 示例消费者的消费组 |
| `ENABLE_STREAM_CONSUMER` | `true` | 是否启动内置示例消费者 |
| `CLOCK_DRIFT_TOLERANCE` | `5m` | `occurred_at` 允许超前服务端的时长 |
| `MAX_BACKFILL_AGE` | `168h` | 断线重传允许回补的最大事件年龄 |
| `SHUTDOWN_TIMEOUT` | `10s` | 优雅退出超时 |

## 仓库结构

```
cmd/server/            进程入口：配置、连接重试、迁移、优雅退出
internal/config/       环境变量配置
internal/domain/       领域类型与可定位错误码
internal/chain/        规范串、payload/链哈希、回放校验
internal/store/        PostgreSQL：幂等写入事务、查询、审计、嵌入式迁移
internal/service/      校验编排、审计留痕、通知触发
internal/notify/       Redis Streams 发布器与消费组消费者
internal/httpapi/      Gin 路由、错误映射、request-id
internal/itest/        集成测试（embedded-postgres + miniredis 跑验收流）
scripts/acceptance.sh  验收脚本
```
