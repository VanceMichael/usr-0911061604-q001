# 后厨影像证据服务（kitchen-evidence）

连锁餐饮后厨摄像头片段的**纯后端索引服务**。门店设备上报带时间戳的事件，服务按
`门店 + 摄像头` 维护一条 **SHA-256 哈希链**，索引与校验摘要写入 PostgreSQL，
索引建立后通过 **Redis Streams** 异步通知下游。面向三类真实门店问题：

- **网络抖动 / 断线重传**：设备用稳定的 `event_id` 重发，服务幂等处理；
  被拒事件进入失败重传台账，可随时查询尝试次数与最终状态。
- **投诉复核 / 防篡改**：每条记录的摘要包含前一条摘要（哈希链），
  任何对历史片段引用、时间戳、摘要的事后修改都会在链校验中被定位到具体序号。
- **时钟漂移 / 片段缺失**：不静默接受——返回带门店、摄像头、序号、event_id
  的结构化错误（HTTP 422），设备修正后用同一 `event_id` 重传即可。

## 技术栈

- Go 1.24 + Gin（HTTP）
- PostgreSQL 17（`pgx/v5`，启动时自动幂等迁移；行级 `FOR UPDATE` 串行化同一摄像头写入）
- Redis 8 Streams（`go-redis`，后台 worker 投递，失败指数退避重试）
- 全部配置经环境变量注入

## 快速开始

```bash
docker compose up --build
curl -s http://localhost:8080/health
# {"status":"ok"}
```

然后运行验收脚本（容器内 PG/Redis 已就绪）：

```bash
BASE_URL=http://localhost:8080 ./scripts/acceptance.sh
```

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HTTP_ADDR` | `:8080` | 监听地址 |
| `DATABASE_URL` | *必填* | PostgreSQL DSN |
| `REDIS_URL` | *必填* | Redis DSN |
| `STREAM_NAME` | `kitchen.clip.indexed` | 片段索引通知流 |
| `MAX_CLOCK_SKEW_SECONDS` | `30` | 设备时钟**超前**服务器的允许上限 |
| `MAX_PAST_WINDOW_SECONDS` | `604800` | 断线重传允许的迟到窗口（默认 7 天，超过判为时钟后漂） |
| `GAP_TOLERANCE` | `1` | 相邻事件序号最大间隔；1 = 必须连续 |
| `RETRY_BASE_INTERVAL_SECONDS` | `5` | Stream 投递退避基数（封顶 1 分钟） |
| `PG_POOL_MAX` | `10` | PG 连接池上限 |

时钟检查是**非对称**的：片段迟到（在过去）是断线重传的正常形态，由较宽的
`MAX_PAST_WINDOW_SECONDS` 兜底；只有“未来时间”才用严格的秒级阈值拦截。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/events` | 上报事件（幂等键为 `event_id`） |
| GET | `/api/v1/stores/:store_id/cameras/:camera_id/events?from=&to=&limit=` | 按时间范围查片段索引（RFC3339，半开区间 `[from,to)`） |
| GET | `/api/v1/events/:event_id` | 按幂等键取单条索引 |
| GET | `/api/v1/retries/:event_id` | 查询某次失败上报的重传状态 |
| GET | `/api/v1/retries?store_id=&resolved=true` | 失败重传台账 |
| GET | `/api/v1/stores/:store_id/cameras/:camera_id/verify` | 哈希链完整性复核 |

### 上报报文

```json
{
  "event_id": "5f8e1c0a-9b2e-4d77-9f10-2a1b3c4d5e6f",
  "store_id": "store-sh-01",
  "camera_id": "cam-fryer-02",
  "sequence_no": 42,
  "occurred_at": "2026-09-19T06:00:00Z",
  "clip_uri": "s3://kitchen-clips/2026/09/19/42.mp4",
  "clip_sha256": "可选：片段文件本身的 SHA256",
  "duration_ms": 15000
}
```

成功：

```json
{
  "result": "accepted",
  "attempt": 1,
  "idempotent": false,
  "event": { "...": "含 prev_hash / record_hash 的完整索引" }
}
```

重复上报同一 `event_id` 且载荷完全一致：HTTP 200，`result="duplicate"`、
`idempotent=true`，返回既有记录，**不产生新行**。同一 `event_id` 但载荷不一致
（疑似伪造/串号）返回 `DUPLICATE_ID_MISMATCH`。

### 错误响应（均可定位）

```json
{
  "error": {
    "code": "SEQUENCE_GAP",
    "message": "检测到片段缺失：期望序号 2，实际收到 5，缺失序号 [2 3 4]；缺失片段补齐后可重传",
    "store_id": "store-sh-01",
    "camera_id": "cam-fryer-02",
    "sequence_no": 5,
    "event_id": "evt-005"
  },
  "retry": { "status_url": "/api/v1/retries/evt-005", "hint": "..." }
}
```

错误码：`CLOCK_SKEW_DETECTED`、`SEQUENCE_GAP`、`SEQUENCE_CONFLICT`、
`EVENT_OUT_OF_ORDER`、`DUPLICATE_ID_MISMATCH`、`VALIDATION_ERROR`。

## 不可篡改性如何成立

每条事件落库前：

1. 对规范化载荷（UTC、微秒精度、SHA 小写归一）求 `payload_hash`；
2. `record_hash = SHA256(prev_record_hash | payload_hash | event_id | seq | occurred_at | received_at)`；
3. `events` 表同时保存 `payload_hash`、`prev_hash`、`record_hash`，
   摄像头维度的链头保存在 `cameras` 表。

`GET .../verify` 从库中重放整链重算摘要：任何一环的载荷/前向指针被改动，
都会在第一个失配序号处返回 `intact=false`、断链序号与期望/实际摘要。
微秒截断保证“入库时算的哈希”与“事后用列值重算的哈希”口径一致（PG
`timestamptz` 无纳秒精度）。

## 验收点对应关系

`scripts/acceptance.sh` 用纯 HTTP 断言：

1. **两次相同上报只产生一条记录**：第二次返回 `duplicate`，时间范围查询 `count=1`；
2. **重启后仍可查到索引**：数据在 PG 命名卷 `pgdata`，见下节手工步骤；
3. **一条失败重传的明确状态**：跳号上报两次 → `/api/v1/retries/<id>`
   返回 `reason=SEQUENCE_GAP, attempts=2, resolved=false`；补齐重传后
   `resolved=true` 并带 `resolved_at`；
4. 时钟漂移拒绝、哈希链复核完整、库内篡改可被定位（集成测试覆盖）。

验证重启持久性（compose）：

```bash
docker compose up --build -d
BASE_URL=http://localhost:8080 ./scripts/acceptance.sh        # 写入
docker compose restart app                                    # 仅重启应用
curl -s "http://localhost:8080/api/v1/events/<event_id>"      # 索引仍在
# 连 PG 数据卷也保留：docker compose down 后再 up -d，数据同样不丢（卷未删除）
```

## 测试

```bash
go test ./...                                   # 单元测试（无外部依赖）
TEST_DATABASE_URL=postgres://app@localhost:5432/app?sslmode=disable \
  go test ./internal/store/ -v                  # 集成测试（幂等/缺口/漂移/链/篡改）
```

## 目录结构

```
internal/
  config/    环境变量配置
  domain/    领域模型与可定位错误码
  postgres/  连接池（启动重试）与 embed SQL 迁移
  store/     事务化入库：幂等、漂移/缺口/乱序检测、哈希链、重传台账、查询、链复核
  notify/    Redis Streams 异步通知 worker（退避重试）
  api/       Gin handlers
scripts/acceptance.sh   端到端验收脚本
```
