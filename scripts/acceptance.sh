#!/usr/bin/env bash
# 验收脚本：模拟门店上报，验证幂等、缺口报错、失败重传状态、链校验与重启持久化。
# 用法:
#   ./scripts/acceptance.sh            # 对 $BASE_URL 执行全部验收步骤
#   ./scripts/acceptance.sh --restart  # 额外执行 "docker compose restart api" 并验证重启后索引仍在
# 环境变量: BASE_URL (默认 http://localhost:8080), COMPOSE (默认 docker compose)
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8080}"
COMPOSE="${COMPOSE:-docker compose}"
STORE="store-sh-001"
CAM="cam-kitchen-01"

if command -v jq >/dev/null 2>&1; then JQ="jq ."; else JQ="cat"; fi

banner() { printf '\n\033[1m=== %s ===\033[0m\n' "$1"; }

ts() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }

post() { # post <request_id> <json>
  curl -sS -w '\nHTTP %{http_code}\n' \
    -H 'Content-Type: application/json' -H "X-Request-ID: $1" \
    -d "$2" "$BASE/v1/events"
}

get() { curl -sS "$BASE$1" | eval "$JQ"; }

NOW=$(date -u +%s)
T1=$((NOW - 3600))  # evt-0001 发生时间
T2=$((NOW - 3300))
T3=$((NOW - 3000))

evt() { # evt <event_id> <seq> <occurred_epoch> <hash_char>
  cat <<JSON
{"event_id":"$1","store_id":"$STORE","camera_id":"$CAM","seq":$2,"event_type":"clip.ready",
 "occurred_at":"$(ts "$3")",
 "clip":{"start_ts":"$(ts $(($3 - 30)))","end_ts":"$(ts "$3")",
         "uri":"s3://clips/$STORE/$CAM/$1.mp4",
         "media_hash":"sha256:$(printf "$4%.0s" $(seq 64))"}}
JSON
}

banner "0. 健康检查 GET /healthz"
get /healthz

banner "1. 首次上报 evt-0001 (期望 201 accepted, notified=true)"
post req-1 "$(evt evt-0001 1 "$T1" a)"

banner "2. 完全相同的上报再发一次 (期望 200 duplicate, 同一条记录 id)"
post req-2 "$(evt evt-0001 1 "$T1" a)"

banner "3. 数据库行数应为 1 (需要本机 docker)"
if command -v docker >/dev/null 2>&1; then
  $COMPOSE exec -T postgres psql -U app -d kitchen \
    -tAc "SELECT count(*) AS rows_should_be_1 FROM clip_events WHERE store_id='$STORE' AND camera_id='$CAM';" || true
else
  echo "跳过: 未检测到 docker"
fi

banner "4. 断线重传乱序: 直接补 seq=3 (期望 409 SEQUENCE_GAP, 定位 expected_seq=2)"
post req-4 "$(evt evt-0003 3 "$T3" c)"

banner "5. 补传缺失的 seq=2 (期望 201)"
post req-5 "$(evt evt-0002 2 "$T2" b)"

banner "6. 重传 seq=3 (期望 201, 缺口已补齐)"
post req-6 "$(evt evt-0003 3 "$T3" c)"

banner "7. 失败重传: 同一 event_id 内容被改动 (期望 409 IDEMPOTENCY_CONFLICT)"
post req-7 "$(evt evt-0001 1 "$T1" f)"

banner "8. 设备时钟漂移: occurred_at 在未来 2 小时 (期望 422 CLOCK_DRIFT_FUTURE)"
post req-8 "$(evt evt-future 4 $((NOW + 7200)) d)"

banner "9. 审计: 失败重传的明确状态 GET /attempts (应见 SEQUENCE_GAP / IDEMPOTENCY_CONFLICT / CLOCK_DRIFT_FUTURE)"
get "/v1/stores/$STORE/cameras/$CAM/attempts?limit=10"

banner "10. 按时间范围查询索引 GET /events?from&to (应返回 3 条)"
get "/v1/stores/$STORE/cameras/$CAM/events?from=$(ts $((NOW - 7200)))&to=$(ts $((NOW + 600)))"

banner "11. 哈希链回放校验 GET /chain/verify (期望 valid=true, checked=3)"
get "/v1/stores/$STORE/cameras/$CAM/chain/verify"

banner "12. Redis Streams 中的异步通知 (应见 3 条 accepted 通知)"
if command -v docker >/dev/null 2>&1; then
  $COMPOSE exec -T redis redis-cli XRANGE clip.events - + || true
else
  echo "跳过: 未检测到 docker"
fi

if [[ "${1:-}" == "--restart" ]]; then
  banner "13. 重启服务 $COMPOSE restart api"
  $COMPOSE restart api
  sleep 4
  banner "14. 重启后索引仍可查 (期望仍返回 3 条)"
  get "/v1/stores/$STORE/cameras/$CAM/events?from=$(ts $((NOW - 7200)))&to=$(ts $((NOW + 600)))"
else
  banner "13. 重启持久化验证"
  echo "执行: $COMPOSE restart api && $0 --restart"
fi
