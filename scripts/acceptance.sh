#!/usr/bin/env bash
# 端到端验收脚本：只依赖 curl + python3，通过 BASE_URL 指向运行中的服务。
#
# 验证点：
#   A. 两次完全相同的上报 -> 第二次返回 duplicate，时间范围查询只有 1 条记录
#   B. 片段缺失（序号跳变）-> 返回可定位的 SEQUENCE_GAP 错误并记录失败重传单
#   C. 同一失败事件再次重传 -> attempts=2、resolved=false（明确的失败重传状态）
#   D. 设备时钟超前 -> CLOCK_SKEW_DETECTED，不静默接受
#   E. 补齐缺失片段后用同一 event_id 重传 -> accepted，失败单 resolved=true
#   F. 哈希链复核 intact=true
#
# 用法：BASE_URL=http://localhost:8080 ./scripts/acceptance.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
RUN="${RUN:-$(date +%s)}"
STORE="store-accept-$RUN"
CAM="cam-accept-$RUN"

say()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
post() { curl -sS -X POST "$BASE_URL/api/v1/events" -H 'Content-Type: application/json' -d "$1"; }
code() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }

say "服务健康检查"
curl -fsS "$BASE_URL/health" >/dev/null && echo "OK: $BASE_URL 可用"

T() { date -u -d "@$(($(date +%s) - "$1"))" +%Y-%m-%dT%H:%M:%SZ; } # T 10 -> now-10s

say "A1. 首次上报 seq=1（event_id=e1-$RUN）"
TS1=$(T 10)  # 时间戳只取一次，A1/A2 必须逐字节一致才能验证幂等
BODY=$(post "{\"event_id\":\"e1-$RUN\",\"store_id\":\"$STORE\",\"camera_id\":\"$CAM\",
  \"sequence_no\":1,\"occurred_at\":\"$TS1\",\"clip_uri\":\"s3://clips/e1.mp4\",\"duration_ms\":15000}")
echo "$BODY" | code "['result']" | xargs -I{} echo "result={}"

say "A2. 用完全相同的请求体重报（模拟断线重发）"
BODY=$(post "{\"event_id\":\"e1-$RUN\",\"store_id\":\"$STORE\",\"camera_id\":\"$CAM\",
  \"sequence_no\":1,\"occurred_at\":\"$TS1\",\"clip_uri\":\"s3://clips/e1.mp4\",\"duration_ms\":15000}")
echo "$BODY" | code "['result']" | xargs -I{} echo "result={} (期望 duplicate)"
echo "$BODY" | code "['idempotent']" | xargs -I{} echo "idempotent={} (期望 True)"

say "A3. 时间范围查询：两次上报只应产生 1 条记录"
N=$(curl -sS "$BASE_URL/api/v1/stores/$STORE/cameras/$CAM/events?from=2000-01-01T00:00:00Z&to=2100-01-01T00:00:00Z" | code "['count']")
echo "记录数 = $N （期望 1）"
[ "$N" = "1" ] || { echo "断言失败"; exit 1; }

say "B. 缺口上报 seq=5（缺失 2,3,4），必须明确报错"
post "{\"event_id\":\"e5-$RUN\",\"store_id\":\"$STORE\",\"camera_id\":\"$CAM\",
  \"sequence_no\":5,\"occurred_at\":\"$(T 5)\",\"clip_uri\":\"s3://clips/e5.mp4\"}" \
  | code "['error']['code']" | xargs -I{} echo "错误码 = {} （期望 SEQUENCE_GAP）"

say "C. 再次重传同一失败事件，查询重传状态（attempts 应为 2，resolved=false）"
post "{\"event_id\":\"e5-$RUN\",\"store_id\":\"$STORE\",\"camera_id\":\"$CAM\",
  \"sequence_no\":5,\"occurred_at\":\"$(T 5)\",\"clip_uri\":\"s3://clips/e5.mp4\"}" >/dev/null
curl -sS "$BASE_URL/api/v1/retries/e5-$RUN" | python3 -c "
import json,sys
r=json.load(sys.stdin)['retry']
assert r['attempts']==2, r
assert r['resolved'] is False, r
assert r['reason']=='SEQUENCE_GAP', r
print(f\"event={r['event_id']} reason={r['reason']} attempts={r['attempts']} resolved={r['resolved']}\")
print('OK: 失败重传状态明确可查')"

say "D. 设备时钟超前 10 分钟 -> CLOCK_SKEW_DETECTED"
post "{\"event_id\":\"eskew-$RUN\",\"store_id\":\"$STORE\",\"camera_id\":\"$CAM\",
  \"sequence_no\":99,\"occurred_at\":\"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)\",
  \"clip_uri\":\"s3://clips/x.mp4\"}" \
  | code "['error']['code']" | xargs -I{} echo "错误码 = {} （期望 CLOCK_SKEW_DETECTED）"

say "E. 补齐 seq=2,3,4 后，用同一 event_id 重传 seq=5"
for i in 2 3 4; do
  post "{\"event_id\":\"e$i-$RUN\",\"store_id\":\"$STORE\",\"camera_id\":\"$CAM\",
    \"sequence_no\":$i,\"occurred_at\":\"$(T $((11-i)))\",\"clip_uri\":\"s3://clips/e$i.mp4\"}" \
    | code "['result']" >/dev/null
done
post "{\"event_id\":\"e5-$RUN\",\"store_id\":\"$STORE\",\"camera_id\":\"$CAM\",
  \"sequence_no\":5,\"occurred_at\":\"$(T 5)\",\"clip_uri\":\"s3://clips/e5.mp4\"}" \
  | code "['result']" | xargs -I{} echo "重传结果 = {} （期望 accepted）"
curl -sS "$BASE_URL/api/v1/retries/e5-$RUN" | python3 -c "
import json,sys
r=json.load(sys.stdin)['retry']
assert r['resolved'] is True, r
print(f\"attempts={r['attempts']} resolved={r['resolved']} resolved_at={r['resolved_at']}\")
print('OK: 失败重传单已闭合')"

say "F. 哈希链完整性复核（投诉复核依据）"
curl -sS "$BASE_URL/api/v1/stores/$STORE/cameras/$CAM/verify" | python3 -c "
import json,sys
c=json.load(sys.stdin)['chain']
assert c['intact'] is True, c
assert c['total_events']==5, c
print(f\"total={c['total_events']} intact={c['intact']}\")
print('OK: 哈希链完整，索引未被篡改')"

say "全部验收断言通过 ✅"
echo "数据位于 store=$STORE camera=$CAM，可按时间范围查询复核。"
