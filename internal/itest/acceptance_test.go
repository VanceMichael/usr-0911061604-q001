// Package itest 用真实 PostgreSQL（embedded-postgres， zonky 二进制）与
// 内存 Redis（miniredis）跑完整验收流：幂等、缺口、冲突、漂移、重启持久化、
// Redis Streams 通知。运行：go test ./internal/itest/ （首次会下载 PG 二进制）。
package itest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"kitchen-evidence/internal/httpapi"
	"kitchen-evidence/internal/notify"
	"kitchen-evidence/internal/service"
	"kitchen-evidence/internal/store"
)

const (
	testStream = "clip.events"
	testStore  = "store-sh-001"
	testCam    = "cam-kitchen-01"
)

func TestAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("集成测试：跳过（-short）")
	}
	ctx := context.Background()

	// 1. 拉起真实 PostgreSQL 与内存 Redis。
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.PostgresVersion("17.11.0")).
		Port(15433).
		Logger(io.Discard))
	if err := pg.Start(); err != nil {
		t.Fatalf("启动内嵌 PostgreSQL 失败: %v", err)
	}
	defer func() { _ = pg.Stop() }()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:15433/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("连接 PostgreSQL: %v", err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}

	gin.SetMode(gin.TestMode)
	newServer := func() *httptest.Server {
		st := store.New(pool)
		pub := notify.NewStreamPublisher(rdb, testStream)
		svc := service.New(st, pub, service.Limits{
			ClockDriftTolerance: 5 * time.Minute,
			MaxBackfillAge:      7 * 24 * time.Hour,
		}, nil)
		return httptest.NewServer(httpapi.NewRouter(svc, st, notify.Pinger{Client: rdb}))
	}
	srv := newServer()
	defer srv.Close()

	now := time.Now().UTC()
	post := func(body string) (int, map[string]any) {
		resp, err := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST /v1/events: %v", err)
		}
		defer resp.Body.Close()
		var parsed map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&parsed)
		return resp.StatusCode, parsed
	}
	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		var parsed map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&parsed)
		return resp.StatusCode, parsed
	}
	eventJSON := func(eventID string, seq int64, occurred time.Time, media string) string {
		return fmt.Sprintf(`{"event_id":%q,"store_id":%q,"camera_id":%q,"seq":%d,"event_type":"clip.ready",
			"occurred_at":%q,"clip":{"start_ts":%q,"end_ts":%q,"uri":"s3://clips/%s.mp4","media_hash":%q}}`,
			eventID, testStore, testCam, seq,
			occurred.Format(time.RFC3339), occurred.Add(-30*time.Second).Format(time.RFC3339),
			occurred.Format(time.RFC3339), eventID, media)
	}
	media := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }

	// 2. 验收点一：两次完全相同的上报只产生一条记录。
	code, r1 := post(eventJSON("evt-0001", 1, now.Add(-time.Hour), media("a")))
	if code != http.StatusCreated || r1["status"] != "accepted" {
		t.Fatalf("首次上报应 201 accepted: %d %v", code, r1)
	}
	if r1["notified"] != true {
		t.Fatalf("新记录应投递异步通知: %v", r1)
	}
	code, r2 := post(eventJSON("evt-0001", 1, now.Add(-time.Hour), media("a")))
	if code != http.StatusOK || r2["status"] != "duplicate" {
		t.Fatalf("重复上报应 200 duplicate: %d %v", code, r2)
	}
	if r1["event"].(map[string]any)["id"] != r2["event"].(map[string]any)["id"] {
		t.Fatal("重复上报应返回同一条记录")
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM clip_events WHERE store_id=$1 AND camera_id=$2`, testStore, testCam).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("两次相同上报后表中应有且仅有 1 条记录，实际 %d", count)
	}

	// 3. 验收点二：片段缺失返回可定位错误，补传后重传成功。
	code, r3 := post(eventJSON("evt-0003", 3, now.Add(-50*time.Minute), media("c")))
	if code != http.StatusConflict || r3["error"].(map[string]any)["code"] != "SEQUENCE_GAP" {
		t.Fatalf("跳号应 409 SEQUENCE_GAP: %d %v", code, r3)
	}
	details := r3["error"].(map[string]any)["details"].(map[string]any)
	if details["expected_seq"].(float64) != 2 {
		t.Fatalf("错误应定位到期望序号 2: %v", details)
	}
	if code, _ := post(eventJSON("evt-0002", 2, now.Add(-55*time.Minute), media("b"))); code != http.StatusCreated {
		t.Fatalf("补传 seq=2 应成功: %d", code)
	}
	if code, _ := post(eventJSON("evt-0003", 3, now.Add(-50*time.Minute), media("c"))); code != http.StatusCreated {
		t.Fatalf("补传后重传 seq=3 应成功: %d", code)
	}

	// 4. 验收点三：同一幂等键内容被改动 → 明确的失败重传状态。
	code, r4 := post(eventJSON("evt-0001", 1, now.Add(-time.Hour), media("f")))
	if code != http.StatusConflict || r4["error"].(map[string]any)["code"] != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("被改动的重传应 409 IDEMPOTENCY_CONFLICT: %d %v", code, r4)
	}

	// 5. 设备时钟漂移 → 422，且不消耗序号。
	code, r5 := post(eventJSON("evt-future", 4, now.Add(2*time.Hour), media("d")))
	if code != http.StatusUnprocessableEntity ||
		r5["error"].(map[string]any)["code"] != "CLOCK_DRIFT_FUTURE" {
		t.Fatalf("时钟漂移应 422 CLOCK_DRIFT_FUTURE: %d %v", code, r5)
	}

	// 6. 失败重传在审计中可见。
	code, r6 := get(fmt.Sprintf("/v1/stores/%s/cameras/%s/attempts?limit=20", testStore, testCam))
	if code != http.StatusOK {
		t.Fatalf("查询审计失败: %d", code)
	}
	seen := map[string]bool{}
	for _, it := range r6["items"].([]any) {
		a := it.(map[string]any)
		if a["outcome"] == "rejected" {
			seen[a["error_code"].(string)] = true
		}
	}
	for _, want := range []string{"SEQUENCE_GAP", "IDEMPOTENCY_CONFLICT", "CLOCK_DRIFT_FUTURE"} {
		if !seen[want] {
			t.Fatalf("审计中应存在被拒绝的 %s: %v", want, r6)
		}
	}

	// 7. 哈希链回放校验通过。
	code, r7 := get(fmt.Sprintf("/v1/stores/%s/cameras/%s/chain/verify", testStore, testCam))
	verify := r7["verify"].(map[string]any)
	if code != http.StatusOK || verify["valid"] != true || verify["checked"].(float64) != 3 {
		t.Fatalf("链校验应通过且检查 3 条: %d %v", code, r7)
	}

	// 8. 验收点四：模拟重启（新建存储/服务实例，同一数据库）后索引仍可查。
	srv.Close()
	srv = newServer()
	defer srv.Close()
	from := now.Add(-2 * time.Hour).Format(time.RFC3339)
	to := now.Add(time.Hour).Format(time.RFC3339)
	code, r8 := get(fmt.Sprintf("/v1/stores/%s/cameras/%s/events?from=%s&to=%s", testStore, testCam, from, to))
	if code != http.StatusOK || r8["count"].(float64) != 3 {
		t.Fatalf("重启后应仍能查到 3 条索引: %d %v", code, r8)
	}

	// 9. 异步通知确实写入了 Redis Streams（只有 3 条 accepted）。
	streamLen, err := rdb.XLen(ctx, testStream).Result()
	if err != nil {
		t.Fatalf("读取 Stream 长度: %v", err)
	}
	if streamLen != 3 {
		t.Fatalf("Stream 中应有 3 条通知（仅 accepted），实际 %d", streamLen)
	}
	entries, err := rdb.XRange(ctx, testStream, "-", "+").Result()
	if err != nil || len(entries) != 3 {
		t.Fatalf("XRANGE 应读到 3 条: %v %d", err, len(entries))
	}
	if entries[0].Values["event_id"] != "evt-0001" || entries[0].Values["chain_hash"] == nil {
		t.Fatalf("通知应携带 event_id 与 chain_hash: %+v", entries[0].Values)
	}

	// 10. 消费组消费者冒烟：能建组、读到并 ACK。
	consumer := notify.NewConsumer(rdb, testStream, "itest-group", "itest-1", nil)
	consumerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { consumer.Run(consumerCtx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := rdb.XPending(ctx, testStream, "itest-group").Result()
		if err == nil && pending.Count == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done
	pending, err := rdb.XPending(ctx, testStream, "itest-group").Result()
	if err != nil {
		t.Fatalf("读取 pending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("消费者应 ACK 全部消息，剩余 pending=%d", pending.Count)
	}
}
