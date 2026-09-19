package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"kitchen-evidence/internal/domain"
	"kitchen-evidence/internal/postgres"
	"kitchen-evidence/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

// 需要真实 PostgreSQL：设置 TEST_DATABASE_URL 后运行（CI / 本地均可）。
// 未设置则跳过。测试数据使用唯一 store 前缀，不与其他数据冲突。
func setupRepo(t *testing.T) (*store.Repo, *pgxpool.Pool, string) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("跳过集成测试：未设置 TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping 失败: %v", err)
	}
	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	prefix := "itest-" + time.Now().Format("20060102T150405.000000000")
	return store.New(pool), pool, prefix
}

func ev(storeID, camID, id string, seq int64, at time.Time) store.SubmitPayload {
	return store.SubmitPayload{
		EventID:    id,
		StoreID:    storeID,
		CameraID:   camID,
		SequenceNo: seq,
		OccurredAt: at,
		ClipURI:    "s3://clips/" + id + ".mp4",
	}
}

func TestAccept_IdempotentAndGapAndChain(t *testing.T) {
	repo, pool, pre := setupRepo(t)
	defer pool.Close()
	ctx := context.Background()
	const skew = 30 * time.Second
	const pastWindow = 7 * 24 * time.Hour
	const gap = 1
	storeID, camID := pre+"-s", pre+"-c"
	now := time.Now().UTC().Add(-time.Minute)

	// seq=1 首次接受
	res, derr := repo.Accept(ctx, ev(storeID, camID, pre+"-e1", 1, now), skew, pastWindow, gap)
	if derr != nil {
		t.Fatalf("首次上报失败: %v", derr)
	}
	if res.Status != domain.StatusAccepted {
		t.Fatalf("期望 accepted，实际 %s", res.Status)
	}
	firstHash := res.Event.RecordHash

	// 完全相同的重传 -> duplicate，且无新记录
	res2, derr := repo.Accept(ctx, ev(storeID, camID, pre+"-e1", 1, now), skew, pastWindow, gap)
	if derr != nil {
		t.Fatalf("幂等重传不应报错: %v", derr)
	}
	if res2.Status != domain.StatusDuplicate {
		t.Fatalf("期望 duplicate，实际 %s", res2.Status)
	}
	if res2.Event.RecordHash != firstHash {
		t.Fatal("幂等返回必须是同一条既有索引（record_hash 应一致）")
	}

	// 跳号 seq=3 -> SEQUENCE_GAP，并产生失败单
	_, derr = repo.Accept(ctx, ev(storeID, camID, pre+"-e3", 3, now.Add(time.Second)), skew, pastWindow, gap)
	if derr == nil || derr.Code != domain.ErrSequenceGap {
		t.Fatalf("期望 SEQUENCE_GAP，实际 %v", derr)
	}
	f, err := repo.GetFailure(ctx, pre+"-e3")
	if err != nil {
		t.Fatalf("应能查到失败单: %v", err)
	}
	if f.Resolved || f.Attempts != 1 {
		t.Fatalf("失败单初始状态错误: %+v", f)
	}
	// 再次失败重传 -> attempts=2
	_, _ = repo.Accept(ctx, ev(storeID, camID, pre+"-e3", 3, now.Add(time.Second)), skew, pastWindow, gap)
	f, _ = repo.GetFailure(ctx, pre+"-e3")
	if f.Attempts != 2 || f.Resolved {
		t.Fatalf("二次失败后 attempts 应为 2 且未闭合，实际 %+v", f)
	}

	// 时钟超前 -> CLOCK_SKEW
	_, derr = repo.Accept(ctx, ev(storeID, camID, pre+"-future", 4, time.Now().Add(time.Hour)), skew, pastWindow, gap)
	if derr == nil || derr.Code != domain.ErrClockSkew {
		t.Fatalf("期望 CLOCK_SKEW_DETECTED，实际 %v", derr)
	}

	// 序号回退 -> SEQUENCE_CONFLICT
	_, derr = repo.Accept(ctx, ev(storeID, camID, pre+"-back", 1, now.Add(2*time.Second)), skew, pastWindow, gap)
	if derr == nil || derr.Code != domain.ErrSequenceConflict {
		t.Fatalf("期望 SEQUENCE_CONFLICT，实际 %v", derr)
	}

	// 乱序（事件时间早于上一条）-> EVENT_OUT_OF_ORDER
	_, derr = repo.Accept(ctx, ev(storeID, camID, pre+"-ooo", 2, now.Add(-time.Hour)), skew, pastWindow, gap)
	if derr == nil || derr.Code != domain.ErrEventOutOfOrder {
		t.Fatalf("期望 EVENT_OUT_OF_ORDER，实际 %v", derr)
	}

	// 补齐 seq=2 后，用同一 event_id 重传 seq=3 -> accepted 且失败单闭合
	_, derr = repo.Accept(ctx, ev(storeID, camID, pre+"-e2", 2, now.Add(500*time.Millisecond)), skew, pastWindow, gap)
	if derr != nil {
		t.Fatalf("seq=2 应被接受: %v", derr)
	}
	res3, derr := repo.Accept(ctx, ev(storeID, camID, pre+"-e3", 3, now.Add(time.Second)), skew, pastWindow, gap)
	if derr != nil {
		t.Fatalf("补齐后重传应成功: %v", derr)
	}
	if res3.Status != domain.StatusAccepted {
		t.Fatalf("期望 accepted，实际 %s", res3.Status)
	}
	if res3.AttemptNo != 3 {
		t.Fatalf("含 2 次失败后成功 attempt 应为 3，实际 %d", res3.AttemptNo)
	}
	f, _ = repo.GetFailure(ctx, pre+"-e3")
	if !f.Resolved {
		t.Fatal("成功重传后失败单必须闭合")
	}

	// 时间范围查询
	events, err := repo.QueryRange(ctx, storeID, camID, now.Add(-time.Minute), now.Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("应有 3 条索引，实际 %d", len(events))
	}

	// 哈希链复核完整
	st, err := repo.VerifyChain(ctx, storeID, camID)
	if err != nil {
		t.Fatalf("链校验失败: %v", err)
	}
	if !st.Intact || st.Total != 3 {
		t.Fatalf("哈希链应完整且有 3 环，实际 %+v", st)
	}

	// 篡改库里的片段地址 -> 链校验必须定位到具体序号
	if _, err := pool.Exec(ctx,
		`UPDATE events SET clip_uri = 's3://clips/TAMPERED.mp4' WHERE event_id = $1`, pre+"-e2"); err != nil {
		t.Fatalf("模拟篡改失败: %v", err)
	}
	st, err = repo.VerifyChain(ctx, storeID, camID)
	if err != nil {
		t.Fatalf("链校验查询失败: %v", err)
	}
	if st.Intact || st.BrokenAt != 2 {
		t.Fatalf("篡改 seq=2 后应在序号 2 处断链，实际 %+v", st)
	}
}

func TestAccept_DuplicateIDMismatch(t *testing.T) {
	repo, pool, pre := setupRepo(t)
	defer pool.Close()
	ctx := context.Background()
	storeID, camID := pre+"-s", pre+"-c"
	now := time.Now().UTC().Add(-time.Minute)

	p1 := ev(storeID, camID, pre+"-dup", 1, now)
	if _, derr := repo.Accept(ctx, p1, 30*time.Second, 7*24*time.Hour, 1); derr != nil {
		t.Fatalf("首次上报失败: %v", derr)
	}
	p2 := p1
	p2.ClipURI = "s3://clips/TAMPERED.mp4" // 同 event_id 不同载荷
	_, derr := repo.Accept(ctx, p2, 30*time.Second, 7*24*time.Hour, 1)
	if derr == nil || derr.Code != domain.ErrDuplicateMismatch {
		t.Fatalf("同 ID 不同载荷必须报 DUPLICATE_ID_MISMATCH，实际 %v", derr)
	}
}
