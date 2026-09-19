package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"kitchen-evidence/internal/chain"
	"kitchen-evidence/internal/domain"
	"kitchen-evidence/internal/service"
)

// inMemRepo 在内存中复刻存储语义：幂等、序号连续、哈希链链接。
// 让 HTTP 层测试覆盖完整的验收行为，而不依赖真实数据库。
type inMemRepo struct {
	mu       sync.Mutex
	events   []*domain.StoredEvent
	attempts []domain.Attempt
	seq      int64
	head     string
}

func newInMemRepo() *inMemRepo { return &inMemRepo{head: chain.GenesisHash} }

func (r *inMemRepo) Ingest(_ context.Context, p domain.IngestParams) (*domain.IngestResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	req := p.Request
	for _, e := range r.events {
		if e.StoreID == req.StoreID && e.CameraID == req.CameraID && e.EventID == req.EventID {
			if e.PayloadHash == p.PayloadHash {
				return &domain.IngestResult{Status: domain.StatusDuplicate, Event: e}, nil
			}
			return nil, domain.ErrIdempotencyConflict(req.StoreID, req.CameraID, req.EventID)
		}
	}
	expected := r.seq + 1
	switch {
	case req.Seq < expected:
		return nil, domain.ErrSequenceReplayed(req.StoreID, req.CameraID, req.Seq, "evt-x")
	case req.Seq > expected:
		return nil, domain.ErrSequenceGap(req.StoreID, req.CameraID, expected, req.Seq)
	}
	ev := &domain.StoredEvent{
		ID: fmt.Sprintf("id-%d", req.Seq), StoreID: req.StoreID, CameraID: req.CameraID,
		EventID: req.EventID, Seq: req.Seq, EventType: req.EventType,
		OccurredAt: req.OccurredAt, ReceivedAt: time.Now().UTC(),
		ClipStart: req.Clip.StartTS, ClipEnd: req.Clip.EndTS, ClipURI: req.Clip.URI,
		MediaHash: p.MediaHash, PayloadHash: p.PayloadHash,
		PrevChainHash: r.head, ChainHash: chain.Link(r.head, p.PayloadHash),
	}
	r.events = append(r.events, ev)
	r.seq, r.head = req.Seq, ev.ChainHash
	return &domain.IngestResult{Status: domain.StatusAccepted, Event: ev}, nil
}

func (r *inMemRepo) GetByEventID(_ context.Context, storeID, cameraID, eventID string) (*domain.StoredEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.StoreID == storeID && e.CameraID == cameraID && e.EventID == eventID {
			return e, nil
		}
	}
	return nil, nil
}

func (r *inMemRepo) List(_ context.Context, q domain.ListQuery) ([]domain.StoredEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.StoredEvent
	for _, e := range r.events {
		if e.StoreID == q.StoreID && e.CameraID == q.CameraID &&
			!e.OccurredAt.Before(q.From) && !e.OccurredAt.After(q.To) && e.Seq > q.AfterSeq {
			out = append(out, *e)
		}
		if len(out) >= q.Limit {
			break
		}
	}
	return out, nil
}

func (r *inMemRepo) ChainEvents(_ context.Context, storeID, cameraID string) ([]domain.StoredEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.StoredEvent
	for _, e := range r.events {
		if e.StoreID == storeID && e.CameraID == cameraID {
			out = append(out, *e)
		}
	}
	return out, nil
}

func (r *inMemRepo) ChainHead(context.Context, string, string) (int64, string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seq == 0 {
		return 0, "", false, nil
	}
	return r.seq, r.head, true, nil
}

func (r *inMemRepo) RecordAttempt(_ context.Context, a domain.Attempt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, a)
	return nil
}

func (r *inMemRepo) ListAttempts(_ context.Context, storeID, cameraID string, limit int) ([]domain.Attempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.Attempt
	for i := len(r.attempts) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, r.attempts[i])
	}
	return out, nil
}

type okPublisher struct{}

func (okPublisher) PublishEvent(context.Context, domain.StoredEvent) error { return nil }

func newTestRouter(repo *inMemRepo) *gin.Engine {
	gin.SetMode(gin.TestMode)
	svc := service.New(repo, okPublisher{}, service.Limits{
		ClockDriftTolerance: 5 * time.Minute,
		MaxBackfillAge:      7 * 24 * time.Hour,
	}, nil)
	return NewRouter(svc, nil, nil)
}

func eventBody(eventID string, seq int64, occurred time.Time, mediaHash string) string {
	return fmt.Sprintf(`{
		"event_id": %q, "store_id": "store-1", "camera_id": "cam-1",
		"seq": %d, "event_type": "clip.ready",
		"occurred_at": %q,
		"clip": {"start_ts": %q, "end_ts": %q, "uri": "s3://clips/%s.mp4", "media_hash": %q}
	}`, eventID, seq, occurred.UTC().Format(time.RFC3339),
		occurred.Add(-30*time.Second).UTC().Format(time.RFC3339),
		occurred.UTC().Format(time.RFC3339), eventID, mediaHash)
}

func doRequest(r *gin.Engine, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	var reader *bytes.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var parsed map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &parsed)
	return w, parsed
}

func mediaHash(ch string) string { return "sha256:" + strings.Repeat(ch, 64) }

// TestDuplicateIngestProducesSingleRecord 验收点：两次相同上报只产生一条记录。
func TestDuplicateIngestProducesSingleRecord(t *testing.T) {
	repo := newInMemRepo()
	r := newTestRouter(repo)
	body := eventBody("evt-1", 1, time.Now().Add(-time.Hour), mediaHash("a"))

	w1, resp1 := doRequest(r, http.MethodPost, "/v1/events", body)
	if w1.Code != http.StatusCreated || resp1["status"] != "accepted" {
		t.Fatalf("首次上报应 201 accepted: %d %v", w1.Code, resp1)
	}
	w2, resp2 := doRequest(r, http.MethodPost, "/v1/events", body)
	if w2.Code != http.StatusOK || resp2["status"] != "duplicate" {
		t.Fatalf("重复上报应 200 duplicate: %d %v", w2.Code, resp2)
	}
	id1 := resp1["event"].(map[string]any)["id"]
	id2 := resp2["event"].(map[string]any)["id"]
	if id1 != id2 {
		t.Fatalf("重复上报应返回同一条记录: %v vs %v", id1, id2)
	}
	if len(repo.events) != 1 {
		t.Fatalf("存储中应只有 1 条记录，实际 %d", len(repo.events))
	}
}

// TestSequenceGapAndRecovery 验收点：缺失片段返回可定位错误，补传后恢复。
func TestSequenceGapAndRecovery(t *testing.T) {
	repo := newInMemRepo()
	r := newTestRouter(repo)
	now := time.Now().Add(-time.Hour)

	doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-1", 1, now, mediaHash("a")))

	w, resp := doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-3", 3, now, mediaHash("c")))
	if w.Code != http.StatusConflict {
		t.Fatalf("跳号应 409: %d %v", w.Code, resp)
	}
	errObj := resp["error"].(map[string]any)
	if errObj["code"] != domain.CodeSequenceGap {
		t.Fatalf("错误码应为 SEQUENCE_GAP: %v", errObj)
	}
	details := errObj["details"].(map[string]any)
	if details["expected_seq"].(float64) != 2 || details["received_seq"].(float64) != 3 {
		t.Fatalf("错误应携带期望/实际序号: %v", details)
	}

	// 补传缺口后，重传 seq=3 应成功。
	if w, _ := doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-2", 2, now, mediaHash("b"))); w.Code != http.StatusCreated {
		t.Fatalf("补传 seq=2 应成功: %d", w.Code)
	}
	if w, _ := doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-3", 3, now, mediaHash("c"))); w.Code != http.StatusCreated {
		t.Fatalf("补传后重传 seq=3 应成功: %d", w.Code)
	}
}

// TestIdempotencyConflict 验收点：同一幂等键携带不同内容是失败重传，状态明确。
func TestIdempotencyConflict(t *testing.T) {
	repo := newInMemRepo()
	r := newTestRouter(repo)
	now := time.Now().Add(-time.Hour)

	doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-1", 1, now, mediaHash("a")))
	w, resp := doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-1", 1, now, mediaHash("b")))
	if w.Code != http.StatusConflict || resp["error"].(map[string]any)["code"] != domain.CodeIdempotencyConflict {
		t.Fatalf("内容被改动的重传应 409 IDEMPOTENCY_CONFLICT: %d %v", w.Code, resp)
	}
}

// TestClockDriftRejected 验收点：设备时钟漂移返回可定位错误而非静默接受。
func TestClockDriftRejected(t *testing.T) {
	repo := newInMemRepo()
	r := newTestRouter(repo)

	w, resp := doRequest(r, http.MethodPost, "/v1/events",
		eventBody("evt-future", 1, time.Now().Add(2*time.Hour), mediaHash("a")))
	if w.Code != http.StatusUnprocessableEntity ||
		resp["error"].(map[string]any)["code"] != domain.CodeClockDriftFuture {
		t.Fatalf("未来时间应 422 CLOCK_DRIFT_FUTURE: %d %v", w.Code, resp)
	}
	if resp["request_id"] == "" {
		t.Fatal("错误响应应携带 request_id 便于定位")
	}
}

// TestQueryAndVerify 验收点：按时间范围查询、单条查询、链校验、审计可见。
func TestQueryAndVerify(t *testing.T) {
	repo := newInMemRepo()
	r := newTestRouter(repo)
	now := time.Now().Add(-time.Hour)

	doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-1", 1, now, mediaHash("a")))
	doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-2", 2, now.Add(time.Minute), mediaHash("b")))
	doRequest(r, http.MethodPost, "/v1/events", eventBody("evt-bad", 5, now.Add(2*time.Minute), mediaHash("c"))) // 触发一次失败重传

	from := now.Add(-time.Hour).UTC().Format(time.RFC3339)
	to := now.Add(time.Hour).UTC().Format(time.RFC3339)
	w, resp := doRequest(r, http.MethodGet,
		fmt.Sprintf("/v1/stores/store-1/cameras/cam-1/events?from=%s&to=%s", from, to), "")
	if w.Code != http.StatusOK || resp["count"].(float64) != 2 {
		t.Fatalf("时间范围查询应返回 2 条: %d %v", w.Code, resp)
	}

	w, resp = doRequest(r, http.MethodGet, "/v1/stores/store-1/cameras/cam-1/events/evt-1", "")
	if w.Code != http.StatusOK || resp["event"].(map[string]any)["event_id"] != "evt-1" {
		t.Fatalf("按幂等键查询失败: %d %v", w.Code, resp)
	}

	w, resp = doRequest(r, http.MethodGet, "/v1/stores/store-1/cameras/cam-1/chain/verify", "")
	verify := resp["verify"].(map[string]any)
	if w.Code != http.StatusOK || verify["valid"] != true || verify["checked"].(float64) != 2 {
		t.Fatalf("链校验应通过且检查 2 条: %d %v", w.Code, resp)
	}

	w, resp = doRequest(r, http.MethodGet, "/v1/stores/store-1/cameras/cam-1/attempts", "")
	items := resp["items"].([]any)
	foundGap := false
	for _, it := range items {
		a := it.(map[string]any)
		if a["outcome"] == domain.OutcomeRejected && a["error_code"] == domain.CodeSequenceGap {
			foundGap = true
		}
	}
	if w.Code != http.StatusOK || !foundGap {
		t.Fatalf("审计中应能看到被拒绝的重传: %d %v", w.Code, resp)
	}
}

func TestHealthz(t *testing.T) {
	r := newTestRouter(newInMemRepo())
	w, resp := doRequest(r, http.MethodGet, "/healthz", "")
	if w.Code != http.StatusOK || resp["status"] != "ok" {
		t.Fatalf("健康检查应 ok: %d %v", w.Code, resp)
	}
}

func TestMalformedJSON(t *testing.T) {
	r := newTestRouter(newInMemRepo())
	w, resp := doRequest(r, http.MethodPost, "/v1/events", "{not json")
	if w.Code != http.StatusBadRequest || resp["error"].(map[string]any)["code"] != domain.CodeValidation {
		t.Fatalf("非法 JSON 应 400: %d %v", w.Code, resp)
	}
}
