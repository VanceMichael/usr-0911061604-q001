package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"kitchen-evidence/internal/domain"
)

// fakeRepo 只记录调用，具体返回值由每个用例注入。
type fakeRepo struct {
	ingestFn    func(ctx context.Context, p domain.IngestParams) (*domain.IngestResult, error)
	ingestCalls int
	attempts    []domain.Attempt
}

func (f *fakeRepo) Ingest(ctx context.Context, p domain.IngestParams) (*domain.IngestResult, error) {
	f.ingestCalls++
	return f.ingestFn(ctx, p)
}
func (f *fakeRepo) GetByEventID(context.Context, string, string, string) (*domain.StoredEvent, error) {
	return nil, nil
}
func (f *fakeRepo) List(context.Context, domain.ListQuery) ([]domain.StoredEvent, error) {
	return nil, nil
}
func (f *fakeRepo) ChainEvents(context.Context, string, string) ([]domain.StoredEvent, error) {
	return nil, nil
}
func (f *fakeRepo) ChainHead(context.Context, string, string) (int64, string, bool, error) {
	return 0, "", false, nil
}
func (f *fakeRepo) RecordAttempt(_ context.Context, a domain.Attempt) error {
	f.attempts = append(f.attempts, a)
	return nil
}
func (f *fakeRepo) ListAttempts(context.Context, string, string, int) ([]domain.Attempt, error) {
	return f.attempts, nil
}

type fakePublisher struct {
	published []domain.StoredEvent
	err       error
}

func (f *fakePublisher) PublishEvent(_ context.Context, ev domain.StoredEvent) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, ev)
	return nil
}

var fixedNow = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

func newTestService(repo *fakeRepo, pub *fakePublisher) *Service {
	s := New(repo, pub, Limits{ClockDriftTolerance: 5 * time.Minute, MaxBackfillAge: 7 * 24 * time.Hour}, nil)
	s.now = func() time.Time { return fixedNow }
	return s
}

func validRequest() domain.IngestRequest {
	return domain.IngestRequest{
		EventID:    "evt-1",
		StoreID:    "store-1",
		CameraID:   "cam-1",
		Seq:        1,
		EventType:  "clip.ready",
		OccurredAt: fixedNow.Add(-time.Hour),
		Clip: domain.ClipInfo{
			StartTS:   fixedNow.Add(-time.Hour),
			EndTS:     fixedNow.Add(-time.Hour + 30*time.Second),
			URI:       "s3://clips/1.mp4",
			MediaHash: "sha256:" + strings.Repeat("ab", 32),
		},
	}
}

func acceptedResult(req domain.IngestRequest) *domain.IngestResult {
	return &domain.IngestResult{
		Status: domain.StatusAccepted,
		Event:  &domain.StoredEvent{EventID: req.EventID, StoreID: req.StoreID, CameraID: req.CameraID, Seq: req.Seq},
	}
}

func TestIngestAcceptedPublishesAndAudits(t *testing.T) {
	repo := &fakeRepo{ingestFn: func(_ context.Context, p domain.IngestParams) (*domain.IngestResult, error) {
		return acceptedResult(p.Request), nil
	}}
	pub := &fakePublisher{}
	svc := newTestService(repo, pub)

	view, err := svc.Ingest(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("不应报错: %v", err)
	}
	if view.Result.Status != domain.StatusAccepted {
		t.Fatalf("状态应为 accepted: %s", view.Result.Status)
	}
	if !view.Notified || len(pub.published) != 1 {
		t.Fatalf("新记录应发布一次通知: notified=%v published=%d", view.Notified, len(pub.published))
	}
	if len(repo.attempts) != 1 || repo.attempts[0].Outcome != domain.OutcomeAccepted {
		t.Fatalf("应记录 accepted 审计: %+v", repo.attempts)
	}
	if repo.attempts[0].PayloadHash == "" {
		t.Fatal("审计应携带 payload_hash")
	}
}

func TestIngestDuplicateSkipsPublish(t *testing.T) {
	repo := &fakeRepo{ingestFn: func(_ context.Context, p domain.IngestParams) (*domain.IngestResult, error) {
		return &domain.IngestResult{Status: domain.StatusDuplicate, Event: &domain.StoredEvent{EventID: p.Request.EventID}}, nil
	}}
	pub := &fakePublisher{}
	svc := newTestService(repo, pub)

	view, err := svc.Ingest(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("幂等命中不应报错: %v", err)
	}
	if view.Result.Status != domain.StatusDuplicate {
		t.Fatalf("状态应为 duplicate: %s", view.Result.Status)
	}
	if view.Notified || len(pub.published) != 0 {
		t.Fatal("重复上报不应重复通知")
	}
	if len(repo.attempts) != 1 || repo.attempts[0].Outcome != domain.OutcomeDuplicate {
		t.Fatalf("应记录 duplicate 审计: %+v", repo.attempts)
	}
}

func TestIngestSequenceGapIsLocatable(t *testing.T) {
	repo := &fakeRepo{ingestFn: func(_ context.Context, p domain.IngestParams) (*domain.IngestResult, error) {
		return nil, domain.ErrSequenceGap(p.Request.StoreID, p.Request.CameraID, 1, p.Request.Seq)
	}}
	svc := newTestService(repo, &fakePublisher{})

	req := validRequest()
	req.Seq = 3
	_, err := svc.Ingest(context.Background(), req)
	var derr *domain.Error
	if !errors.As(err, &derr) || derr.Code != domain.CodeSequenceGap {
		t.Fatalf("应返回 SEQUENCE_GAP: %v", err)
	}
	if derr.Details["expected_seq"].(int64) != 1 || derr.Details["received_seq"].(int64) != 3 {
		t.Fatalf("错误应携带定位细节: %+v", derr.Details)
	}
	if len(repo.attempts) != 1 || repo.attempts[0].Outcome != domain.OutcomeRejected ||
		repo.attempts[0].ErrorCode != domain.CodeSequenceGap {
		t.Fatalf("拒绝应留审计: %+v", repo.attempts)
	}
}

func TestIngestUnknownErrorBecomesInternal(t *testing.T) {
	repo := &fakeRepo{ingestFn: func(context.Context, domain.IngestParams) (*domain.IngestResult, error) {
		return nil, errors.New("connection reset")
	}}
	svc := newTestService(repo, &fakePublisher{})

	_, err := svc.Ingest(context.Background(), validRequest())
	var derr *domain.Error
	if !errors.As(err, &derr) || derr.Code != domain.CodeInternal {
		t.Fatalf("未知错误应映射为 INTERNAL: %v", err)
	}
}

func TestIngestRejectsFutureClock(t *testing.T) {
	repo := &fakeRepo{}
	svc := newTestService(repo, &fakePublisher{})

	req := validRequest()
	req.OccurredAt = fixedNow.Add(2 * time.Hour) // 设备时钟明显偏快
	_, err := svc.Ingest(context.Background(), req)
	var derr *domain.Error
	if !errors.As(err, &derr) || derr.Code != domain.CodeClockDriftFuture {
		t.Fatalf("应返回 CLOCK_DRIFT_FUTURE: %v", err)
	}
	if repo.ingestCalls != 0 {
		t.Fatal("时钟漂移的事件不应写入存储")
	}
	if len(repo.attempts) != 1 || repo.attempts[0].Outcome != domain.OutcomeRejected {
		t.Fatalf("漂移拒绝应留审计: %+v", repo.attempts)
	}
}

func TestIngestRejectsTooOld(t *testing.T) {
	repo := &fakeRepo{}
	svc := newTestService(repo, &fakePublisher{})

	req := validRequest()
	req.OccurredAt = fixedNow.Add(-30 * 24 * time.Hour) // 超出 7 天回传窗口
	_, err := svc.Ingest(context.Background(), req)
	var derr *domain.Error
	if !errors.As(err, &derr) || derr.Code != domain.CodeEventTooOld {
		t.Fatalf("应返回 EVENT_TOO_OLD: %v", err)
	}
	if repo.ingestCalls != 0 {
		t.Fatal("过旧事件不应写入存储")
	}
}

func TestIngestValidationFailures(t *testing.T) {
	cases := map[string]func(*domain.IngestRequest){
		"缺 event_id":    func(r *domain.IngestRequest) { r.EventID = "" },
		"seq 为 0":       func(r *domain.IngestRequest) { r.Seq = 0 },
		"片段起止倒置":        func(r *domain.IngestRequest) { r.Clip.EndTS = r.Clip.StartTS },
		"媒体摘要非 hex":     func(r *domain.IngestRequest) { r.Clip.MediaHash = "sha256:zz" + strings.Repeat("0", 62) },
		"媒体摘要长度不足":      func(r *domain.IngestRequest) { r.Clip.MediaHash = "abcd" },
		"缺 occurred_at": func(r *domain.IngestRequest) { r.OccurredAt = time.Time{} },
		"缺 clip.uri":    func(r *domain.IngestRequest) { r.Clip.URI = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			repo := &fakeRepo{}
			svc := newTestService(repo, &fakePublisher{})
			req := validRequest()
			mutate(&req)
			_, err := svc.Ingest(context.Background(), req)
			var derr *domain.Error
			if !errors.As(err, &derr) || derr.Code != domain.CodeValidation {
				t.Fatalf("应返回 VALIDATION_FAILED: %v", err)
			}
			if repo.ingestCalls != 0 {
				t.Fatal("校验失败不应写入存储")
			}
		})
	}
}

func TestMediaHashNormalization(t *testing.T) {
	repo := &fakeRepo{ingestFn: func(_ context.Context, p domain.IngestParams) (*domain.IngestResult, error) {
		if p.MediaHash != strings.Repeat("ab", 32) {
			t.Fatalf("媒体摘要应归一化为小写裸 hex: %s", p.MediaHash)
		}
		return acceptedResult(p.Request), nil
	}}
	svc := newTestService(repo, &fakePublisher{})

	req := validRequest()
	req.Clip.MediaHash = "SHA256:" + strings.ToUpper(strings.Repeat("ab", 32))
	if _, err := svc.Ingest(context.Background(), req); err != nil {
		t.Fatalf("大写带前缀的摘要应被接受并归一化: %v", err)
	}
}
