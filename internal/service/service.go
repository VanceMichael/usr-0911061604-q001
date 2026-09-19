// Package service 编排上报校验、幂等写入、审计留痕与异步通知。
package service

import (
	"context"
	"encoding/hex"
	"log/slog"
	"strings"
	"time"

	"kitchen-evidence/internal/chain"
	"kitchen-evidence/internal/domain"
)

// Repository 是存储层抽象，便于用内存实现做单元测试。
type Repository interface {
	Ingest(ctx context.Context, p domain.IngestParams) (*domain.IngestResult, error)
	GetByEventID(ctx context.Context, storeID, cameraID, eventID string) (*domain.StoredEvent, error)
	List(ctx context.Context, q domain.ListQuery) ([]domain.StoredEvent, error)
	ChainEvents(ctx context.Context, storeID, cameraID string) ([]domain.StoredEvent, error)
	ChainHead(ctx context.Context, storeID, cameraID string) (seq int64, hash string, found bool, err error)
	RecordAttempt(ctx context.Context, a domain.Attempt) error
	ListAttempts(ctx context.Context, storeID, cameraID string, limit int) ([]domain.Attempt, error)
}

// Publisher 是异步通知抽象（生产实现为 Redis Streams）。
type Publisher interface {
	PublishEvent(ctx context.Context, ev domain.StoredEvent) error
}

// Limits 是时间相关的校验阈值。
type Limits struct {
	ClockDriftTolerance time.Duration
	MaxBackfillAge      time.Duration
}

// Service 是上报与查询的业务入口。
type Service struct {
	repo Repository
	pub  Publisher
	lim  Limits
	now  func() time.Time
	log  *slog.Logger
}

// New 创建业务服务；pub 可为 nil（禁用通知），log 可为 nil。
func New(repo Repository, pub Publisher, lim Limits, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{repo: repo, pub: pub, lim: lim, now: time.Now, log: log}
}

// IngestResultView 是上报接口的完整返回。
type IngestResultView struct {
	Result   *domain.IngestResult
	Notified bool // 异步通知是否已成功投递到 Redis Streams
}

// Ingest 处理一次事件上报：校验 → 幂等写入 → 审计 → 异步通知。
// 所有被拒绝的上报都会写入审计并返回可定位的 *domain.Error。
func (s *Service) Ingest(ctx context.Context, req domain.IngestRequest) (*IngestResultView, error) {
	mediaHash, verr := s.validate(req)
	if verr != nil {
		s.recordAttempt(ctx, req, domain.OutcomeRejected, verr, "")
		return nil, verr
	}

	canonical := chain.Canonical(req.StoreID, req.CameraID, req.EventID, req.Seq,
		req.EventType, req.OccurredAt, req.Clip.StartTS, req.Clip.EndTS, req.Clip.URI, mediaHash)
	payloadHash := chain.PayloadHash(canonical)

	res, err := s.repo.Ingest(ctx, domain.IngestParams{Request: req, MediaHash: mediaHash, PayloadHash: payloadHash})
	if err != nil {
		if derr, ok := err.(*domain.Error); ok {
			s.recordAttempt(ctx, req, domain.OutcomeRejected, derr, payloadHash)
			return nil, derr
		}
		s.log.Error("索引写入失败", "store_id", req.StoreID, "camera_id", req.CameraID,
			"event_id", req.EventID, "error", err)
		return nil, domain.ErrInternal()
	}

	outcome := domain.OutcomeAccepted
	if res.Status == domain.StatusDuplicate {
		outcome = domain.OutcomeDuplicate
	}
	s.recordAttempt(ctx, req, outcome, nil, payloadHash)

	notified := false
	if res.Status == domain.StatusAccepted && s.pub != nil {
		if err := s.pub.PublishEvent(ctx, *res.Event); err != nil {
			// 索引已落库，通知失败不阻断上报；门店网络抖动时由消费端补偿。
			s.log.Error("异步通知投递失败", "event_id", res.Event.EventID, "error", err)
		} else {
			notified = true
		}
	}
	return &IngestResultView{Result: res, Notified: notified}, nil
}

// GetEvent 按幂等键查询单条索引。
func (s *Service) GetEvent(ctx context.Context, storeID, cameraID, eventID string) (*domain.StoredEvent, error) {
	ev, err := s.repo.GetByEventID(ctx, storeID, cameraID, eventID)
	if err != nil {
		s.log.Error("查询事件失败", "error", err)
		return nil, domain.ErrInternal()
	}
	if ev == nil {
		return nil, domain.ErrNotFound("事件 " + eventID)
	}
	return ev, nil
}

// ListEvents 按时间范围查询索引。
func (s *Service) ListEvents(ctx context.Context, q domain.ListQuery) ([]domain.StoredEvent, error) {
	events, err := s.repo.List(ctx, q)
	if err != nil {
		s.log.Error("查询索引列表失败", "error", err)
		return nil, domain.ErrInternal()
	}
	return events, nil
}

// VerifyChain 回放某摄像头的哈希链，验证索引未被篡改。
func (s *Service) VerifyChain(ctx context.Context, storeID, cameraID string) (*domain.VerifyResult, error) {
	events, err := s.repo.ChainEvents(ctx, storeID, cameraID)
	if err != nil {
		s.log.Error("读取链记录失败", "error", err)
		return nil, domain.ErrInternal()
	}
	headSeq, headHash, found, err := s.repo.ChainHead(ctx, storeID, cameraID)
	if err != nil {
		s.log.Error("读取链头失败", "error", err)
		return nil, domain.ErrInternal()
	}
	if !found {
		headSeq, headHash = 0, chain.GenesisHash
	}
	res := chain.Verify(events, headSeq, headHash)
	return &res, nil
}

// ListAttempts 返回最近的上报尝试（含失败重传及原因）。
func (s *Service) ListAttempts(ctx context.Context, storeID, cameraID string, limit int) ([]domain.Attempt, error) {
	attempts, err := s.repo.ListAttempts(ctx, storeID, cameraID, limit)
	if err != nil {
		s.log.Error("查询上报尝试失败", "error", err)
		return nil, domain.ErrInternal()
	}
	return attempts, nil
}

// recordAttempt 尽力而为地写审计；审计失败只记日志，不影响主流程。
func (s *Service) recordAttempt(ctx context.Context, req domain.IngestRequest, outcome string, derr *domain.Error, payloadHash string) {
	a := domain.Attempt{
		StoreID: req.StoreID, CameraID: req.CameraID, EventID: req.EventID,
		Outcome: outcome, PayloadHash: payloadHash,
	}
	if derr != nil {
		a.ErrorCode = derr.Code
		a.Detail = derr.Message
	}
	if err := s.repo.RecordAttempt(ctx, a); err != nil {
		s.log.Error("审计写入失败", "event_id", req.EventID, "error", err)
	}
}

// validate 做全部入站校验，返回归一化后的媒体摘要。
// 时钟漂移与事件过旧在这里被拦截，返回可定位错误而不是静默接受。
func (s *Service) validate(req domain.IngestRequest) (string, *domain.Error) {
	switch {
	case req.StoreID == "":
		return "", domain.ErrValidation("store_id 不能为空")
	case req.CameraID == "":
		return "", domain.ErrValidation("camera_id 不能为空")
	case req.EventID == "":
		return "", domain.ErrValidation("event_id 不能为空")
	case len(req.EventID) > 128:
		return "", domain.ErrValidation("event_id 长度不能超过 128")
	case req.EventType == "":
		return "", domain.ErrValidation("event_type 不能为空")
	case req.Seq < 1:
		return "", domain.ErrValidation("seq 必须从 1 开始")
	case req.OccurredAt.IsZero():
		return "", domain.ErrValidation("occurred_at 不能为空")
	case req.Clip.StartTS.IsZero() || req.Clip.EndTS.IsZero():
		return "", domain.ErrValidation("clip.start_ts 与 clip.end_ts 不能为空")
	case !req.Clip.EndTS.After(req.Clip.StartTS):
		return "", domain.ErrValidation("clip.end_ts 必须晚于 clip.start_ts")
	case req.Clip.URI == "":
		return "", domain.ErrValidation("clip.uri 不能为空")
	}

	mediaHash, err := normalizeMediaHash(req.Clip.MediaHash)
	if err != nil {
		return "", domain.ErrValidation(err.Error())
	}

	now := s.now()
	if req.OccurredAt.After(now.Add(s.lim.ClockDriftTolerance)) {
		return "", domain.ErrClockDriftFuture(req.StoreID, req.CameraID, req.EventID,
			req.OccurredAt, now, s.lim.ClockDriftTolerance)
	}
	if req.OccurredAt.Before(now.Add(-s.lim.MaxBackfillAge)) {
		return "", domain.ErrEventTooOld(req.StoreID, req.CameraID, req.EventID,
			req.OccurredAt, now, s.lim.MaxBackfillAge)
	}
	return mediaHash, nil
}

// normalizeMediaHash 接受 "sha256:<hex>" 或裸 hex，统一为 64 位小写 hex。
func normalizeMediaHash(s string) (string, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.TrimPrefix(s, "sha256:")
	if len(s) != 64 {
		return "", domain.ErrValidation("clip.media_hash 必须是 64 位十六进制摘要（可带 sha256: 前缀）")
	}
	if _, err := hex.DecodeString(s); err != nil {
		return "", domain.ErrValidation("clip.media_hash 含非十六进制字符")
	}
	return s, nil
}
