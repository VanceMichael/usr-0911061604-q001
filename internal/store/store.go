package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"kitchen-evidence/internal/chain"
	"kitchen-evidence/internal/domain"
)

// 事件列清单，供各查询复用，保证扫描顺序一致。
const eventColumns = `id, store_id, camera_id, event_id, seq, event_type,
	occurred_at, received_at, clip_start, clip_end, clip_uri, media_hash,
	payload_hash, prev_chain_hash, chain_hash`

// Store 是 PostgreSQL 存储实现。写路径通过"链头行锁 + 唯一约束"保证：
// 同一摄像头的写入串行化，断线重传与并发重试都不会产生重复或错序记录。
type Store struct {
	pool *pgxpool.Pool
}

// New 创建存储实例。
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Ping 检查数据库连通性，供健康检查使用。
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Ingest 在单个事务内完成：锁定链头 → 幂等检查 → 序号连续性检查 →
// 计算链哈希 → 写入索引 → 推进链头。
// 幂等命中返回 StatusDuplicate；业务冲突返回 *domain.Error。
func (s *Store) Ingest(ctx context.Context, p domain.IngestParams) (*domain.IngestResult, error) {
	req := p.Request
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("开启事务: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 确保链头行存在，再对其加行锁，串行化本摄像头的所有写入。
	if _, err := tx.Exec(ctx,
		`INSERT INTO camera_chains (store_id, camera_id, last_seq, last_chain_hash)
		 VALUES ($1, $2, 0, $3) ON CONFLICT DO NOTHING`,
		req.StoreID, req.CameraID, chain.GenesisHash); err != nil {
		return nil, fmt.Errorf("初始化链头: %w", err)
	}
	var lastSeq int64
	var lastHash string
	if err := tx.QueryRow(ctx,
		`SELECT last_seq, last_chain_hash FROM camera_chains
		 WHERE store_id = $1 AND camera_id = $2 FOR UPDATE`,
		req.StoreID, req.CameraID).Scan(&lastSeq, &lastHash); err != nil {
		return nil, fmt.Errorf("锁定链头: %w", err)
	}

	// 幂等检查：同一 event_id 的重传直接返回原记录；内容被改动则明确冲突。
	existing, err := s.getByEventID(ctx, tx, req.StoreID, req.CameraID, req.EventID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.PayloadHash == p.PayloadHash {
			return &domain.IngestResult{Status: domain.StatusDuplicate, Event: existing}, nil
		}
		return nil, domain.ErrIdempotencyConflict(req.StoreID, req.CameraID, req.EventID)
	}

	// 序号连续性检查：缺口与序号占用都返回可定位错误，绝不静默接受。
	expected := lastSeq + 1
	switch {
	case req.Seq < expected:
		var occupant string
		_ = tx.QueryRow(ctx,
			`SELECT event_id FROM clip_events WHERE store_id = $1 AND camera_id = $2 AND seq = $3`,
			req.StoreID, req.CameraID, req.Seq).Scan(&occupant)
		return nil, domain.ErrSequenceReplayed(req.StoreID, req.CameraID, req.Seq, occupant)
	case req.Seq > expected:
		return nil, domain.ErrSequenceGap(req.StoreID, req.CameraID, expected, req.Seq)
	}

	ev := domain.StoredEvent{
		ID:            uuid.NewString(),
		StoreID:       req.StoreID,
		CameraID:      req.CameraID,
		EventID:       req.EventID,
		Seq:           req.Seq,
		EventType:     req.EventType,
		OccurredAt:    req.OccurredAt.UTC(),
		ReceivedAt:    time.Now().UTC(),
		ClipStart:     req.Clip.StartTS.UTC(),
		ClipEnd:       req.Clip.EndTS.UTC(),
		ClipURI:       req.Clip.URI,
		MediaHash:     p.MediaHash,
		PayloadHash:   p.PayloadHash,
		PrevChainHash: lastHash,
		ChainHash:     chain.Link(lastHash, p.PayloadHash),
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO clip_events (`+eventColumns+`)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		ev.ID, ev.StoreID, ev.CameraID, ev.EventID, ev.Seq, ev.EventType,
		ev.OccurredAt, ev.ReceivedAt, ev.ClipStart, ev.ClipEnd, ev.ClipURI,
		ev.MediaHash, ev.PayloadHash, ev.PrevChainHash, ev.ChainHash)
	if err != nil {
		// 并发重传在唯一约束上兜底：事务已中止，回滚后用新连接重查。
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return s.resolveUniqueConflict(ctx, pgErr.ConstraintName, p)
		}
		return nil, fmt.Errorf("写入索引: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE camera_chains SET last_seq = $3, last_chain_hash = $4, updated_at = now()
		 WHERE store_id = $1 AND camera_id = $2`,
		req.StoreID, req.CameraID, ev.Seq, ev.ChainHash); err != nil {
		return nil, fmt.Errorf("推进链头: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("提交事务: %w", err)
	}
	return &domain.IngestResult{Status: domain.StatusAccepted, Event: &ev}, nil
}

// resolveUniqueConflict 处理并发写入撞上唯一约束的情况（原事务已回滚）。
func (s *Store) resolveUniqueConflict(ctx context.Context, constraint string, p domain.IngestParams) (*domain.IngestResult, error) {
	switch constraint {
	case "uq_camera_event":
		existing, err := s.GetByEventID(ctx, p.Request.StoreID, p.Request.CameraID, p.Request.EventID)
		if err != nil {
			return nil, err
		}
		if existing.PayloadHash == p.PayloadHash {
			return &domain.IngestResult{Status: domain.StatusDuplicate, Event: existing}, nil
		}
		return nil, domain.ErrIdempotencyConflict(p.Request.StoreID, p.Request.CameraID, p.Request.EventID)
	case "uq_camera_seq":
		return nil, domain.ErrSequenceReplayed(p.Request.StoreID, p.Request.CameraID, p.Request.Seq, "")
	default:
		return nil, fmt.Errorf("唯一约束冲突 %s", constraint)
	}
}

// GetByEventID 按幂等键查询索引记录。
func (s *Store) GetByEventID(ctx context.Context, storeID, cameraID, eventID string) (*domain.StoredEvent, error) {
	return s.getByEventID(ctx, s.pool, storeID, cameraID, eventID)
}

func (s *Store) getByEventID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, storeID, cameraID, eventID string) (*domain.StoredEvent, error) {
	row := q.QueryRow(ctx,
		`SELECT `+eventColumns+` FROM clip_events
		 WHERE store_id = $1 AND camera_id = $2 AND event_id = $3`,
		storeID, cameraID, eventID)
	ev, err := scanEvent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("查询事件: %w", err)
	}
	return ev, nil
}

// List 按时间范围与序号游标查询索引，按 seq 升序返回。
func (s *Store) List(ctx context.Context, q domain.ListQuery) ([]domain.StoredEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+eventColumns+` FROM clip_events
		 WHERE store_id = $1 AND camera_id = $2
		   AND occurred_at >= $3 AND occurred_at <= $4 AND seq > $5
		 ORDER BY seq ASC LIMIT $6`,
		q.StoreID, q.CameraID, q.From, q.To, q.AfterSeq, q.Limit)
	if err != nil {
		return nil, fmt.Errorf("查询索引列表: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// ChainEvents 返回某摄像头全部索引（按 seq 升序），用于哈希链回放校验。
func (s *Store) ChainEvents(ctx context.Context, storeID, cameraID string) ([]domain.StoredEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+eventColumns+` FROM clip_events
		 WHERE store_id = $1 AND camera_id = $2 ORDER BY seq ASC`,
		storeID, cameraID)
	if err != nil {
		return nil, fmt.Errorf("查询链记录: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// ChainHead 读取链头；摄像头还没有任何记录时 found 为 false。
func (s *Store) ChainHead(ctx context.Context, storeID, cameraID string) (seq int64, hash string, found bool, err error) {
	err = s.pool.QueryRow(ctx,
		`SELECT last_seq, last_chain_hash FROM camera_chains WHERE store_id = $1 AND camera_id = $2`,
		storeID, cameraID).Scan(&seq, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, fmt.Errorf("查询链头: %w", err)
	}
	return seq, hash, true, nil
}

// RecordAttempt 记录一次上报尝试（成功/幂等/拒绝），供投诉复核审计。
func (s *Store) RecordAttempt(ctx context.Context, a domain.Attempt) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO ingest_attempts (store_id, camera_id, event_id, outcome, error_code, detail, payload_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		a.StoreID, a.CameraID, a.EventID, a.Outcome, a.ErrorCode, a.Detail, a.PayloadHash)
	if err != nil {
		return fmt.Errorf("记录上报尝试: %w", err)
	}
	return nil
}

// ListAttempts 返回某摄像头最近的上报尝试，最新的在前。
func (s *Store) ListAttempts(ctx context.Context, storeID, cameraID string, limit int) ([]domain.Attempt, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, store_id, camera_id, event_id, outcome, error_code, detail, payload_hash, created_at
		 FROM ingest_attempts
		 WHERE store_id = $1 AND camera_id = $2
		 ORDER BY id DESC LIMIT $3`,
		storeID, cameraID, limit)
	if err != nil {
		return nil, fmt.Errorf("查询上报尝试: %w", err)
	}
	defer rows.Close()

	var out []domain.Attempt
	for rows.Next() {
		var a domain.Attempt
		if err := rows.Scan(&a.ID, &a.StoreID, &a.CameraID, &a.EventID,
			&a.Outcome, &a.ErrorCode, &a.Detail, &a.PayloadHash, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("扫描上报尝试: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func scanEvent(row pgx.Row) (*domain.StoredEvent, error) {
	var ev domain.StoredEvent
	err := row.Scan(&ev.ID, &ev.StoreID, &ev.CameraID, &ev.EventID, &ev.Seq, &ev.EventType,
		&ev.OccurredAt, &ev.ReceivedAt, &ev.ClipStart, &ev.ClipEnd, &ev.ClipURI,
		&ev.MediaHash, &ev.PayloadHash, &ev.PrevChainHash, &ev.ChainHash)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func scanEvents(rows pgx.Rows) ([]domain.StoredEvent, error) {
	var out []domain.StoredEvent
	for rows.Next() {
		var ev domain.StoredEvent
		if err := rows.Scan(&ev.ID, &ev.StoreID, &ev.CameraID, &ev.EventID, &ev.Seq, &ev.EventType,
			&ev.OccurredAt, &ev.ReceivedAt, &ev.ClipStart, &ev.ClipEnd, &ev.ClipURI,
			&ev.MediaHash, &ev.PayloadHash, &ev.PrevChainHash, &ev.ChainHash); err != nil {
			return nil, fmt.Errorf("扫描索引记录: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}
