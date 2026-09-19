package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kitchen-evidence/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repo 封装片段索引的全部持久化操作。
type Repo struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// SubmitPayload 是对门店端 JSON 的直接映射，time 解析在绑定层完成。
type SubmitPayload struct {
	EventID    string    `json:"event_id"`
	StoreID    string    `json:"store_id"`
	CameraID   string    `json:"camera_id"`
	SequenceNo int64     `json:"sequence_no"`
	OccurredAt time.Time `json:"occurred_at"`
	ClipURI    string    `json:"clip_uri"`
	ClipSHA256 string    `json:"clip_sha256,omitempty"`
	DurationMs int64     `json:"duration_ms,omitempty"`
}

func (p *SubmitPayload) validate() *domain.Error {
	trim := func(s string) string { return strings.TrimSpace(s) }
	p.EventID = trim(p.EventID)
	p.StoreID = trim(p.StoreID)
	p.CameraID = trim(p.CameraID)
	p.ClipURI = strings.TrimSpace(p.ClipURI)
	p.ClipSHA256 = strings.TrimSpace(p.ClipSHA256)

	loc := func(msg string) *domain.Error {
		return domain.NewError(domain.ErrValidation, msg, p.StoreID, p.CameraID, p.SequenceNo, p.EventID)
	}
	if p.EventID == "" {
		return loc("event_id 不能为空")
	}
	if p.StoreID == "" {
		return loc("store_id 不能为空")
	}
	if p.CameraID == "" {
		return loc("camera_id 不能为空")
	}
	if p.SequenceNo <= 0 {
		return loc("sequence_no 必须为正整数")
	}
	if p.OccurredAt.IsZero() {
		return loc("occurred_at 必须是 RFC3339 时间戳")
	}
	if p.ClipURI == "" {
		return loc("clip_uri 不能为空")
	}
	if p.ClipSHA256 != "" && len(p.ClipSHA256) != 64 {
		return loc("clip_sha256 若提供必须是 64 位十六进制 SHA256")
	}
	if p.DurationMs < 0 {
		return loc("duration_ms 不能为负数")
	}
	return nil
}

// canonicalJSON 稳定序列化载荷；同 event_id 重传时据此判断载荷是否一致。
func payloadHash(p SubmitPayload) string {
	raw, _ := json.Marshal([]any{
		p.EventID, p.StoreID, p.CameraID, p.SequenceNo,
		p.OccurredAt.UTC().Format(time.RFC3339Nano),
		p.ClipURI, strings.ToLower(p.ClipSHA256), p.DurationMs,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// recordHash 生成哈希链的一环：prev_hash 与本条规范化内容一起摘要。
func recordHash(prevHash string, p SubmitPayload, pHash string, receivedAt time.Time) string {
	material := strings.Join([]string{
		prevHash,
		pHash,
		p.EventID,
		fmt.Sprintf("%d", p.SequenceNo),
		p.OccurredAt.UTC().Format(time.RFC3339Nano),
		receivedAt.UTC().Format(time.RFC3339Nano),
	}, "|")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}

// Accept 在一个事务内完成：幂等判定 -> 时钟/序号校验 -> 哈希链落库 -> 失败重传状态维护。
// maxFutureSkew 限制设备时钟“快”的幅度；maxPastWindow 是断线重传允许的迟到窗口
// （旧片段重传天然发生在过去，不能用对称的 ±skew 误杀）。
func (r *Repo) Accept(ctx context.Context, raw SubmitPayload, maxFutureSkew, maxPastWindow time.Duration, gapTolerance int64) (*domain.AcceptResult, *domain.Error) {
	if verr := raw.validate(); verr != nil {
		r.recordFailure(context.WithoutCancel(ctx), raw, verr)
		return nil, verr
	}
	// 统一归一化：PG timestamptz 为微秒精度，哈希输入必须与落库值完全一致，
	// 否则纳秒级时间戳会导致事后复核哈希链时假性失配。
	raw.OccurredAt = raw.OccurredAt.UTC().Truncate(time.Microsecond)
	pHash := payloadHash(raw)
	locErr := func(code domain.ErrCode, msg string) *domain.Error {
		return domain.NewError(code, msg, raw.StoreID, raw.CameraID, raw.SequenceNo, raw.EventID)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, locErr(domain.ErrInternal, "无法开启事务: "+err.Error())
	}
	defer tx.Rollback(ctx)

	// 1) 幂等：event_id 已存在时只允许“完全相同”的重传。
	var existingPayloadHash string
	err = tx.QueryRow(ctx, `SELECT payload_hash FROM events WHERE event_id = $1`, raw.EventID).Scan(&existingPayloadHash)
	switch {
	case err == nil:
		if existingPayloadHash != pHash {
			de := locErr(domain.ErrDuplicateMismatch, "event_id 已存在但上报载荷与首次不一致，拒绝覆盖")
			r.recordFailureTx(ctx, tx, raw, de)
			if cErr := tx.Commit(ctx); cErr != nil {
				return nil, locErr(domain.ErrInternal, cErr.Error())
			}
			return nil, de
		}
		ev, gErr := r.getEventTx(ctx, tx, raw.EventID)
		if gErr != nil {
			return nil, locErr(domain.ErrInternal, gErr.Error())
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, locErr(domain.ErrInternal, err.Error())
		}
		return &domain.AcceptResult{Status: domain.StatusDuplicate, Event: ev, AttemptNo: r.attempts(ctx, raw.EventID)}, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, locErr(domain.ErrInternal, err.Error())
	}

	// 2) 锁定该摄像头的链头（首次上报时先插入占位行，再加锁），串行化同一链路的写入。
	if _, err := tx.Exec(ctx, `
		INSERT INTO cameras (store_id, camera_id) VALUES ($1, $2)
		ON CONFLICT (store_id, camera_id) DO NOTHING`, raw.StoreID, raw.CameraID); err != nil {
		return nil, locErr(domain.ErrInternal, err.Error())
	}
	var (
		lastSeq       int64
		lastEventTime *time.Time
		prevHash      string
	)
	if err := tx.QueryRow(ctx, `
		SELECT last_sequence_no, last_event_time, last_record_hash
		FROM cameras
		WHERE store_id = $1 AND camera_id = $2
		FOR UPDATE`, raw.StoreID, raw.CameraID).Scan(&lastSeq, &lastEventTime, &prevHash); err != nil {
		return nil, locErr(domain.ErrInternal, err.Error())
	}

	// 3) 设备时钟检查（非对称，适配断线重传场景）：
	//    - 事件时间落在未来且超过 maxFutureSkew：设备时钟快，无歧义地拒绝；
	//    - 事件时间早于 now-maxPastWindow：超出重传窗口，疑似时钟后漂，同样拒绝。
	serverNow := time.Now().UTC()
	if delta := raw.OccurredAt.UTC().Sub(serverNow); delta > maxFutureSkew {
		de := locErr(domain.ErrClockSkew, fmt.Sprintf(
			"设备时间超前服务器 %s，超过允许阈值 %s；请校准时钟后用同一 event_id 重传",
			delta.Round(time.Second), maxFutureSkew))
		r.recordFailureTx(ctx, tx, raw, de)
		if err := tx.Commit(ctx); err != nil {
			return nil, locErr(domain.ErrInternal, err.Error())
		}
		return nil, de
	} else if age := serverNow.Sub(raw.OccurredAt.UTC()); age > maxPastWindow {
		de := locErr(domain.ErrClockSkew, fmt.Sprintf(
			"事件时间比服务器早 %s，超出断线重传窗口 %s；请检查设备时钟后用同一 event_id 重传",
			age.Round(time.Hour), maxPastWindow))
		r.recordFailureTx(ctx, tx, raw, de)
		if err := tx.Commit(ctx); err != nil {
			return nil, locErr(domain.ErrInternal, err.Error())
		}
		return nil, de
	}

	// 4) 序号检查：回退/冲突/缺口都必须显式报错。
	switch {
	case raw.SequenceNo <= lastSeq:
		// 序号被占用但 event_id 不同：唯一索引也会兜底，这里给出可定位错误。
		de := locErr(domain.ErrSequenceConflict, fmt.Sprintf(
			"sequence_no=%d 不大于该摄像头已接受的最大序号 %d，序号不可回退或复用",
			raw.SequenceNo, lastSeq))
		r.recordFailureTx(ctx, tx, raw, de)
		if err := tx.Commit(ctx); err != nil {
			return nil, locErr(domain.ErrInternal, err.Error())
		}
		return nil, de
	case raw.SequenceNo-lastSeq > gapTolerance:
		var missing []int64
		for s := lastSeq + 1; s < raw.SequenceNo; s++ {
			missing = append(missing, s)
		}
		de := locErr(domain.ErrSequenceGap, fmt.Sprintf(
			"检测到片段缺失：期望序号 %d，实际收到 %d，缺失序号 %v；缺失片段补齐后可重传",
			lastSeq+1, raw.SequenceNo, missing))
		r.recordFailureTx(ctx, tx, raw, de)
		if err := tx.Commit(ctx); err != nil {
			return nil, locErr(domain.ErrInternal, err.Error())
		}
		return nil, de
	}

	// 5) 事件时间不得早于上一条（防设备时钟回拨/乱序静默入库）。
	if lastEventTime != nil && raw.OccurredAt.UTC().Before(lastEventTime.UTC()) {
		de := locErr(domain.ErrEventOutOfOrder, fmt.Sprintf(
			"occurred_at %s 早于该摄像头上一条已接受事件 %s",
			raw.OccurredAt.UTC().Format(time.RFC3339Nano),
			lastEventTime.UTC().Format(time.RFC3339Nano)))
		r.recordFailureTx(ctx, tx, raw, de)
		if err := tx.Commit(ctx); err != nil {
			return nil, locErr(domain.ErrInternal, err.Error())
		}
		return nil, de
	}

	// 6) 计算哈希链并落库。receivedAt 同样截断到微秒：
	//    PG timestamptz 只存微秒，哈希输入与列值必须逐字节一致，复核才可复现。
	receivedAt := serverNow.Truncate(time.Microsecond)
	rHash := recordHash(prevHash, raw, pHash, receivedAt)
	if _, err := tx.Exec(ctx, `
		INSERT INTO events
			(event_id, store_id, camera_id, sequence_no, occurred_at, clip_uri,
			 clip_sha256, duration_ms, payload_hash, prev_hash, record_hash, received_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		raw.EventID, raw.StoreID, raw.CameraID, raw.SequenceNo, raw.OccurredAt.UTC(),
		raw.ClipURI, strings.ToLower(raw.ClipSHA256), raw.DurationMs,
		pHash, prevHash, rHash, receivedAt,
	); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			de := locErr(domain.ErrSequenceConflict, "同一 (store_id, camera_id, sequence_no) 已被其他 event_id 占用")
			r.recordFailureTx(ctx, tx, raw, de)
			if err := tx.Commit(ctx); err != nil {
				return nil, locErr(domain.ErrInternal, err.Error())
			}
			return nil, de
		}
		return nil, locErr(domain.ErrInternal, err.Error())
	}

	if _, err := tx.Exec(ctx, `
		UPDATE cameras
		SET last_sequence_no = $3, last_event_time = $4, last_record_hash = $5, updated_at = now()
		WHERE store_id = $1 AND camera_id = $2`,
		raw.StoreID, raw.CameraID, raw.SequenceNo, raw.OccurredAt.UTC(), rHash); err != nil {
		return nil, locErr(domain.ErrInternal, err.Error())
	}

	// 7) 此前因缺口/漂移等被拒的同一 event_id，重传成功后闭合失败单。
	var priorFailures int
	err = tx.QueryRow(ctx, `
		UPDATE failed_ingests
		SET resolved = TRUE, resolved_at = now(), last_seen_at = now()
		WHERE event_id = $1 AND resolved = FALSE
		RETURNING attempts`, raw.EventID).Scan(&priorFailures)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, locErr(domain.ErrInternal, err.Error())
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, locErr(domain.ErrInternal, err.Error())
	}

	ev := toDomain(raw, receivedAt, prevHash, rHash)
	return &domain.AcceptResult{Status: domain.StatusAccepted, Event: ev, AttemptNo: priorFailures + 1}, nil
}

func (r *Repo) attempts(ctx context.Context, eventID string) int {
	var n int
	if err := r.pool.QueryRow(ctx,
		`SELECT attempts FROM failed_ingests WHERE event_id = $1`, eventID).Scan(&n); err != nil {
		return 1
	}
	return n + 1 // 首次成功 + 之前的失败尝试次数
}

func (r *Repo) recordFailureTx(ctx context.Context, tx pgx.Tx, p SubmitPayload, de *domain.Error) {
	raw, _ := json.Marshal(p)
	_, _ = tx.Exec(ctx, `
		INSERT INTO failed_ingests
			(event_id, store_id, camera_id, sequence_no, payload, reason, detail, attempts)
		VALUES ($1,$2,$3,$4,$5,$6,$7,1)
		ON CONFLICT (event_id) DO UPDATE SET
			attempts = failed_ingests.attempts + 1,
			reason = EXCLUDED.reason,
			detail = EXCLUDED.detail,
			payload = EXCLUDED.payload,
			last_seen_at = now(),
			resolved = FALSE,
			resolved_at = NULL`,
		p.EventID, p.StoreID, p.CameraID, p.SequenceNo, string(raw), string(de.Code), de.Message)
}

// recordFailure 处理连事务都未开启的纯校验错误。
func (r *Repo) recordFailure(ctx context.Context, p SubmitPayload, de *domain.Error) {
	if strings.TrimSpace(p.EventID) == "" {
		return // 没有幂等键无法追踪重传，错误本身仍会返回给调用方
	}
	raw, _ := json.Marshal(p)
	_, _ = r.pool.Exec(ctx, `
		INSERT INTO failed_ingests
			(event_id, store_id, camera_id, sequence_no, payload, reason, detail, attempts)
		VALUES ($1,$2,$3,$4,$5,$6,$7,1)
		ON CONFLICT (event_id) DO UPDATE SET
			attempts = failed_ingests.attempts + 1,
			reason = EXCLUDED.reason,
			detail = EXCLUDED.detail,
			payload = EXCLUDED.payload,
			last_seen_at = now(),
			resolved = FALSE,
			resolved_at = NULL`,
		p.EventID, p.StoreID, p.CameraID, p.SequenceNo, string(raw), string(de.Code), de.Message)
}

func (r *Repo) getEventTx(ctx context.Context, tx pgx.Tx, eventID string) (*domain.Event, error) {
	return scanEvent(tx.QueryRow(ctx, eventSelect+` WHERE event_id = $1`, eventID))
}

const eventSelect = `
	SELECT event_id, store_id, camera_id, sequence_no, occurred_at, clip_uri,
	       clip_sha256, duration_ms, received_at, prev_hash, record_hash
	FROM events`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEvent(row rowScanner) (*domain.Event, error) {
	var ev domain.Event
	err := row.Scan(&ev.EventID, &ev.StoreID, &ev.CameraID, &ev.SequenceNo, &ev.OccurredAt,
		&ev.ClipURI, &ev.ClipSHA256, &ev.DurationMs, &ev.ReceivedAt, &ev.PrevHash, &ev.RecordHash)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func toDomain(p SubmitPayload, receivedAt time.Time, prevHash, recordHash string) *domain.Event {
	return &domain.Event{
		EventID: p.EventID, StoreID: p.StoreID, CameraID: p.CameraID, SequenceNo: p.SequenceNo,
		OccurredAt: p.OccurredAt.UTC(), ClipURI: p.ClipURI, ClipSHA256: strings.ToLower(p.ClipSHA256),
		DurationMs: p.DurationMs, ReceivedAt: receivedAt, PrevHash: prevHash, RecordHash: recordHash,
	}
}
