package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"kitchen-evidence/internal/domain"
)

// QueryRange 按门店+摄像头+时间范围（UTC）返回片段索引，按发生时间升序。
func (r *Repo) QueryRange(ctx context.Context, storeID, cameraID string, from, to time.Time, limit int) ([]*domain.Event, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, eventSelect+`
		WHERE store_id = $1 AND camera_id = $2
		  AND occurred_at >= $3 AND occurred_at < $4
		ORDER BY occurred_at ASC, sequence_no ASC
		LIMIT $5`,
		storeID, cameraID, from.UTC(), to.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*domain.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// GetEvent 按 event_id 取单条索引。
func (r *Repo) GetEvent(ctx context.Context, eventID string) (*domain.Event, error) {
	ev, err := scanEvent(r.pool.QueryRow(ctx, eventSelect+` WHERE event_id = $1`, eventID))
	if err != nil {
		return nil, err
	}
	return ev, nil
}

// GetFailure 返回一次失败上报的重传追踪状态。
func (r *Repo) GetFailure(ctx context.Context, eventID string) (*domain.FailedIngest, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT event_id, store_id, camera_id, sequence_no, payload, reason, detail,
		       attempts, resolved, first_seen_at, last_seen_at, resolved_at
		FROM failed_ingests WHERE event_id = $1`, eventID)
	return scanFailure(row)
}

// ListFailures 列出某门店（可选全部）未解决/全部失败单，最近优先。
func (r *Repo) ListFailures(ctx context.Context, storeID string, includeResolved bool, limit int) ([]*domain.FailedIngest, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `
		SELECT event_id, store_id, camera_id, sequence_no, payload, reason, detail,
		       attempts, resolved, first_seen_at, last_seen_at, resolved_at
		FROM failed_ingests
		WHERE ($1 = '' OR store_id = $1) AND ($2 OR resolved = FALSE)
		ORDER BY last_seen_at DESC
		LIMIT $3`
	rows, err := r.pool.Query(ctx, q, storeID, includeResolved, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.FailedIngest
	for rows.Next() {
		f, err := scanFailure(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

type failureScanner interface {
	Scan(dest ...any) error
}

func scanFailure(row failureScanner) (*domain.FailedIngest, error) {
	var f domain.FailedIngest
	err := row.Scan(&f.EventID, &f.StoreID, &f.CameraID, &f.SequenceNo, &f.Payload,
		&f.Reason, &f.Detail, &f.Attempts, &f.Resolved, &f.FirstSeen, &f.LastSeen, &f.ResolvedAt)
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// ChainStatus 是一条摄像头哈希链的复核结果。
type ChainStatus struct {
	StoreID      string `json:"store_id"`
	CameraID     string `json:"camera_id"`
	Total        int    `json:"total_events"`
	Intact       bool   `json:"intact"`
	BrokenAt     int64  `json:"broken_at_sequence_no,omitempty"`
	ExpectedHash string `json:"expected_hash,omitempty"`
	StoredHash   string `json:"stored_hash,omitempty"`
}

// VerifyChain 重放某摄像头的哈希链，任一环摘要对不上即定位到具体序号。
// 复核口径必须与入库口径一致，因此这里反查事件载荷重新计算 payload_hash。
func (r *Repo) VerifyChain(ctx context.Context, storeID, cameraID string) (*ChainStatus, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT event_id, sequence_no, occurred_at, clip_uri, clip_sha256,
		       duration_ms, payload_hash, received_at, prev_hash, record_hash
		FROM events
		WHERE store_id = $1 AND camera_id = $2
		ORDER BY sequence_no ASC`, storeID, cameraID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	st := &ChainStatus{StoreID: storeID, CameraID: cameraID, Intact: true}
	prev := ""
	for rows.Next() {
		var (
			eventID, clipURI, clipSHA, pHash, storedPrev, storedHash string
			seq, durationMs                                          int64
			occurredAt, receivedAt                                   time.Time
		)
		if err := rows.Scan(&eventID, &seq, &occurredAt, &clipURI, &clipSHA,
			&durationMs, &pHash, &receivedAt, &storedPrev, &storedHash); err != nil {
			return nil, err
		}
		st.Total++
		// 注意：payload_hash 的原始输入含 store/camera，复核时用已知值重建。
		recomputedPayload := payloadHash(SubmitPayload{
			EventID: eventID, StoreID: storeID, CameraID: cameraID,
			SequenceNo: seq, OccurredAt: occurredAt, ClipURI: clipURI,
			ClipSHA256: clipSHA, DurationMs: durationMs,
		})
		if recomputedPayload != pHash {
			st.Intact = false
			st.BrokenAt = seq
			st.ExpectedHash = recomputedPayload
			st.StoredHash = pHash
			return st, nil
		}
		if storedPrev != prev {
			st.Intact = false
			st.BrokenAt = seq
			st.ExpectedHash = prev
			st.StoredHash = storedPrev
			return st, nil
		}
		recomputed := recomputeRecordHash(prev, pHash, eventID, seq, occurredAt, receivedAt)
		if recomputed != storedHash {
			st.Intact = false
			st.BrokenAt = seq
			st.ExpectedHash = recomputed
			st.StoredHash = storedHash
			return st, nil
		}
		prev = storedHash
	}
	return st, rows.Err()
}

// recomputeRecordHash 与入库时 recordHash 保持相同摘要口径。
func recomputeRecordHash(prevHash, pHash, eventID string, seq int64, occurredAt, receivedAt time.Time) string {
	material := strings.Join([]string{
		prevHash,
		pHash,
		eventID,
		fmt.Sprintf("%d", seq),
		occurredAt.UTC().Format(time.RFC3339Nano),
		receivedAt.UTC().Format(time.RFC3339Nano),
	}, "|")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}
