package domain

import "time"

// Event 是门店上报的一条带时间戳的摄像头事件。
type Event struct {
	EventID    string    `json:"event_id"`     // 设备端生成的幂等键（UUID/ULID）
	StoreID    string    `json:"store_id"`     // 门店标识
	CameraID   string    `json:"camera_id"`    // 摄像头标识
	SequenceNo int64     `json:"sequence_no"`  // 设备端单调递增序号，用于检测片段缺失
	OccurredAt time.Time `json:"occurred_at"`  // 设备时钟记录的事件发生时间（RFC3339）
	ClipURI    string    `json:"clip_uri"`     // 片段对象存储地址/引用
	ClipSHA256 string    `json:"clip_sha256,omitempty"` // 片段文件摘要（可选，用于内容复核）
	DurationMs int64     `json:"duration_ms,omitempty"`  // 片段时长（毫秒）
	ReceivedAt time.Time `json:"received_at"`   // 服务端落库时间
	PrevHash   string    `json:"prev_hash"`     // 前一条记录摘要
	RecordHash string    `json:"record_hash"`   // 本条记录摘要（哈希链）
}

// AcceptResult 是一次上报的处理结果。
type AcceptResult struct {
	Status    AcceptStatus `json:"status"`     // accepted | duplicate
	Event     *Event       `json:"event"`      // 落库/已存在的记录
	AttemptNo int          `json:"attempt_no"` // 这是该载荷第几次尝试（含首次）
}

type AcceptStatus string

const (
	StatusAccepted  AcceptStatus = "accepted"
	StatusDuplicate AcceptStatus = "duplicate"
)

// FailedIngest 记录一次被拒绝的上报，供门店端断线重传后追踪状态。
type FailedIngest struct {
	EventID    string     `json:"event_id"`
	StoreID    string     `json:"store_id"`
	CameraID   string     `json:"camera_id"`
	SequenceNo int64      `json:"sequence_no"`
	Payload    string     `json:"payload"`
	Reason     ErrCode    `json:"reason"`
	Detail     string     `json:"detail"`
	Attempts   int        `json:"attempts"`
	Resolved   bool       `json:"resolved"`
	FirstSeen  time.Time  `json:"first_seen_at"`
	LastSeen   time.Time  `json:"last_seen_at"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
}
