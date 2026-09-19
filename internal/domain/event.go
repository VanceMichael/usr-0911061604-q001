// Package domain 定义后厨影像证据索引的核心领域类型。
package domain

import "time"

// IngestRequest 是门店设备上报的带时间戳片段事件。
// EventID 由设备生成，作为幂等键：断线重传时同一 EventID 只会落库一次。
type IngestRequest struct {
	EventID    string    `json:"event_id"`
	StoreID    string    `json:"store_id"`
	CameraID   string    `json:"camera_id"`
	Seq        int64     `json:"seq"` // 摄像头维度从 1 开始连续递增的片段序号
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"` // 设备侧事件发生时间（用于时钟漂移校验）
	Clip       ClipInfo  `json:"clip"`
}

// ClipInfo 描述一个待公开的影像片段及其内容摘要。
type ClipInfo struct {
	StartTS   time.Time `json:"start_ts"`
	EndTS     time.Time `json:"end_ts"`
	URI       string    `json:"uri"`
	MediaHash string    `json:"media_hash"` // 片段内容摘要，形如 "sha256:<64 hex>"，也接受裸 64 hex
}

// StoredEvent 是落库后的索引记录，携带防篡改哈希链字段。
type StoredEvent struct {
	ID            string    `json:"id"`
	StoreID       string    `json:"store_id"`
	CameraID      string    `json:"camera_id"`
	EventID       string    `json:"event_id"`
	Seq           int64     `json:"seq"`
	EventType     string    `json:"event_type"`
	OccurredAt    time.Time `json:"occurred_at"`
	ReceivedAt    time.Time `json:"received_at"` // 服务端接收时间
	ClipStart     time.Time `json:"clip_start"`
	ClipEnd       time.Time `json:"clip_end"`
	ClipURI       string    `json:"clip_uri"`
	MediaHash     string    `json:"media_hash"`      // 归一化后的 64 位小写 hex
	PayloadHash   string    `json:"payload_hash"`    // 事件规范串摘要，用于幂等冲突判定
	PrevChainHash string    `json:"prev_chain_hash"` // 上一条记录的链哈希，创世为 64 个 0
	ChainHash     string    `json:"chain_hash"`      // 本记录的链哈希
}

// IngestStatus 是上报结果状态。
type IngestStatus string

const (
	StatusAccepted  IngestStatus = "accepted"  // 新记录已写入
	StatusDuplicate IngestStatus = "duplicate" // 幂等命中，返回已存在的记录
)

// IngestResult 是一次上报的处理结果。
type IngestResult struct {
	Status IngestStatus `json:"status"`
	Event  *StoredEvent `json:"event"`
}

// IngestParams 是存储层写入所需的全部参数。
type IngestParams struct {
	Request     IngestRequest
	MediaHash   string // 归一化后的媒体摘要
	PayloadHash string
}

// ListQuery 是按时间范围查询索引的条件。
type ListQuery struct {
	StoreID  string
	CameraID string
	From     time.Time
	To       time.Time
	AfterSeq int64 // 游标：只返回 seq 更大的记录
	Limit    int
}

// Attempt 是一次上报尝试的审计记录（含失败重传），用于投诉复核时定位问题。
type Attempt struct {
	ID          int64     `json:"id"`
	StoreID     string    `json:"store_id"`
	CameraID    string    `json:"camera_id"`
	EventID     string    `json:"event_id"`
	Outcome     string    `json:"outcome"` // accepted | duplicate | rejected
	ErrorCode   string    `json:"error_code,omitempty"`
	Detail      string    `json:"detail,omitempty"`
	PayloadHash string    `json:"payload_hash,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// 上报尝试的结果。
const (
	OutcomeAccepted  = "accepted"
	OutcomeDuplicate = "duplicate"
	OutcomeRejected  = "rejected"
)

// VerifyResult 是哈希链回放校验的结果。
type VerifyResult struct {
	Valid          bool   `json:"valid"`
	Checked        int    `json:"checked"`
	HeadSeq        int64  `json:"head_seq"`
	HeadChainHash  string `json:"head_chain_hash"`
	FirstBrokenSeq int64  `json:"first_broken_seq,omitempty"`
	Reason         string `json:"reason,omitempty"`
}
