package domain

import "fmt"

// ErrCode 是机器可读的错误码，便于门店端按码决定是否重传。
type ErrCode string

const (
	ErrValidation        ErrCode = "VALIDATION_ERROR"      // 请求字段不合法
	ErrClockSkew         ErrCode = "CLOCK_SKEW_DETECTED"   // 设备时钟相对服务器漂移超阈值
	ErrEventOutOfOrder   ErrCode = "EVENT_OUT_OF_ORDER"    // 事件时间早于该摄像头上一条已接受事件
	ErrSequenceGap       ErrCode = "SEQUENCE_GAP"          // 序号不连续，存在片段缺失
	ErrSequenceConflict  ErrCode = "SEQUENCE_CONFLICT"     // 同一序号被不同事件占用
	ErrDuplicateMismatch ErrCode = "DUPLICATE_ID_MISMATCH" // event_id 重复但载荷与首次不一致
	ErrInternal          ErrCode = "INTERNAL_ERROR"        // 服务端内部错误
)

// Error 携带可定位信息：错误码 + 门店/摄像头/序号 + 供排查的细节。
type Error struct {
	Code    ErrCode `json:"code"`
	Message string  `json:"message"`
	// Locator 用于定位问题数据来源
	StoreID    string `json:"store_id,omitempty"`
	CameraID   string `json:"camera_id,omitempty"`
	SequenceNo int64  `json:"sequence_no,omitempty"`
	EventID    string `json:"event_id,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s (store=%s camera=%s seq=%d event=%s)",
		e.Code, e.Message, e.StoreID, e.CameraID, e.SequenceNo, e.EventID)
}

func NewError(code ErrCode, msg, storeID, cameraID string, seq int64, eventID string) *Error {
	return &Error{Code: code, Message: msg, StoreID: storeID, CameraID: cameraID, SequenceNo: seq, EventID: eventID}
}

// HTTPStatus 把领域错误码映射到 HTTP 状态。
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case ErrValidation, ErrClockSkew, ErrEventOutOfOrder, ErrSequenceGap, ErrSequenceConflict, ErrDuplicateMismatch:
		return 422 // 语义错误：服务端理解请求但拒绝静默接受
	default:
		return 500
	}
}
