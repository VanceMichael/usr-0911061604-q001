package domain

import (
	"fmt"
	"net/http"
	"time"
)

// 稳定的业务错误码，供门店网关与客服系统据此定位问题。
const (
	CodeValidation          = "VALIDATION_FAILED"
	CodeClockDriftFuture    = "CLOCK_DRIFT_FUTURE"
	CodeEventTooOld         = "EVENT_TOO_OLD"
	CodeSequenceGap         = "SEQUENCE_GAP"
	CodeSequenceReplayed    = "SEQUENCE_REPLAYED"
	CodeIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	CodeNotFound            = "NOT_FOUND"
	CodeInternal            = "INTERNAL"
)

// Error 是可定位的业务错误：除稳定错误码外，还携带门店、摄像头、序号等
// 定位细节，保证设备时钟漂移或片段缺失时不会被静默接受。
type Error struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Details    map[string]any `json:"details,omitempty"`
	HTTPStatus int            `json:"-"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newError(status int, code, msg string, details map[string]any) *Error {
	return &Error{Code: code, Message: msg, HTTPStatus: status, Details: details}
}

// ErrValidation 返回 400，表示请求体字段不合法。
func ErrValidation(msg string) *Error {
	return newError(http.StatusBadRequest, CodeValidation, msg, nil)
}

// ErrClockDriftFuture 返回 422，表示设备时钟明显快于服务端时钟。
func ErrClockDriftFuture(storeID, cameraID, eventID string, occurredAt, serverTime time.Time, tolerance time.Duration) *Error {
	return newError(http.StatusUnprocessableEntity, CodeClockDriftFuture,
		fmt.Sprintf("occurred_at 比服务端时间快 %s，超过容忍度 %s，请校准设备时钟", occurredAt.Sub(serverTime).Round(time.Second), tolerance),
		map[string]any{
			"store_id": storeID, "camera_id": cameraID, "event_id": eventID,
			"occurred_at": occurredAt.UTC(), "server_time": serverTime.UTC(),
			"tolerance": tolerance.String(),
		})
}

// ErrEventTooOld 返回 422，表示事件过旧、超出允许的回传窗口。
func ErrEventTooOld(storeID, cameraID, eventID string, occurredAt, serverTime time.Time, maxAge time.Duration) *Error {
	return newError(http.StatusUnprocessableEntity, CodeEventTooOld,
		fmt.Sprintf("occurred_at 距服务端时间 %s，超过最大回传窗口 %s", serverTime.Sub(occurredAt).Round(time.Second), maxAge),
		map[string]any{
			"store_id": storeID, "camera_id": cameraID, "event_id": eventID,
			"occurred_at": occurredAt.UTC(), "server_time": serverTime.UTC(),
			"max_backfill_age": maxAge.String(),
		})
}

// ErrSequenceGap 返回 409，表示中间缺失片段，需要先补传。
func ErrSequenceGap(storeID, cameraID string, expectedSeq, receivedSeq int64) *Error {
	return newError(http.StatusConflict, CodeSequenceGap,
		fmt.Sprintf("片段缺失：期望序号 %d，收到 %d，请先补传缺失的 %d 个片段", expectedSeq, receivedSeq, receivedSeq-expectedSeq),
		map[string]any{
			"store_id": storeID, "camera_id": cameraID,
			"expected_seq": expectedSeq, "received_seq": receivedSeq,
			"missing": receivedSeq - expectedSeq,
		})
}

// ErrSequenceReplayed 返回 409，表示该序号已被另一条事件占用（疑似设备序号重置或数据被改动）。
func ErrSequenceReplayed(storeID, cameraID string, seq int64, existingEventID string) *Error {
	return newError(http.StatusConflict, CodeSequenceReplayed,
		fmt.Sprintf("序号 %d 已被事件 %s 占用，新事件与其不一致", seq, existingEventID),
		map[string]any{
			"store_id": storeID, "camera_id": cameraID,
			"seq": seq, "existing_event_id": existingEventID,
		})
}

// ErrIdempotencyConflict 返回 409，表示同一幂等键携带了不同的内容（失败重传被改动）。
func ErrIdempotencyConflict(storeID, cameraID, eventID string) *Error {
	return newError(http.StatusConflict, CodeIdempotencyConflict,
		fmt.Sprintf("event_id %s 已存在但内容摘要不一致，拒绝覆盖", eventID),
		map[string]any{
			"store_id": storeID, "camera_id": cameraID, "event_id": eventID,
		})
}

// ErrNotFound 返回 404。
func ErrNotFound(what string) *Error {
	return newError(http.StatusNotFound, CodeNotFound, what+" 不存在", nil)
}

// ErrInternal 返回 500，内部细节只进日志，不外泄。
func ErrInternal() *Error {
	return newError(http.StatusInternalServerError, CodeInternal, "内部错误，请携带 request_id 联系平台排查", nil)
}
