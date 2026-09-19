package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"kitchen-evidence/internal/domain"
	"kitchen-evidence/internal/notify"
	"kitchen-evidence/internal/store"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

type Handler struct {
	repo       *store.Repo
	notifier   *notify.Notifier
	maxFutSkew time.Duration
	maxPast    time.Duration
	gap        int64
}

func New(repo *store.Repo, notifier *notify.Notifier, maxFutSkew, maxPast time.Duration, gap int64) *Handler {
	return &Handler{repo: repo, notifier: notifier, maxFutSkew: maxFutSkew, maxPast: maxPast, gap: gap}
}

func (h *Handler) Register(r gin.IRouter) {
	r.POST("/api/v1/events", h.submit)
	r.GET("/api/v1/stores/:store_id/cameras/:camera_id/events", h.queryRange)
	r.GET("/api/v1/events/:event_id", h.getEvent)
	r.GET("/api/v1/retries/:event_id", h.getRetry)
	r.GET("/api/v1/retries", h.listRetries)
	r.GET("/api/v1/stores/:store_id/cameras/:camera_id/verify", h.verifyChain)
}

// submitRequest 与 store.SubmitPayload 字段一致；时间在绑定阶段按 RFC3339 解析。
type submitRequest struct {
	EventID    string    `json:"event_id" binding:"required"`
	StoreID    string    `json:"store_id" binding:"required"`
	CameraID   string    `json:"camera_id" binding:"required"`
	SequenceNo int64     `json:"sequence_no" binding:"required"`
	OccurredAt time.Time `json:"occurred_at" binding:"required"`
	ClipURI    string    `json:"clip_uri" binding:"required"`
	ClipSHA256 string    `json:"clip_sha256,omitempty"`
	DurationMs int64     `json:"duration_ms,omitempty"`
}

func (h *Handler) submit(c *gin.Context) {
	var req submitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": domain.Error{Code: domain.ErrValidation, Message: "请求体解析失败: " + err.Error()},
		})
		return
	}

	res, derr := h.repo.Accept(c.Request.Context(), toPayload(req), h.maxFutSkew, h.maxPast, h.gap)
	if derr != nil {
		c.JSON(derr.HTTPStatus(), gin.H{"error": derr, "retry": gin.H{
			"status_url": "/api/v1/retries/" + derr.EventID,
			"hint":       "修正问题后可用同一 event_id 重传，或通过该地址查询重传状态",
		}})
		return
	}

	if res.Status == domain.StatusAccepted {
		// 索引落库后再异步通知，通知失败不影响响应。
		h.notifier.Publish(res.Event)
	}
	c.JSON(http.StatusOK, gin.H{
		"result":     res.Status,
		"attempt":    res.AttemptNo,
		"event":      res.Event,
		"idempotent": res.Status == domain.StatusDuplicate,
		"message": map[domain.AcceptStatus]string{
			domain.StatusAccepted:  "事件已写入片段索引",
			domain.StatusDuplicate: "重复事件，幂等返回既有索引，未产生新记录",
		}[res.Status],
	})
}

func (h *Handler) queryRange(c *gin.Context) {
	storeID := c.Param("store_id")
	cameraID := c.Param("camera_id")
	from, err := time.Parse(time.RFC3339, c.Query("from"))
	if err != nil {
		badRequest(c, "query 参数 from 必须是 RFC3339 时间，例如 2026-09-19T00:00:00Z")
		return
	}
	to, err := time.Parse(time.RFC3339, c.Query("to"))
	if err != nil {
		badRequest(c, "query 参数 to 必须是 RFC3339 时间，例如 2026-09-19T23:59:59Z")
		return
	}
	if !to.After(from) {
		badRequest(c, "to 必须晚于 from")
		return
	}
	events, err := h.repo.QueryRange(c.Request.Context(), storeID, cameraID, from, to, atoi(c.Query("limit")))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": domain.NewError(domain.ErrInternal, err.Error(), storeID, cameraID, 0, "")})
		return
	}
	c.JSON(http.StatusOK, gin.H{"store_id": storeID, "camera_id": cameraID, "from": from.UTC(), "to": to.UTC(), "count": len(events), "events": events})
}

func (h *Handler) getEvent(c *gin.Context) {
	ev, err := h.repo.GetEvent(c.Request.Context(), c.Param("event_id"))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": gin.H{"code": "NOT_FOUND", "message": "索引中不存在该 event_id"}})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"event": ev})
}

func (h *Handler) getRetry(c *gin.Context) {
	f, err := h.repo.GetFailure(c.Request.Context(), c.Param("event_id"))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": gin.H{"code": "NOT_FOUND", "message": "没有该 event_id 的失败重传记录"}})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"retry": f})
}

func (h *Handler) listRetries(c *gin.Context) {
	includeResolved := c.Query("resolved") == "true"
	failures, err := h.repo.ListFailures(c.Request.Context(), c.Query("store_id"), includeResolved, atoi(c.Query("limit")))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"count": len(failures), "retries": failures})
}

func (h *Handler) verifyChain(c *gin.Context) {
	st, err := h.repo.VerifyChain(c.Request.Context(), c.Param("store_id"), c.Param("camera_id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	status := http.StatusOK
	if !st.Intact {
		status = http.StatusConflict
	}
	c.JSON(status, gin.H{"chain": st})
}

func badRequest(c *gin.Context, msg string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": domain.Error{Code: domain.ErrValidation, Message: msg}})
}

func atoi(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

func toPayload(req submitRequest) store.SubmitPayload {
	return store.SubmitPayload{
		EventID:    req.EventID,
		StoreID:    req.StoreID,
		CameraID:   req.CameraID,
		SequenceNo: req.SequenceNo,
		OccurredAt: req.OccurredAt,
		ClipURI:    req.ClipURI,
		ClipSHA256: req.ClipSHA256,
		DurationMs: req.DurationMs,
	}
}
