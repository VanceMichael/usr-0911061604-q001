// Package httpapi 提供 Gin HTTP 接口：事件上报、索引查询、链校验与审计查询。
package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"kitchen-evidence/internal/domain"
	"kitchen-evidence/internal/service"
)

// Pinger 抽象数据库/缓存的健康探针。
type Pinger interface {
	Ping(ctx context.Context) error
}

// NewRouter 组装路由与中间件。db、cache 用于 /healthz，可为 nil。
func NewRouter(svc *service.Service, db, cache Pinger) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), requestID(), gin.Logger())

	h := &handler{svc: svc, db: db, cache: cache}

	r.GET("/healthz", h.healthz)

	v1 := r.Group("/v1")
	v1.POST("/events", h.postEvent)

	cam := v1.Group("/stores/:store_id/cameras/:camera_id")
	cam.GET("/events", h.listEvents)
	cam.GET("/events/:event_id", h.getEvent)
	cam.GET("/chain/verify", h.verifyChain)
	cam.GET("/attempts", h.listAttempts)

	return r
}

type handler struct {
	svc   *service.Service
	db    Pinger
	cache Pinger
}

func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	}
}

func rid(c *gin.Context) string {
	if v, ok := c.Get("request_id"); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// respondError 把业务错误映射为稳定的错误响应；未知错误统一 500。
func respondError(c *gin.Context, err error) {
	var derr *domain.Error
	if errors.As(err, &derr) {
		c.JSON(derr.HTTPStatus, gin.H{"error": derr, "request_id": rid(c)})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": domain.ErrInternal(), "request_id": rid(c)})
}

// healthz 报告进程与依赖状态，任一依赖不可用即 503。
func (h *handler) healthz(c *gin.Context) {
	status := gin.H{"status": "ok"}
	code := http.StatusOK
	check := func(name string, p Pinger) {
		if p == nil {
			status[name] = "disabled"
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if err := p.Ping(ctx); err != nil {
			status[name] = "down"
			status["status"] = "degraded"
			code = http.StatusServiceUnavailable
			return
		}
		status[name] = "up"
	}
	check("postgres", h.db)
	check("redis", h.cache)
	c.JSON(code, status)
}

// postEvent 接收门店事件上报。重复上报返回 200 + duplicate，新记录返回 201。
func (h *handler) postEvent(c *gin.Context) {
	var req domain.IngestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, domain.ErrValidation("请求体不是合法的 JSON 事件: "+err.Error()))
		return
	}
	view, err := h.svc.Ingest(c.Request.Context(), req)
	if err != nil {
		respondError(c, err)
		return
	}
	code := http.StatusCreated
	if view.Result.Status == domain.StatusDuplicate {
		code = http.StatusOK
	}
	c.JSON(code, gin.H{
		"status":     view.Result.Status,
		"event":      view.Result.Event,
		"notified":   view.Notified,
		"request_id": rid(c),
	})
}

// listEvents 按时间范围查询索引：?from=&to=&after_seq=&limit=
func (h *handler) listEvents(c *gin.Context) {
	q := domain.ListQuery{
		StoreID:  c.Param("store_id"),
		CameraID: c.Param("camera_id"),
		From:     time.Unix(0, 0).UTC(),
		To:       time.Now().UTC().Add(time.Hour),
		Limit:    100,
	}
	if v := c.Query("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			respondError(c, domain.ErrValidation("from 必须是 RFC3339 时间"))
			return
		}
		q.From = t
	}
	if v := c.Query("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			respondError(c, domain.ErrValidation("to 必须是 RFC3339 时间"))
			return
		}
		q.To = t
	}
	if v := c.Query("after_seq"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			respondError(c, domain.ErrValidation("after_seq 必须是非负整数"))
			return
		}
		q.AfterSeq = n
	}
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			respondError(c, domain.ErrValidation("limit 必须在 1..500 之间"))
			return
		}
		q.Limit = n
	}

	events, err := h.svc.ListEvents(c.Request.Context(), q)
	if err != nil {
		respondError(c, err)
		return
	}
	if events == nil {
		events = []domain.StoredEvent{}
	}
	var nextCursor *int64
	if len(events) == q.Limit {
		last := events[len(events)-1].Seq
		nextCursor = &last
	}
	c.JSON(http.StatusOK, gin.H{
		"items":       events,
		"count":       len(events),
		"next_cursor": nextCursor,
		"request_id":  rid(c),
	})
}

func (h *handler) getEvent(c *gin.Context) {
	ev, err := h.svc.GetEvent(c.Request.Context(), c.Param("store_id"), c.Param("camera_id"), c.Param("event_id"))
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"event": ev, "request_id": rid(c)})
}

// verifyChain 回放哈希链，供投诉复核时证明索引未被篡改。
func (h *handler) verifyChain(c *gin.Context) {
	res, err := h.svc.VerifyChain(c.Request.Context(), c.Param("store_id"), c.Param("camera_id"))
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"store_id": c.Param("store_id"), "camera_id": c.Param("camera_id"),
		"verify": res, "request_id": rid(c)})
}

// listAttempts 返回最近的上报尝试（含失败重传的明确状态）。
func (h *handler) listAttempts(c *gin.Context) {
	limit := 50
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			respondError(c, domain.ErrValidation("limit 必须在 1..200 之间"))
			return
		}
		limit = n
	}
	attempts, err := h.svc.ListAttempts(c.Request.Context(), c.Param("store_id"), c.Param("camera_id"), limit)
	if err != nil {
		respondError(c, err)
		return
	}
	if attempts == nil {
		attempts = []domain.Attempt{}
	}
	c.JSON(http.StatusOK, gin.H{"items": attempts, "count": len(attempts), "request_id": rid(c)})
}
