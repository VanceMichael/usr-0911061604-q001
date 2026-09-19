package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"kitchen-evidence/internal/api"
	"kitchen-evidence/internal/config"
	"kitchen-evidence/internal/notify"
	"kitchen-evidence/internal/postgres"
	"kitchen-evidence/internal/store"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("配置错误: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL, cfg.PGPoolMax)
	if err != nil {
		log.Fatalf("连接 PostgreSQL 失败: %v", err)
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool); err != nil {
		log.Fatalf("数据库迁移失败: %v", err)
	}
	log.Print("PostgreSQL 就绪，迁移完成")

	rdb := redis.NewClient(mustParseRedisURL(cfg.RedisURL))
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("连接 Redis 失败: %v", err)
	}
	defer rdb.Close()
	log.Print("Redis 就绪")

	notifier := notify.New(rdb, cfg.StreamName, cfg.RetryBaseInterval)
	go notifier.Run(ctx)

	repo := store.New(pool)
	handler := api.New(repo, notifier, cfg.MaxClockSkew, cfg.MaxPastWindow, cfg.GapTolerance)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLog())
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	handler.Register(r)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: r}
	go func() {
		log.Printf("HTTP 服务监听 %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务异常: %v", err)
		}
	}()

	<-ctx.Done()
	log.Print("收到退出信号，正在优雅关闭…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP 关闭超时: %v", err)
	}
}

func requestLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Printf("%s %s -> %d (%s)", c.Request.Method, c.Request.URL.RequestURI(), c.Writer.Status(), time.Since(start).Round(time.Millisecond))
	}
}

func mustParseRedisURL(rawURL string) *redis.Options {
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		log.Fatalf("REDIS_URL 解析失败: %v", err)
	}
	return opts
}
