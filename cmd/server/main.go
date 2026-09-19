// 后厨影像证据索引服务入口。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"kitchen-evidence/internal/config"
	"kitchen-evidence/internal/httpapi"
	"kitchen-evidence/internal/notify"
	"kitchen-evidence/internal/service"
	"kitchen-evidence/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg, err := config.Load()
	if err != nil {
		log.Error("配置加载失败", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := connectPostgres(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("PostgreSQL 连接失败", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := store.Migrate(ctx, pool); err != nil {
		log.Error("数据库迁移失败", "error", err)
		os.Exit(1)
	}
	log.Info("数据库迁移完成")

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer rdb.Close()
	// Redis 暂时不可用不阻断启动：客户端会后台重连，上报接口以 notified=false 明示。
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Warn("Redis 暂不可用，异步通知将在重连后恢复", "addr", cfg.RedisAddr, "error", err)
	}
	cancel()

	pgStore := store.New(pool)
	publisher := notify.NewStreamPublisher(rdb, cfg.RedisStream)
	svc := service.New(pgStore, publisher, service.Limits{
		ClockDriftTolerance: cfg.ClockDriftTolerance,
		MaxBackfillAge:      cfg.MaxBackfillAge,
	}, log)

	if cfg.EnableStreamConsumer {
		host, _ := os.Hostname()
		consumer := notify.NewConsumer(rdb, cfg.RedisStream, cfg.RedisConsumerGroup, host, log)
		go consumer.Run(ctx)
	}

	srv := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           httpapi.NewRouter(svc, pgStore, notify.Pinger{Client: rdb}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("HTTP 服务启动", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP 服务异常退出", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("收到退出信号，开始优雅停机")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("优雅停机失败", "error", err)
		os.Exit(1)
	}
	log.Info("服务已停止")
}

// connectPostgres 带重试地连接数据库，容忍编排环境下依赖尚未就绪。
func connectPostgres(ctx context.Context, url string, log *slog.Logger) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		log.Warn("等待 PostgreSQL 就绪", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, lastErr
}
