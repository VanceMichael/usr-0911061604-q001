// Package config 从环境变量加载服务配置。
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config 是服务的全部运行配置，均通过环境变量注入。
type Config struct {
	HTTPPort             string        // HTTP_PORT，默认 8080
	DatabaseURL          string        // DATABASE_URL，PostgreSQL 连接串
	RedisAddr            string        // REDIS_ADDR，host:port
	RedisPassword        string        // REDIS_PASSWORD
	RedisDB              int           // REDIS_DB
	RedisStream          string        // REDIS_STREAM，异步通知使用的 Stream 名
	RedisConsumerGroup   string        // REDIS_CONSUMER_GROUP
	EnableStreamConsumer bool          // ENABLE_STREAM_CONSUMER，是否在本进程内启动示例消费者
	ClockDriftTolerance  time.Duration // CLOCK_DRIFT_TOLERANCE，occurred_at 允许超前服务端的时间
	MaxBackfillAge       time.Duration // MAX_BACKFILL_AGE，断线重传允许回补的最大事件年龄
	ShutdownTimeout      time.Duration // SHUTDOWN_TIMEOUT，优雅退出超时
}

// Load 读取环境变量并填充默认值；非法的时长/布尔值会直接报错。
func Load() (Config, error) {
	cfg := Config{
		HTTPPort:             getStr("HTTP_PORT", "8080"),
		DatabaseURL:          getStr("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/kitchen?sslmode=disable"),
		RedisAddr:            getStr("REDIS_ADDR", "localhost:6379"),
		RedisPassword:        getStr("REDIS_PASSWORD", ""),
		RedisStream:          getStr("REDIS_STREAM", "clip.events"),
		RedisConsumerGroup:   getStr("REDIS_CONSUMER_GROUP", "clip-indexer"),
		EnableStreamConsumer: getBool("ENABLE_STREAM_CONSUMER", true),
		ClockDriftTolerance:  getDur("CLOCK_DRIFT_TOLERANCE", 5*time.Minute),
		MaxBackfillAge:       getDur("MAX_BACKFILL_AGE", 7*24*time.Hour),
		ShutdownTimeout:      getDur("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	db, err := getInt("REDIS_DB", 0)
	if err != nil {
		return Config{}, err
	}
	cfg.RedisDB = db
	if cfg.ClockDriftTolerance < 0 {
		return Config{}, fmt.Errorf("CLOCK_DRIFT_TOLERANCE 不能为负")
	}
	if cfg.MaxBackfillAge <= 0 {
		return Config{}, fmt.Errorf("MAX_BACKFILL_AGE 必须为正")
	}
	return cfg, nil
}

func getStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

func getBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func getInt(key string, def int) (int, error) {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("%s 必须是整数: %w", key, err)
		}
		return n, nil
	}
	return def, nil
}
