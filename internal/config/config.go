package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config 是服务的全部运行配置，一律来自环境变量。
type Config struct {
	HTTPAddr          string
	DatabaseURL       string
	RedisURL          string
	StreamName        string
	MaxClockSkew      time.Duration
	MaxPastWindow     time.Duration // 允许的事件最大迟到时间（断线重传窗口）；超过视为时钟后漂
	GapTolerance      int64         // 对同一 (store, camera) 相邻事件允许的最大序号间隔；1 表示必须连续
	RetryBaseInterval time.Duration
	PGPoolMax         int32
}

func Load() (Config, error) {
	maxSkew, err := secondsEnv("MAX_CLOCK_SKEW_SECONDS", 30)
	if err != nil {
		return Config{}, err
	}
	maxPast, err := secondsEnv("MAX_PAST_WINDOW_SECONDS", 604800)
	if err != nil {
		return Config{}, err
	}
	gap, err := int64Env("GAP_TOLERANCE", 1)
	if err != nil {
		return Config{}, err
	}
	retryBase, err := secondsEnv("RETRY_BASE_INTERVAL_SECONDS", 5)
	if err != nil {
		return Config{}, err
	}
	poolMax, err := int64Env("PG_POOL_MAX", 10)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		RedisURL:          os.Getenv("REDIS_URL"),
		StreamName:        getenv("STREAM_NAME", "kitchen.clip.indexed"),
		MaxClockSkew:      maxSkew,
		MaxPastWindow:     maxPast,
		GapTolerance:      gap,
		RetryBaseInterval: retryBase,
		PGPoolMax:         int32(poolMax),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("环境变量 DATABASE_URL 未设置")
	}
	if cfg.RedisURL == "" {
		return Config{}, fmt.Errorf("环境变量 REDIS_URL 未设置")
	}
	if cfg.MaxClockSkew <= 0 {
		return Config{}, fmt.Errorf("MAX_CLOCK_SKEW_SECONDS 必须为正数")
	}
	if cfg.MaxPastWindow <= 0 {
		return Config{}, fmt.Errorf("MAX_PAST_WINDOW_SECONDS 必须为正数")
	}
	if cfg.GapTolerance < 1 {
		return Config{}, fmt.Errorf("GAP_TOLERANCE 必须 >= 1")
	}
	if cfg.PGPoolMax < 1 {
		return Config{}, fmt.Errorf("PG_POOL_MAX 必须 >= 1")
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func secondsEnv(key string, def int64) (time.Duration, error) {
	n, err := int64Env(key, def)
	if err != nil {
		return 0, err
	}
	return time.Duration(n) * time.Second, nil
}

func int64Env(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("环境变量 %s=%q 不是合法整数", key, v)
	}
	return n, nil
}
