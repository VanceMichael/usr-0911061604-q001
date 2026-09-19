// Package notify 基于 Redis Streams 实现片段索引的异步通知。
package notify

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"kitchen-evidence/internal/domain"
)

// streamMaxLen 是 Stream 的近似长度上限，防止无限增长。
const streamMaxLen = 100000

// Pinger 把 *redis.Client 适配成健康检查接口（Ping 返回 error）。
type Pinger struct {
	Client *redis.Client
}

// Ping 实现健康探针。
func (p Pinger) Ping(ctx context.Context) error { return p.Client.Ping(ctx).Err() }

// StreamPublisher 把已落库的索引事件发布到 Redis Stream。
type StreamPublisher struct {
	client *redis.Client
	stream string
}

// NewStreamPublisher 创建发布器。
func NewStreamPublisher(client *redis.Client, stream string) *StreamPublisher {
	return &StreamPublisher{client: client, stream: stream}
}

// PublishEvent 以 XADD 追加一条索引通知；字段全部为字符串，便于跨语言消费。
func (p *StreamPublisher) PublishEvent(ctx context.Context, ev domain.StoredEvent) error {
	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: map[string]any{
			"event_id":    ev.EventID,
			"store_id":    ev.StoreID,
			"camera_id":   ev.CameraID,
			"seq":         strconv.FormatInt(ev.Seq, 10),
			"event_type":  ev.EventType,
			"occurred_at": ev.OccurredAt.UTC().Format(time.RFC3339Nano),
			"clip_uri":    ev.ClipURI,
			"media_hash":  ev.MediaHash,
			"chain_hash":  ev.ChainHash,
		},
	}).Err()
}

// Consumer 是一个示例消费组消费者：把索引通知打印到日志并 ACK。
// 真实的下游（公开页渲染、投诉复核预取等）可以替换这里的处理逻辑。
type Consumer struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
	log      *slog.Logger
}

// NewConsumer 创建消费者；name 通常为实例主机名。
func NewConsumer(client *redis.Client, stream, group, name string, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{client: client, stream: stream, group: group, consumer: name, log: log}
}

// Run 循环消费直到 ctx 取消；启动时确保消费组存在。
func (c *Consumer) Run(ctx context.Context) {
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && !isBusyGroup(err) {
		c.log.Error("创建消费组失败", "stream", c.stream, "group", c.group, "error", err)
	}

	for {
		if ctx.Err() != nil {
			return
		}
		res, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumer,
			Streams:  []string{c.stream, ">"},
			Count:    16,
			Block:    2 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue
			}
			c.log.Error("读取通知流失败", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				c.log.Info("收到索引通知",
					"stream_id", msg.ID,
					"store_id", msg.Values["store_id"],
					"camera_id", msg.Values["camera_id"],
					"event_id", msg.Values["event_id"],
					"seq", msg.Values["seq"],
					"chain_hash", msg.Values["chain_hash"],
				)
				if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
					c.log.Error("ACK 失败", "stream_id", msg.ID, "error", err)
				}
			}
		}
	}
}

func isBusyGroup(err error) bool {
	return err != nil && len(err.Error()) >= 9 && err.Error()[:9] == "BUSYGROUP"
}
