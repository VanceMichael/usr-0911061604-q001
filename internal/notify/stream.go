package notify

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"kitchen-evidence/internal/domain"

	"github.com/redis/go-redis/v9"
)

// Notifier 把已入索引的片段事件异步投递到 Redis Stream。
// 投递失败不回滚入库（索引已是事实），由后台 worker 按退避重试。
type Notifier struct {
	client     *redis.Client
	streamName string
	baseDelay  time.Duration
	queue      chan *domain.Event
}

func New(client *redis.Client, streamName string, baseDelay time.Duration) *Notifier {
	if baseDelay <= 0 {
		baseDelay = 5 * time.Second
	}
	return &Notifier{
		client:     client,
		streamName: streamName,
		baseDelay:  baseDelay,
		queue:      make(chan *domain.Event, 1024),
	}
}

// Publish 非阻塞入队；队列满时退回后台 goroutine 阻塞投递，绝不拖慢 HTTP 响应。
func (n *Notifier) Publish(ev *domain.Event) {
	select {
	case n.queue <- ev:
	default:
		go func() { n.queue <- ev }()
	}
}

// Run 启动通知 worker，直到 ctx 取消才退出。
func (n *Notifier) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-n.queue:
			n.deliverWithRetry(ctx, ev)
		}
	}
}

func (n *Notifier) deliverWithRetry(ctx context.Context, ev *domain.Event) {
	payload, err := json.Marshal(ev)
	if err != nil {
		log.Printf("notify: 序列化失败 event=%s: %v", ev.EventID, err)
		return
	}
	attempt := 0
	for {
		attempt++
		err := n.client.XAdd(ctx, &redis.XAddArgs{
			Stream: n.streamName,
			MaxLen: 10000, // 近似裁剪，防止流无限增长
			Approx: true,
			Values: map[string]any{
				"event_id":    ev.EventID,
				"store_id":    ev.StoreID,
				"camera_id":   ev.CameraID,
				"sequence_no": ev.SequenceNo,
				"occurred_at": ev.OccurredAt.Format(time.RFC3339Nano),
				"payload":     payload,
			},
		}).Err()
		if err == nil {
			if attempt > 1 {
				log.Printf("notify: 第 %d 次尝试投递成功 event=%s", attempt, ev.EventID)
			}
			return
		}
		if ctx.Err() != nil {
			log.Printf("notify: 服务关闭，放弃投递 event=%s: %v", ev.EventID, err)
			return
		}
		// 指数退避，封顶 1 分钟
		delay := n.baseDelay * time.Duration(1<<min(attempt-1, 6))
		log.Printf("notify: 投递失败(event=%s, 第%d次): %v，%s 后重试", ev.EventID, attempt, err, delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
