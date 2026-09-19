package store

import (
	"strings"
	"testing"
	"time"
)

func samplePayload() SubmitPayload {
	return SubmitPayload{
		EventID:    "evt-1",
		StoreID:    "store-1",
		CameraID:   "cam-1",
		SequenceNo: 1,
		OccurredAt: time.Date(2026, 9, 19, 6, 0, 0, 0, time.UTC),
		ClipURI:    "s3://clips/1.mp4",
		ClipSHA256: strings.Repeat("a", 64),
		DurationMs: 1500,
	}
}

func TestPayloadHash_DeterministicAndCaseInsensitiveSHA(t *testing.T) {
	a := samplePayload()
	b := samplePayload()
	b.ClipSHA256 = strings.ToUpper(a.ClipSHA256) // 大小写归一后应等同
	if payloadHash(a) != payloadHash(b) {
		t.Fatal("clip_sha256 的十六进制大小写不应影响 payload_hash")
	}
	c := samplePayload()
	c.ClipURI = "s3://clips/changed.mp4"
	if payloadHash(a) == payloadHash(c) {
		t.Fatal("载荷变化必须产生不同的 payload_hash，否则无法识别同 ID 不同载荷")
	}
}

func TestPayloadHash_TimezoneStable(t *testing.T) {
	utc := samplePayload()
	shanghai := samplePayload()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	shanghai.OccurredAt = utc.OccurredAt.In(loc) // 同一时刻，不同时区表示
	if payloadHash(utc) != payloadHash(shanghai) {
		t.Fatal("同一时刻的不同时区表示必须归一化，否则重传会被误判为载荷不一致")
	}
}

func TestPayloadHash_NanosecondTruncation(t *testing.T) {
	a := samplePayload()
	b := samplePayload()
	b.OccurredAt = a.OccurredAt.Add(500 * time.Nanosecond)
	a.OccurredAt = a.OccurredAt.Truncate(time.Microsecond)
	b.OccurredAt = b.OccurredAt.Truncate(time.Microsecond)
	if payloadHash(a) != payloadHash(b) {
		t.Fatal("亚微秒部分应在落库/哈希前截断，保证与 PG timestamptz 一致")
	}
}

func TestRecordHash_ChainsPreviousHash(t *testing.T) {
	p := samplePayload()
	h := payloadHash(p)
	now := time.Now().UTC().Truncate(time.Microsecond)
	first := recordHash("", p, h, now)
	second := recordHash(first, p, h, now.Add(time.Second))
	third := recordHash(second, p, h, now.Add(2*time.Second))

	if first == "" || second == "" || third == "" {
		t.Fatal("哈希链摘要不能为空")
	}
	if first == second || second == third {
		t.Fatal("链上每一环必须唯一（prev_hash 不同）")
	}
	// 篡改前一环必须导致后续摘要改变
	if recordHash("forged", p, h, now.Add(time.Second)) == second {
		t.Fatal("prev_hash 被篡改后 record_hash 必须变化，链才能检测篡改")
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*SubmitPayload)
		wantErr bool
	}{
		{"合法", func(p *SubmitPayload) {}, false},
		{"缺 event_id", func(p *SubmitPayload) { p.EventID = " " }, true},
		{"序号为 0", func(p *SubmitPayload) { p.SequenceNo = 0 }, true},
		{"缺时间", func(p *SubmitPayload) { p.OccurredAt = time.Time{} }, true},
		{"非法 sha 长度", func(p *SubmitPayload) { p.ClipSHA256 = "abc" }, true},
		{"负时长", func(p *SubmitPayload) { p.DurationMs = -1 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := samplePayload()
			tc.mutate(&p)
			err := p.validate()
			if tc.wantErr && err == nil {
				t.Fatal("期望校验错误，实际通过")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("未期望错误，实际: %v", err)
			}
		})
	}
}
