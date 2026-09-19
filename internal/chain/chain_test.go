package chain

import (
	"strings"
	"testing"
	"time"

	"kitchen-evidence/internal/domain"
)

// TestGoldenVector 锁定规范串与哈希格式：任何改动都会破坏既有索引的可验证性。
func TestGoldenVector(t *testing.T) {
	occ := time.Date(2026, 9, 19, 1, 0, 0, 0, time.UTC)
	start := time.Date(2026, 9, 19, 0, 59, 30, 0, time.UTC)
	end := time.Date(2026, 9, 19, 1, 0, 0, 0, time.UTC)
	media := strings.Repeat("a", 64)

	c := Canonical("store-1", "cam-1", "evt-1", 1, "clip.ready", occ, start, end, "s3://clips/1.mp4", media)
	p := PayloadHash(c)
	const wantPayload = "b50615f4c318fbaccce93dd95dbdcec467ea9345f6e34842e0389394d01d2f86"
	if p != wantPayload {
		t.Fatalf("payload hash 格式漂移: got %s want %s", p, wantPayload)
	}
	const wantChain = "48a9156b9de62c5eb398e7f1741cd3489d80c8e06761153bea6b5cd63bfdd83e"
	if got := Link(GenesisHash, p); got != wantChain {
		t.Fatalf("chain hash 格式漂移: got %s want %s", got, wantChain)
	}
}

// TestCanonicalUTCNormalization 同一时刻的不同时区写法必须得到相同规范串。
func TestCanonicalUTCNormalization(t *testing.T) {
	utc := time.Date(2026, 9, 19, 1, 0, 0, 0, time.UTC)
	shanghai := utc.In(time.FixedZone("CST", 8*3600))
	a := Canonical("s", "c", "e", 1, "t", utc, utc, utc, "u", "m")
	b := Canonical("s", "c", "e", 1, "t", shanghai, shanghai, shanghai, "u", "m")
	if a != b {
		t.Fatal("同一时刻不同时区产生了不同规范串")
	}
}

func buildChain(n int) []domain.StoredEvent {
	prev := GenesisHash
	var evs []domain.StoredEvent
	ts := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= n; i++ {
		ph := PayloadHash(Canonical("s", "c", "evt", int64(i), "t", ts, ts, ts, "u", "m"))
		ch := Link(prev, ph)
		evs = append(evs, domain.StoredEvent{Seq: int64(i), PayloadHash: ph, PrevChainHash: prev, ChainHash: ch})
		prev = ch
	}
	return evs
}

func TestVerifyValidChain(t *testing.T) {
	evs := buildChain(5)
	res := Verify(evs, 5, evs[4].ChainHash)
	if !res.Valid || res.Checked != 5 {
		t.Fatalf("合法链应通过: %+v", res)
	}
}

func TestVerifyDetectsTampering(t *testing.T) {
	evs := buildChain(3)
	evs[1].PayloadHash = strings.Repeat("f", 64) // 篡改第二条内容摘要
	res := Verify(evs, 3, evs[2].ChainHash)
	if res.Valid || res.FirstBrokenSeq != 2 {
		t.Fatalf("应检出第 2 条被篡改: %+v", res)
	}
}

func TestVerifyDetectsBrokenLink(t *testing.T) {
	evs := buildChain(3)
	evs[2].PrevChainHash = GenesisHash // 人为断开链接
	res := Verify(evs, 3, evs[2].ChainHash)
	if res.Valid || res.FirstBrokenSeq != 3 {
		t.Fatalf("应检出链接断裂: %+v", res)
	}
}

func TestVerifyDetectsGap(t *testing.T) {
	evs := buildChain(3)
	evs = append(evs[:1], evs[2]) // 抽掉中间一条，序号不再连续
	res := Verify(evs, 2, evs[1].ChainHash)
	if res.Valid || res.FirstBrokenSeq != 3 {
		t.Fatalf("应检出序号缺口: %+v", res)
	}
}

func TestVerifyDetectsHeadMismatch(t *testing.T) {
	evs := buildChain(3)
	res := Verify(evs, 3, strings.Repeat("0", 64)) // 链头与记录不符
	if res.Valid {
		t.Fatal("应检出链头不一致")
	}
}

func TestVerifyEmptyChain(t *testing.T) {
	res := Verify(nil, 0, GenesisHash)
	if !res.Valid || res.Checked != 0 {
		t.Fatalf("空链应视为合法: %+v", res)
	}
}
