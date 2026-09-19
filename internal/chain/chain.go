// Package chain 实现按（门店, 摄像头）维度的防篡改哈希链。
//
// 每条索引记录的 chain_hash 由前一条记录的 chain_hash 与本条内容的
// payload_hash 链接而成：chain_hash = SHA256("C1" | prev_chain_hash | payload_hash)。
// 任何对历史记录的改动都会使后续所有链哈希校验失败，从而可被发现。
package chain

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"kitchen-evidence/internal/domain"
)

// GenesisHash 是每条链的创世前驱哈希（64 个 0）。
const GenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// fieldSep 使用 ASCII 单元分隔符，避免与字段内容冲突。
const fieldSep = "\x1f"

// 域分隔前缀，防止 payload 哈希与链哈希跨域混用。
const (
	payloadDomain = "P1"
	chainDomain   = "C1"
	formatVersion = "v1"
)

// Canonical 生成事件的规范串：字段顺序固定，时间统一为 UTC RFC3339Nano。
// 同一事件在任何机器上都会得到完全相同的串，这是可复核摘要的基础。
func Canonical(storeID, cameraID, eventID string, seq int64, eventType string,
	occurredAt, clipStart, clipEnd time.Time, clipURI, mediaHash string) string {
	parts := []string{
		formatVersion,
		storeID,
		cameraID,
		eventID,
		strconv.FormatInt(seq, 10),
		eventType,
		occurredAt.UTC().Format(time.RFC3339Nano),
		clipStart.UTC().Format(time.RFC3339Nano),
		clipEnd.UTC().Format(time.RFC3339Nano),
		clipURI,
		mediaHash,
	}
	return strings.Join(parts, fieldSep)
}

// PayloadHash 计算事件规范串的摘要，用于幂等冲突判定与链式链接。
func PayloadHash(canonical string) string {
	return sha256Hex(payloadDomain + fieldSep + canonical)
}

// Link 由前驱链哈希与本条 payload 哈希推出本条链哈希。
func Link(prevChainHash, payloadHash string) string {
	return sha256Hex(chainDomain + fieldSep + prevChainHash + fieldSep + payloadHash)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// Verify 回放整条链：要求序号从 1 连续、prev 链接正确、链哈希重算一致，
// 且最终链头与 camera_chains 中登记的链头相等。events 必须按 seq 升序。
func Verify(events []domain.StoredEvent, headSeq int64, headHash string) domain.VerifyResult {
	res := domain.VerifyResult{HeadSeq: headSeq, HeadChainHash: headHash}

	prev := GenesisHash
	for i, ev := range events {
		res.Checked++
		expectedSeq := int64(i) + 1
		if ev.Seq != expectedSeq {
			res.Reason = "序号不连续，存在缺失片段"
			res.FirstBrokenSeq = ev.Seq
			return res
		}
		if ev.PrevChainHash != prev {
			res.Reason = "prev_chain_hash 链接断裂，记录可能被增删"
			res.FirstBrokenSeq = ev.Seq
			return res
		}
		if Link(prev, ev.PayloadHash) != ev.ChainHash {
			res.Reason = "chain_hash 重算不一致，记录内容可能被篡改"
			res.FirstBrokenSeq = ev.Seq
			return res
		}
		prev = ev.ChainHash
	}

	if len(events) == 0 {
		if headSeq != 0 {
			res.Reason = "链头存在但没有任何索引记录"
			return res
		}
		res.Valid = true
		return res
	}
	last := events[len(events)-1]
	if headSeq != last.Seq || headHash != last.ChainHash {
		res.Reason = "链头与最新记录不一致，可能存在未登记的写入"
		res.FirstBrokenSeq = last.Seq
		return res
	}
	res.Valid = true
	return res
}
