-- 0001_init.sql 后厨影像证据索引的初始 schema

CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 片段索引：一行就是一条可公开复核的片段证据。
-- 幂等约束 uq_camera_event 保证同一 (门店, 摄像头, event_id) 只落库一次；
-- 唯一约束 uq_camera_seq 保证序号在摄像头维度唯一，配合哈希链防篡改。
CREATE TABLE IF NOT EXISTS clip_events (
    id              TEXT PRIMARY KEY,
    store_id        TEXT NOT NULL,
    camera_id       TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    seq             BIGINT NOT NULL,
    event_type      TEXT NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    clip_start      TIMESTAMPTZ NOT NULL,
    clip_end        TIMESTAMPTZ NOT NULL,
    clip_uri        TEXT NOT NULL,
    media_hash      TEXT NOT NULL,
    payload_hash    TEXT NOT NULL,
    prev_chain_hash TEXT NOT NULL,
    chain_hash      TEXT NOT NULL,
    CONSTRAINT uq_camera_event UNIQUE (store_id, camera_id, event_id),
    CONSTRAINT uq_camera_seq   UNIQUE (store_id, camera_id, seq)
);

-- 按时间范围复核投诉片段的查询索引。
CREATE INDEX IF NOT EXISTS idx_clip_events_time
    ON clip_events (store_id, camera_id, occurred_at);

-- 每个 (门店, 摄像头) 的链头：最新连续序号与链哈希。
CREATE TABLE IF NOT EXISTS camera_chains (
    store_id        TEXT NOT NULL,
    camera_id       TEXT NOT NULL,
    last_seq        BIGINT NOT NULL DEFAULT 0,
    last_chain_hash TEXT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_id, camera_id)
);

-- 上报尝试审计：成功、幂等命中、被拒绝（含原因）都留痕，供投诉复核定位。
CREATE TABLE IF NOT EXISTS ingest_attempts (
    id           BIGSERIAL PRIMARY KEY,
    store_id     TEXT NOT NULL DEFAULT '',
    camera_id    TEXT NOT NULL DEFAULT '',
    event_id     TEXT NOT NULL DEFAULT '',
    outcome      TEXT NOT NULL,
    error_code   TEXT NOT NULL DEFAULT '',
    detail       TEXT NOT NULL DEFAULT '',
    payload_hash TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_attempts_camera
    ON ingest_attempts (store_id, camera_id, id DESC);
