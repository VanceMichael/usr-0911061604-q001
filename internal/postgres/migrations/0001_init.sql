-- 后厨影像证据服务：片段索引与不可篡改哈希链
-- 所有表均由服务启动时自动迁移（CREATE ... IF NOT EXISTS）。

CREATE TABLE IF NOT EXISTS cameras (
    store_id          TEXT        NOT NULL,
    camera_id         TEXT        NOT NULL,
    last_sequence_no  BIGINT      NOT NULL DEFAULT 0,
    last_event_time   TIMESTAMPTZ,
    last_record_hash  TEXT        NOT NULL DEFAULT '',
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_id, camera_id)
);

CREATE TABLE IF NOT EXISTS events (
    event_id         TEXT        PRIMARY KEY,
    store_id         TEXT        NOT NULL,
    camera_id        TEXT        NOT NULL,
    sequence_no      BIGINT      NOT NULL,
    occurred_at      TIMESTAMPTZ NOT NULL,
    clip_uri         TEXT        NOT NULL,
    clip_sha256      TEXT        NOT NULL DEFAULT '',
    duration_ms      BIGINT      NOT NULL DEFAULT 0,
    payload_hash     TEXT        NOT NULL,            -- 归一化载荷摘要，用于识别“同 ID 不同载荷”
    prev_hash        TEXT        NOT NULL DEFAULT '',
    record_hash      TEXT        NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 片段索引按门店+摄像头+时间查询；同一摄像头序号唯一，冲突时可据此报错
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_camera_seq
    ON events (store_id, camera_id, sequence_no);
CREATE INDEX IF NOT EXISTS idx_events_range
    ON events (store_id, camera_id, occurred_at);

-- 断线重传追踪：被拒绝的上报按 event_id 记录，重传成功后标记 resolved
CREATE TABLE IF NOT EXISTS failed_ingests (
    event_id         TEXT        PRIMARY KEY,
    store_id         TEXT        NOT NULL,
    camera_id        TEXT        NOT NULL,
    sequence_no      BIGINT      NOT NULL,
    payload          TEXT        NOT NULL,
    reason           TEXT        NOT NULL,
    detail           TEXT        NOT NULL DEFAULT '',
    attempts         INT         NOT NULL DEFAULT 1,
    resolved         BOOLEAN     NOT NULL DEFAULT FALSE,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_failed_open
    ON failed_ingests (store_id, resolved, last_seen_at DESC);
