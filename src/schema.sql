-- videos = shared, mostly-immutable facts about the upload + the rolled-up status.
CREATE TABLE IF NOT EXISTS videos (
  id            UUID PRIMARY KEY,               -- app-generated (randomUUID) before insert
  original_path TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'uploaded' -- ROLLUP of the jobs rows
                  CHECK (status IN ('uploaded', 'processing', 'done', 'failed')),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- jobs = per-job mutable state. One row per (video, job_type).
-- Each worker owns its own row: its own status, its own attempts, its own output.
CREATE TABLE IF NOT EXISTS jobs (
  video_id    UUID NOT NULL REFERENCES videos(id),
  job_type    TEXT NOT NULL CHECK (job_type IN ('transcode', 'thumbnail', 'caption')),
  status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'processing', 'done', 'failed')),
  attempts    INT  NOT NULL DEFAULT 0,
  output_path TEXT,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (video_id, job_type)
);

-- outbox = messages to publish, written in the SAME txn as the state above.
-- A relay process publishes unsent rows to RabbitMQ, then stamps published_at.
-- BIGSERIAL (not UUID) is fine here: these rows are internal, ordered, throwaway.
CREATE TABLE IF NOT EXISTS outbox (
  id           BIGSERIAL PRIMARY KEY,
  exchange     TEXT NOT NULL,
  routing_key  TEXT NOT NULL DEFAULT '',
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

-- partial index keeps the relay's "find unpublished" poll cheap even as the table grows.
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
