-- The video's state lives here. The RabbitMQ message is only a trigger that
-- points a worker at one of these rows.
CREATE TABLE IF NOT EXISTS videos (
  id              UUID PRIMARY KEY,                 -- app-generated (randomUUID) before insert
  original_path   TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'uploaded'
                    CHECK (status IN ('uploaded', 'processing', 'done', 'failed')),
  transcoded_path TEXT,
  attempts INT NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
