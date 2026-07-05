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
