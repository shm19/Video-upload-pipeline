# VidPipe — a YouTube-style async video pipeline

A hands-on learning project for **async messaging**: message brokers, queues vs. pub/sub,
at-least-once delivery, idempotency, backpressure, and monitoring — built as a miniature
of a real video-ingest pipeline.

Stack: **Node + TypeScript**, **RabbitMQ**, **PostgreSQL**, **ffmpeg** + **whisper**,
all via **Docker Compose**. Monitoring via **Prometheus + Grafana**.

## What it does

Upload a video → it's stored → an event fans out to three independent jobs that run in
parallel: **transcode** (480p via ffmpeg), **thumbnail** (a frame grab), and **caption**
(audio → whisper → `.vtt`). Each job tracks its own state; the video's overall status is
rolled up from its jobs.

```
                              ┌─→ [transcode] → 480p mp4
POST /videos ─┐               │
              ├─ store file   ├─→ [thumbnail] → frame.jpg
              ├─ insert rows  │
              └─ publish ──► video.events (fanout) ─┼─→ [caption]   → whisper → .vtt
                                                    │
              failed 3× / poison ───────────────► video.dlx → video.dead
```

The message carries only a **pointer + metadata** (`{videoId, originalPath}`), never the
video bytes. State lives in Postgres; the message is just a trigger.

## Data model

- **`videos`** — shared, mostly-immutable facts + a rolled-up `status`
  (`uploaded | processing | done | failed`).
- **`jobs`** — one row per `(video_id, job_type)` with its own `status`, `attempts`, and
  `output_path`. Per-job state is what lets the three workers run independently without
  fighting over a single status/attempts field.

The API inserts the video row **and** its three `pending` job rows in one transaction, so
every worker is guaranteed to find its row.

## Architecture notes

- **Fanout exchange** (`video.events`) copies each upload event into three queues — this is
  **pub/sub** (each job type gets its own copy). Within a queue, running multiple worker
  processes gives **competing consumers** (each message handled once), which is how a job
  type scales horizontally — no code change, just more instances.
- **`prefetch(1)`** caps in-flight (unacked) messages per consumer to one → fair dispatch
  across workers and a per-worker concurrency cap (one ffmpeg/whisper at a time).
- **At-least-once + idempotency.** A worker acks only after the work is durably recorded, so
  a crash before ack causes redelivery. Workers are idempotent: the claim is guarded
  (`... AND status <> 'done'`) so an already-done job is skipped, and the actual work
  (ffmpeg `-y` overwrite, set-value DB updates) is safe to repeat.
- **Dead-letter exchange** (`video.dlx` → `video.dead`) catches poison messages: malformed
  bodies, orphans (no job row), and jobs that fail `MAX_ATTEMPTS` times.
- **Rollup by recompute, not coordination.** After finishing, each worker runs one atomic
  `UPDATE videos SET status = CASE ...` derived from the `jobs` rows — no worker decides
  whether it's "last", which avoids a check-then-write race.

## Code layout

| File | Role |
|---|---|
| `src/api.ts` | Express: serves the web UI, upload endpoint (file + txn + publish), and `GET /videos` status feed |
| `public/index.html` | Minimal web UI — upload a video, watch per-job status update live |
| `src/rabbit.ts` | Connection helper + `assertVideoTopology()` (exchange, DLX, dead queue, work queue) |
| `src/db.ts` | pg pool, `initDb()`, `recomputeVideoStatus()` |
| `src/base-worker.ts` | **Template Method** base: validate → guarded claim → delegate work → mark done + rollup → ack, with retry/DLX |
| `src/transcode-worker.ts` / `thumbnail-worker.ts` / `caption-worker.ts` | Each extends `BaseWorker` and implements only `processMessage()` |
| `src/utils.ts` | `run(cmd, args)` + `ffmpeg(args)` child-process helpers |
| `src/schema.sql` | `videos` + `jobs` tables |
| `monitoring/` | Prometheus scrape config + Grafana datasource provisioning |

## Setup

```bash
docker compose up -d          # RabbitMQ, Postgres, Prometheus, Grafana
npm install
mkdir -p storage
brew install ffmpeg
pipx install openai-whisper   # provides the `whisper` CLI
```

## Run

**One command** brings up infra (waits for health) and launches the API + all workers:

```bash
npm start
```

Then open the web UI at **http://localhost:3000** — pick a file, hit Upload, and watch the
per-job badges go `pending → processing → done` live (the page polls `GET /videos`).

Prefer separate terminals (nicer per-worker logs, and how you scale)?

```bash
docker compose up -d --wait
npm run api                 # UI + API on :3000
npm run worker              # transcode  (run several for competing consumers)
npm run worker:thumbnail
npm run worker:caption      # whisper — slow
```

Or upload from the CLI: `curl -F "video=@test-1min.mp4" http://localhost:3000/videos`

Scale a job type by running more instances of the same worker (competing consumers).

### Consoles
- RabbitMQ management UI: http://localhost:15672 (`guest`/`guest`)
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3001 — import dashboard ID **10991** (RabbitMQ-Overview)

## Testing the failure paths

- **Poison → DLX:** upload a non-video file; after 3 attempts the job goes `failed` and the
  message lands in `video.dead`.
- **Idempotency / redelivery:** `CRASH_AFTER_WORK=1 npm run worker` — the worker finishes,
  marks done, then dies before acking. Restart without the flag: the redelivered message is
  recognized as already-done and skipped.
- **Backpressure:** stop the workers, upload a burst, watch the queues' "Ready" count climb
  in the UI, then drain when workers restart.

## Monitoring — and an honest caveat

RabbitMQ exposes Prometheus metrics natively on `:15692` (no app code). Prometheus scrapes
them; Grafana graphs them. The metrics that map to decisions:

- **messages ready** = backlog → climbing = scale out consumers.
- **unacked** = in-flight work → stuck high = a wedged/slow consumer.
- **consumers per queue** → dropped = a worker died.
- **publish rate vs. ack rate** → publish outpacing ack = under-provisioned.
- **`video.dead` depth** → poison piling up; alert on `> 0`.

> **Reflection:** genuinely *deep* monitoring of a system like this — meaningful SLOs,
> alerting thresholds tuned to real traffic, dashboards that catch subtle regressions —
> needs an actual running system under real load and a lot of time to tune. What's here is
> the plumbing and the vocabulary (what to watch and why), not a production observability
> setup. That's a project of its own.

## Concepts demonstrated

Sync vs async · message broker (RabbitMQ) · exchange / queue / binding / routing key ·
fanout / pub-sub vs. work queue · prefetch & backpressure · at-least-once delivery ·
idempotent consumers · dead-letter exchange · competing consumers · rollup via recompute ·
Template Method for shared worker logic · Prometheus/Grafana metrics.
