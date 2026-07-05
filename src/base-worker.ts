import type { Channel, ConsumeMessage } from "amqplib";
import { connect, assertVideoTopology } from "./rabbit.js";
import { pool, recomputeVideoStatus } from "./db.js";

export interface JobMessage {
  videoId: string;
  originalPath: string;
}

/**
 * Template Method base for all job workers.
 *
 * The parent owns the invariant pipeline that's identical for every job type:
 *   validate format → claim job row (idempotent) → run work → mark done + roll up
 *   → ack, with retry/DLX on failure.
 *
 * A child supplies only:
 *   - queue + jobType (via the constructor)
 *   - processMessage() — the one step that actually differs (ffmpeg/whisper/etc.)
 */
export abstract class BaseWorker {
  protected readonly queue: string;
  protected readonly jobType: string;
  protected readonly maxAttempts: number;
  private channel!: Channel;

  constructor(opts: { queue: string; jobType: string; maxAttempts?: number }) {
    this.queue = opts.queue;
    this.jobType = opts.jobType;
    this.maxAttempts = opts.maxAttempts ?? 3;
  }

  protected log(...a: unknown[]): void {
    console.log(`[${this.jobType}]`, ...a);
  }

  /** Child implements the actual work and returns the output_path to store. */
  protected abstract processMessage(job: JobMessage): Promise<string>;

  async start(): Promise<void> {
    const { channel } = await connect();
    this.channel = channel;
    await assertVideoTopology(channel, this.queue);
    channel.prefetch(1);
    await channel.consume(this.queue, (msg) => this.handle(msg));
    this.log(`waiting on "${this.queue}"`);
  }

  // ── the invariant pipeline ────────────────────────────────────────────
  private async handle(msg: ConsumeMessage | null): Promise<void> {
    if (!msg) return;
    const ch = this.channel;

    // 1. validate format
    const job = this.parse(msg.content.toString());
    if (!job) {
      this.log("malformed message → DLX");
      ch.nack(msg, false, false);
      return;
    }
    const { videoId } = job;

    // 2. claim my job row: set processing + bump attempts, guarded so an
    //    already-done row is NOT re-claimed (idempotency).
    const claim = await pool.query(
      `UPDATE jobs SET status = 'processing', attempts = attempts + 1, updated_at = now()
       WHERE video_id = $1 AND job_type = $2 AND status <> 'done'
       RETURNING attempts`,
      [videoId, this.jobType],
    );
    if (claim.rows.length === 0) {
      // zero rows = already done (duplicate) OR no row (orphan) — tell them apart
      const { rows } = await pool.query(
        "SELECT status FROM jobs WHERE video_id = $1 AND job_type = $2",
        [videoId, this.jobType],
      );
      if (rows[0]?.status === "done") {
        this.log(`${videoId} already done → skip (idempotent)`);
        ch.ack(msg);
      } else {
        this.log(`no job row for ${videoId} → DLX`);
        ch.nack(msg, false, false);
      }
      return;
    }
    const attempts: number = claim.rows[0].attempts;
    this.log(`${videoId} received — attempt ${attempts}, working...`);

    // 3. delegate the actual work to the child, then mark done + roll up
    try {
      const outputPath = await this.processMessage(job);
      await this.markDone(videoId, outputPath);
      this.log(`${videoId} DONE → ${outputPath}`);
      if (process.env.CRASH_AFTER_WORK) process.exit(1); // test hook
      ch.ack(msg);
    } catch (e) {
      this.log(`${videoId} error on attempt ${attempts}:`, e);
      if (attempts >= this.maxAttempts) {
        await this.markFailed(videoId);
        this.log(`${videoId} gave up after ${attempts} → DLX`);
        ch.nack(msg, false, false);
      } else {
        this.log(`${videoId} will retry`);
        ch.nack(msg);
      }
    }
  }

  private parse(raw: string): JobMessage | null {
    try {
      const { videoId, originalPath } = JSON.parse(raw);
      if (typeof videoId === "string" && typeof originalPath === "string") {
        return { videoId, originalPath };
      }
      return null;
    } catch {
      return null;
    }
  }

  private async markDone(videoId: string, outputPath: string): Promise<void> {
    await pool.query(
      `UPDATE jobs SET status = 'done', output_path = $3, updated_at = now()
       WHERE video_id = $1 AND job_type = $2`,
      [videoId, this.jobType, outputPath],
    );
    await recomputeVideoStatus(videoId);
  }

  private async markFailed(videoId: string): Promise<void> {
    await pool.query(
      `UPDATE jobs SET status = 'failed', updated_at = now()
       WHERE video_id = $1 AND job_type = $2`,
      [videoId, this.jobType],
    );
    await recomputeVideoStatus(videoId);
  }
}
