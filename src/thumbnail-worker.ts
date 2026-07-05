import { connect, assertVideoTopology } from "./rabbit.js";
import { pool, recomputeVideoStatus } from "./db.js";
import { ffmpeg } from "./utils.js";

const QUEUE = "thumbnail";
const JOB_TYPE = "thumbnail";
const MAX_ATTEMPTS = 3;
const log = (...a: unknown[]) => console.log(`[${JOB_TYPE}]`, ...a);

async function main() {
  const { channel } = await connect();
  await assertVideoTopology(channel, QUEUE);
  channel.prefetch(1);

  await channel.consume(QUEUE, async (msg) => {
    if (!msg) return;

    let videoId: string, originalPath: string;
    try {
      ({ videoId, originalPath } = JSON.parse(msg.content.toString()));
    } catch {
      log("malformed message → DLX");
      channel.nack(msg, false, false);
      return;
    }

    const claim = await pool.query(
      `UPDATE jobs SET status = 'processing', attempts = attempts + 1, updated_at = now()
       WHERE video_id = $1 AND job_type = $2
       RETURNING attempts`,
      [videoId, JOB_TYPE],
    );
    if (claim.rows.length === 0) {
      log(`no job row for ${videoId} → DLX`);
      channel.nack(msg, false, false);
      return;
    }
    const attempts: number = claim.rows[0].attempts;
    log(`${videoId} received — attempt ${attempts}, working...`);

    try {
      const out = `storage/${videoId}.jpg`;
      await ffmpeg([
        "-y",
        "-i",
        originalPath,
        "-ss",
        "00:00:01",
        "-vframes",
        "1",
        out,
      ]);

      await pool.query(
        `UPDATE jobs SET status = 'done', output_path = $3, updated_at = now()
         WHERE video_id = $1 AND job_type = $2`,
        [videoId, JOB_TYPE, out],
      );
      await recomputeVideoStatus(videoId);
      log(`${videoId} DONE → ${out}`);
      channel.ack(msg);
    } catch (e) {
      log(`${videoId} error on attempt ${attempts}:`, e);
      if (attempts >= MAX_ATTEMPTS) {
        await pool.query(
          `UPDATE jobs SET status = 'failed', updated_at = now()
           WHERE video_id = $1 AND job_type = $2`,
          [videoId, JOB_TYPE],
        );
        await recomputeVideoStatus(videoId);
        log(`${videoId} gave up after ${attempts} → DLX`);
        channel.nack(msg, false, false);
      } else {
        log(`${videoId} will retry`);
        channel.nack(msg);
      }
    }
  });

  log(`waiting on "${QUEUE}"`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
