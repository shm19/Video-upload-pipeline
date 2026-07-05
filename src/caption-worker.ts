/*
STEPS:
  - create the queue and channel and bind them (use dlx as well)
  - consume the messages of the queue and validate the message and nack if the format is not correct
  - update the status in jobs to processing and increase the attempts
  - process the caption job and update the status to done and update the video status
  - if there are any error check the number of attemp and if its more than max update to failed and not requeue
*/

import { connect, assertVideoTopology } from "./rabbit.js";
import { pool, recomputeVideoStatus } from "./db.js";
import { ffmpeg, run } from "./utils.js";

const QUEUE = "caption";
const JOB_TYPE = "caption";
const MAX_ATTEMPTS = 3;

async function main() {
  // first get the channel create the queue and binding and set prefetch
  const { channel } = await connect();
  await assertVideoTopology(channel, QUEUE);
  channel.prefetch(1);

  // lets check the queue message response
  await channel.consume(QUEUE, async (msg) => {
    let videoId: string, originalPath: string;

    if (!msg) return;
    // try to decode the message and process the caption job
    try {
      ({ videoId, originalPath } = JSON.parse(msg.content.toString()));
      // now you have your message, you need to update the tables to say you are processing and also increase teh attemp
    } catch (err) {
      channel.nack(msg, false, false);
      return;
    }

    const claims = await pool.query(
      `UPDATE jobs SET attempts = attempts + 1, status = 'processing', updated_at = now()
      WHERE video_id = $1 AND job_type = $2
      RETURNING attempts
      `,
      [videoId, JOB_TYPE],
    );
    if (claims.rows.length === 0) {
      console.error(`[caption] no rows from claim for ${videoId} → DLX`);
      channel.nack(msg, false, false);
      return;
    }
    const attemps = claims.rows[0].attempts;
    console.log(
      `[caption] ${videoId} received — attempt ${attemps}, working...`,
    );

    try {
      // now process the caption job
      const wav = `storage/${videoId}.wav`;
      await ffmpeg(["-y", "-i", originalPath, "-ar", "16000", "-ac", "1", wav]);
      await run("whisper", [
        wav,
        "--model",
        "tiny.en",
        "--output_format",
        "vtt",
        "--output_dir",
        "storage",
      ]);
      const out = `storage/${videoId}.vtt`; // whisper names output after the input basename

      await pool.query(
        `UPDATE jobs SET status = 'done', output_path = $3, updated_at = now()
         WHERE video_id = $1 AND job_type = $2`,
        [videoId, JOB_TYPE, out],
      );
      await recomputeVideoStatus(videoId);
      channel.ack(msg);
      console.log("finish", videoId);
    } catch (err) {
      console.error(`[caption] ${videoId} error on attempt ${attemps}:`, err);
      // here check the attempts with max attempts
      if (attemps >= MAX_ATTEMPTS) {
        await pool.query(
          `UPDATE jobs SET status = 'failed', updated_at = now()
           WHERE video_id = $1 AND job_type = $2`,
          [videoId, JOB_TYPE],
        );
        await recomputeVideoStatus(videoId);
        channel.nack(msg, false, false);
      } else {
        channel.nack(msg, false, true);
      }
    }
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
