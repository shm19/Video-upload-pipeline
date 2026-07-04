import { spawn } from "node:child_process";
import { connect, DLX, EXCHANGE } from "./rabbit.js";
import { pool } from "./db.js";

const QUEUE = "transcode";
const MAX_ATTEMPTS = 3;

export const DEAD_QUEUE = "video.dead";

// Helper: run ffmpeg as a child process, resolve when it exits 0. (boilerplate)
function ffmpeg(args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const p = spawn("ffmpeg", args, { stdio: "inherit" });
    p.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`ffmpeg exited ${code}`)),
    );
  });
}

async function main() {
  const { channel } = await connect();

  await channel.assertExchange(DLX, "fanout", { durable: true });
  await channel.assertQueue(DEAD_QUEUE, { durable: true });
  await channel.bindQueue(DEAD_QUEUE, DLX, "");

  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue(QUEUE, {
    durable: true,
    arguments: { "x-dead-letter-exchange": DLX },
  });
  await channel.bindQueue(QUEUE, EXCHANGE, "");
  channel.prefetch(1); // don't hand me a 2nd job until I ack the 1st

  await channel.consume(QUEUE, async (msg) => {
    if (!msg) return;
    let localVideoId;
    try {
      const { videoId, originalPath } = JSON.parse(msg.content.toString());
      localVideoId = videoId;
      await pool.query(
        "UPDATE videos SET status = 'processing' WHERE id = $1",
        [videoId],
      );
      await pool.query(
        "UPDATE videos SET attempts = attempts + 1 WHERE id = $1",
        [videoId],
      );
      await ffmpeg([
        "-y",
        "-i",
        originalPath,
        "-vf",
        "scale=-2:480",
        `storage/${videoId}-480.mp4`,
      ]);
      await pool.query(
        "UPDATE videos SET status = 'done', transcoded_path = $1 WHERE id = $2",
        [`storage/${videoId}-480.mp4`, videoId],
      );
      channel.ack(msg);
    } catch (e) {
      // first fetch the number of attempts
      const attemps = await pool.query(
        "SELECT attempts FROM videos WHERE id = $1",
        [localVideoId],
      );

      if (attemps.rows.length > 0 && attemps.rows[0].attempts >= MAX_ATTEMPTS) {
        await pool.query("UPDATE videos SET status = 'failed' WHERE id = $1", [
          localVideoId,
        ]);
        channel.nack(msg, false, false);
        return;
      }
      channel.nack(msg);
    }
  });

  console.log(`transcode worker waiting on "${QUEUE}"`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
