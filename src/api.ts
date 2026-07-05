import express from "express";
import multer from "multer";
import { randomUUID } from "node:crypto";
import { connect, EXCHANGE } from "./rabbit.js";
import { pool, initDb } from "./db.js";

const upload = multer({ dest: "storage/" });

async function main() {
  await initDb();

  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });

  const app = express();

  app.post("/videos", upload.single("video"), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: "no file" });

    const videoId = randomUUID();
    const originalPath = req.file.path;

    // Insert the video AND its three pending jobs in ONE transaction.
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "INSERT INTO videos (id, original_path) VALUES ($1, $2)",
        [videoId, originalPath],
      );
      await client.query(
        `INSERT INTO jobs (video_id, job_type) VALUES
           ($1, 'transcode'), ($1, 'thumbnail'), ($1, 'caption')`,
        [videoId],
      );
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }

    await channel.publish(
      EXCHANGE,
      "",
      Buffer.from(JSON.stringify({ videoId, originalPath })),
    );

    res.status(202).json({ videoId, status: "uploaded" });
  });

  app.listen(3000, () => console.log("upload API on http://localhost:3000"));

  // debug: print video + job status every 5s
  setInterval(async () => {
    const { rows } = await pool.query(
      `SELECT v.id, v.status AS video, j.job_type, j.status AS job, j.attempts
       FROM videos v
       LEFT JOIN jobs j ON j.video_id = v.id
       ORDER BY v.created_at DESC, j.job_type`,
    );
    console.log(`\n=== status @ ${new Date().toLocaleTimeString()} ===`);
    console.table(rows);
  }, 5000);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
