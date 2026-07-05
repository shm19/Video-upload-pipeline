import express from "express";
import multer from "multer";
import { randomUUID } from "node:crypto";
import { pool, initDb } from "./db.js";
import { EXCHANGE } from "./rabbit.js";

const upload = multer({ dest: "storage/" });

async function main() {
  await initDb();

  const app = express();
  app.use(express.static("public"));
  app.use("/media", express.static("storage"));

  // The API no longer talks to RabbitMQ at all — the relay publishes the outbox row.
  app.post("/videos", upload.single("video"), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: "no file" });

    const videoId = randomUUID();
    const originalPath = req.file.path;

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
      await client.query(
        "INSERT INTO outbox (exchange, payload) VALUES ($1, $2)",
        [EXCHANGE, { videoId, originalPath }], // pg serializes the object to jsonb
      );
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }

    res.status(202).json({ videoId, status: "uploaded" });
  });

  // Status feed the UI polls.
  app.get("/videos", async (_req, res) => {
    const { rows } = await pool.query(
      `SELECT v.id, v.status, v.created_at,
              json_agg(
                json_build_object(
                  'job_type', j.job_type,
                  'status', j.status,
                  'attempts', j.attempts,
                  'output_path', j.output_path
                ) ORDER BY j.job_type
              ) AS jobs
       FROM videos v
       LEFT JOIN jobs j ON j.video_id = v.id
       GROUP BY v.id
       ORDER BY v.created_at DESC
       LIMIT 20`,
    );
    res.json(rows);
  });

  app.listen(3000, () =>
    console.log("VidPipe UI + API on http://localhost:3000"),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
