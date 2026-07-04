import express from "express";
import multer from "multer";
import { randomUUID } from "node:crypto";
import { connect, EXCHANGE } from "./rabbit.js";
import { pool, initDb } from "./db.js";

// multer drops uploaded files into ./storage as <uuid>.<ext> — boilerplate.
const upload = multer({ dest: "storage/" });

async function main() {
  await initDb();

  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });

  const app = express();

  // POST /videos  (multipart form, field name "video")
  app.post("/videos", upload.single("video"), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: "no file" });

    const videoId = randomUUID();
    const originalPath = req.file.path; // where multer saved it

    await pool.query(
      "INSERT INTO videos (id, original_path, status) VALUES ($1, $2, 'uploaded')",
      [videoId, originalPath],
    );

    await channel.publish(
      EXCHANGE,
      "",
      Buffer.from(JSON.stringify({ videoId, originalPath })),
    );

    res.status(202).json({ videoId, status: "uploaded" });
  });

  app.listen(3000, () => console.log("upload API on http://localhost:3000"));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
