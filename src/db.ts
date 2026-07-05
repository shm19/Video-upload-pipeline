import { readFile } from "node:fs/promises";
import pg from "pg";

const { Pool } = pg;

// Shared connection pool — import { pool } wherever you query.
export const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ?? "postgres://vidpipe:vidpipe@localhost:5432/vidpipe",
});

// Create the schema if it doesn't exist. Call once at startup (the API does this).
export async function initDb(): Promise<void> {
  const sql = await readFile(new URL("./schema.sql", import.meta.url), "utf8");
  await pool.query(sql);
}

// Roll up the per-job rows into the video's overall status.
// Every worker calls this after updating its OWN job row. It doesn't decide
// anything — it just re-derives the truth from the jobs table in one atomic
// statement, so concurrent workers can't leave the video stuck.
export async function recomputeVideoStatus(videoId: string): Promise<void> {
  await pool.query(
    `UPDATE videos v SET
       status = CASE
         WHEN (SELECT bool_and(status = 'done')   FROM jobs WHERE video_id = v.id) THEN 'done'
         WHEN (SELECT bool_or(status  = 'failed') FROM jobs WHERE video_id = v.id) THEN 'failed'
         ELSE 'processing'
       END,
       updated_at = now()
     WHERE v.id = $1`,
    [videoId],
  );
}
