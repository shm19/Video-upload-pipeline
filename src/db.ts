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
