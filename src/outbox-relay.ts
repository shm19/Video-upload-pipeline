import amqp, { type ConfirmChannel } from "amqplib";
import { EXCHANGE } from "./rabbit.js";
import { pool } from "./db.js";

const URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
const POLL_MS = 500;
const BATCH = 20;

async function main() {
  const conn = await amqp.connect(URL);
  // A *confirm* channel lets us wait for the broker to durably accept a publish
  // before we mark the outbox row as sent — so we never lose a message.
  const channel = await conn.createConfirmChannel();
  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });

  console.log("[outbox] relay started");
  setInterval(() => {
    drain(channel).catch((e) => console.error("[outbox] drain error:", e));
  }, POLL_MS);
}

async function drain(channel: ConfirmChannel): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Claim a batch of unpublished rows. FOR UPDATE SKIP LOCKED means multiple
    // relay instances can run without ever grabbing the same row.
    const { rows } = await client.query(
      `SELECT id, exchange, routing_key, payload
       FROM outbox
       WHERE published_at IS NULL
       ORDER BY id
       FOR UPDATE SKIP LOCKED
       LIMIT $1`,
      [BATCH],
    );

    if (rows.length === 0) {
      await client.query("ROLLBACK"); // nothing to do; release locks
      return;
    }

    for (const r of rows) {
      channel.publish(r.exchange, r.routing_key ?? "", Buffer.from(JSON.stringify(r.payload)));
    }
    await channel.waitForConfirms(); // broker has durably accepted ALL of them

    await client.query(
      "UPDATE outbox SET published_at = now() WHERE id = ANY($1)",
      [rows.map((r) => r.id)],
    );
    await client.query("COMMIT");
    console.log(`[outbox] published ${rows.length}`);
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
