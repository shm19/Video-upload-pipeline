import { connect, EXCHANGE } from "./rabbit.js";

// ── YOUR JOB (Phase 1) ────────────────────────────────────────────────
// Consume messages and log them.
//
// Steps to figure out and implement:
//   1. connect().
//   2. Declare the SAME exchange the producer uses (both sides assert it;
//      assert is idempotent, so it's safe for either to run first).
//   3. Declare a queue for this consumer.
//        - For Phase 1 you can start with a simple named queue.
//        - Later (Phase 3) you'll have one queue per job type.
//   4. BIND the queue to the exchange. This is the step that actually makes
//      messages flow exchange -> queue. Without a binding, a fanout exchange
//      drops the message (no bound queue = nowhere to route it).
//   5. channel.consume(queue, (msg) => { ... }) and log the content.
//        - decide: auto-ack or manual ack? (you said this is early — for now
//          pick one and be ready to defend it in Phase 5)
//
// Run it (in a second terminal, leave it running):  npm run consumer
// Then run the producer in another terminal and watch this log.

async function main() {
  const { channel } = await connect();

  // TODO: assert the exchange (same name/type as producer)
  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });

  // TODO: assert a queue
  // lets create the transcode queue
  await channel.assertQueue("transcode", { durable: true });

  // TODO: bind the queue to the exchange
  await channel.bindQueue("transcode", EXCHANGE, "");

  // TODO: channel.consume(...) and log msg.content.toString()
  await channel.consume("transcode", (msg) => {
    if (msg) {
      console.log(msg.content.toString());
      channel.ack(msg);
    }
  });

  console.log("TODO: consume messages. Waiting...");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
