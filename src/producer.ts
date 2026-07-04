import { connect, EXCHANGE } from "./rabbit.js";

// ── YOUR JOB (Phase 1) ────────────────────────────────────────────────
// Publish ONE message representing a video upload event.
//
// Steps to figure out and implement:
//   1. connect() to get { conn, channel }.
//   2. Declare the exchange. Which TYPE did you decide on, and why?
//        channel.assertExchange(EXCHANGE, /* type */, { durable: true })
//   3. Publish one message to that exchange.
//        - remember: the payload is METADATA, not the video bytes
//        - amqplib wants a Buffer: Buffer.from(JSON.stringify(payload))
//        - for a fanout exchange, what should the routing key be?
//   4. Close the channel + connection so the process exits.
//
// Run it with:  npm run producer
// Watch it land in the UI:  http://localhost:15672  (guest / guest)

async function main() {
  const { conn, channel } = await connect();

  // TODO: assert the exchange

  // TODO: build a sample payload, e.g.
  //   { videoId: "123", filePath: "/storage/123.mp4", uploadedAt: Date.now() }

  // TODO: publish it

  console.log("TODO: publish a message");

  await channel.close();
  await conn.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
