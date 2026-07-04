import { connect, EXCHANGE } from "./rabbit.js";

async function main() {
  const { conn, channel } = await connect();

  // TODO: assert the exchange
  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });

  //   { videoId: "123", filePath: "/storage/123.mp4", uploadedAt: Date.now() }
  const payload = {
    videoId: "123",
    filePath: "./test.mp4",
    uploadedAt: Date.now(),
  };

  await channel.publish(EXCHANGE, "", Buffer.from(JSON.stringify(payload)));

  console.log("TODO: publish a message");

  await channel.close();
  await conn.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
