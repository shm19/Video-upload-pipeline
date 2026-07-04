import { connect, EXCHANGE } from "./rabbit.js";

async function main() {
  const { channel } = await connect();

  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });

  // lets create the transcode queue
  await channel.assertQueue("transcode", { durable: true });

  await channel.bindQueue("transcode", EXCHANGE, "");

  await channel.consume("transcode", (msg) => {
    if (msg) {
      console.log(msg.toString());
      console.log(msg.content);
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
