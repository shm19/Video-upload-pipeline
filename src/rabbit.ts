import amqp, { type Channel, type ChannelModel } from "amqplib";

const URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

// Thin connection helper — boilerplate, so you don't rewrite it in every file.
// Opens a TCP connection to the broker and a channel (the thing you actually
// publish/consume on). Everything below the connection is YOUR job.
export async function connect(): Promise<{
  conn: ChannelModel;
  channel: Channel;
}> {
  const conn = await amqp.connect(URL);
  const channel = await conn.createChannel();
  return { conn, channel };
}

// Shared names so producer and consumer agree on the topology.
export const EXCHANGE = "video.events";
export const DLX = "video.dlx";
export const DEAD_QUEUE = "video.dead";

// Declare the whole topology a worker needs: the fanout exchange, the DLX +
// dead-letter queue, and this worker's own work queue (bound to the fanout,
// dead-lettering to the DLX). assert is idempotent, so every worker can call it.
export async function assertVideoTopology(channel: Channel, queue: string): Promise<void> {
  await channel.assertExchange(DLX, "fanout", { durable: true });
  await channel.assertQueue(DEAD_QUEUE, { durable: true });
  await channel.bindQueue(DEAD_QUEUE, DLX, "");

  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue(queue, {
    durable: true,
    arguments: { "x-dead-letter-exchange": DLX },
  });
  await channel.bindQueue(queue, EXCHANGE, "");
}
