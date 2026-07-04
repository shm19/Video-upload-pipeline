import amqp, { type Channel, type ChannelModel } from "amqplib";

const URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

// Thin connection helper — boilerplate, so you don't rewrite it in every file.
// Opens a TCP connection to the broker and a channel (the thing you actually
// publish/consume on). Everything below the connection is YOUR job.
export async function connect(): Promise<{ conn: ChannelModel; channel: Channel }> {
  const conn = await amqp.connect(URL);
  const channel = await conn.createChannel();
  return { conn, channel };
}

// Shared names so producer and consumer agree on the topology.
export const EXCHANGE = "video.events";
