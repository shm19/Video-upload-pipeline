# VidPipe — RabbitMQ video pipeline (learning project)

A YouTube-style upload pipeline for learning async messaging: queue vs pub/sub,
at-least-once delivery, idempotency, and broker monitoring.

## Setup

```bash
docker compose up -d          # start RabbitMQ + Postgres
npm install                   # install deps
```

Management UI: http://localhost:15672  (user: guest / pass: guest)

## Phase 1 — prove the loop

Two files are stubbed with TODOs — you write the messaging logic:

```bash
npm run consumer   # terminal 1 — leave running
npm run producer   # terminal 2 — publishes one message
```

Success = the consumer logs the message, and you can see the exchange, queue,
and binding in the management UI.

## Design decisions to hold in your head
- The message carries **metadata + a file pointer**, never the video bytes.
- Producer publishes to an **exchange**, not directly to a queue.
- `exchange -> binding -> queue -> consumer` is the full path. No binding = message dropped.
