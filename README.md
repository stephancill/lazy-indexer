# Farcaster Indexer

This is an indexer that listens for messages from a [Farcaster Hub](https://docs.farcaster.xyz/learn/architecture/hubs) and inserts relevant data into a postgres database.

The most performant way to run this is to co-locate everything (hub, node app, postgres, redis) on the same machine. I recommend [Latitude](https://www.latitude.sh/r/673C7DB2) (referral code for $200 of free credits).

## How to run

Clone this repo

```bash
git clone -b hubs https://github.com/gskril/farcaster-indexer.git
```

Install dependencies

```bash
yarn install
```

Create a `.env` file with hub and database connection details

```bash
cp .env.example .env
```

Run the latest database migrations

```bash
yarn kysely:migrate
```

Run the indexer

```bash
yarn start
```

## How it works

- If certain conditions are met, a backfill process is triggered to fetch the full Farcaster state from a hub. This uses [BullMQ](https://bullmq.io/) for job queueing and parallelization.
- Separately, the indexer subscribes to a hub's event stream and processes messages as they arrive.

## Todo

- [x] Subscribe to a hub's stream
- [x] Backfill select data from all FIDs
- [x] Handle messages in batches (e.g. queue up 1,000 messages of the same type and insert them on a schedule)
- [ ] Handle REVOKE_MESSAGE messages
- [ ] Improve handling of messages that arrive out of order (e.g. a MESSAGE_TYPE_CAST_REMOVE arriving before a MESSAGE_TYPE_CAST_ADD). Merkle's replicator enforces foreign key constraints to ensure that the data is consistent, and implements retry logic to handle out-of-order messages.
- [ ] Improve shutdown process to ensure no messages are lost (e.g. recover messages that are currently in queue). Maybe save ids every x messages so we don't have to depend on `handleShutdownSignal()`?
