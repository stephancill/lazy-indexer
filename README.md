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

Create a `.env` file with your hub, database, and redis connection details

```bash
cp .env.example .env
```

Run the latest database migrations

```bash
yarn kysely:migrate
```

Run the indexer

```bash
# Recommended to get the full state. You only need to run this once.
# Streaming will start after the backfill is complete.
yarn run backfill

# Ignores backfill and start streaming from the latest recorded event.
# You should run this after one initial backfill.
yarn start
```

## How it works

- Backfill and streaming are separate processes.
- Every operation is run through [BullMQ](https://bullmq.io/) for better concurrency and error handling.
- For backfill, the indexer adds all FIDs (in batches of 100) to a queue and processes them in parallel. The `WORKER_CONCURRENCY` environment variable controls how many workers are spawned.
- Once backfill is complete, the indexer subscribes to a hub's event stream and processes messages as they arrive. BullMQ is used as middleware to ensure that hub events are getting handled fast enough, otherwise the stream will disconnect.

## Extras

If you want to add search functionality, you can manually apply the SQL migration at [src/db/search-migrations.sql](./src/db/search-migrations.sql)
