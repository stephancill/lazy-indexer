# Lazy Indexer

This is a Farcaster indexer which only syncs network data for specified target user IDs. It borrows indexing logic from the [Greg's indexer](https://github.com/gskril/farcaster-indexer), and streaming logic from [shuttle](https://github.com/farcasterxyz/hub-monorepo/tree/main/packages/shuttle).

## Principles

- Hub stream events are only indexed when they involve a target FID
- A root target is a target which also spawns other targets
- Root targets are backfilled along with all the users they follow.
- When a root target user follows a new user, the indexer will automatically backfill the new FID and add it as a target.
- When a new signer is detected which contains metadata from a specified app, the FID or signer public key will be added as a root target.

## API Endpoints

### POST `/root-backfill/:fid`

- **Description**: Creates or updates a root backfill job for the specified FID (file identifier). If a job exists and the force query parameter is not set, the existing job's status is returned.
- **Parameters**:
  - `:fid` - Farcaster ID of the user
  - `force` - Query parameter to force the creation of a new job even if one exists (optional).
- **Response Codes**:
  - `200` - Job successfully created or updated.
  - `409` - Conflict, job already exists.

### GET `/root-backfill/:fid`

- **Description**: Retrieves the status of the root backfill job for the specified FID.
- **Parameters**:
  - `:fid` - Farcaster ID of the user
- **Response Codes**:
  - `200` - Job status retrieved successfully.
  - `404` - Job not found.
- **Sample Response**:

```json
{
  "status": "50 minutes remaining",
  "completedCount": 5,
  "waitingCount": 10,
  "childCount": 15,
  "done": false
}
```

### POST `/backfill/:fid`

- **Description**: Creates or updates a backfill job for the specified FID. If a job exists and the force query parameter is not set, the existing job's data is returned.
- **Parameters**:
  - `:fid` - Farcaster ID of the user
  - `force` - Query parameter to force the creation of a new job even if one exists (optional).
- **Response Codes**:
  - `200` - Job successfully created or updated.
  - `409` - Conflict, job already exists.

## How it works

- Targets are stored in a Redis set
- Whenever the hub stream consumer receives an event it checks if the FID involved with the message is in the targets set and only indexes it if it is

## How to run

Clone this repo

```bash
git clone https://github.com/stephancill/lazy-indexer.git
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
yarn migrate
```

Run the indexer

```bash
# Starts a server and backfill workers
yarn backfill

# Opens a Hub stream and indexes events in the targets set
yarn stream
```
