import 'dotenv/config'

import { getLatestEvent } from './api/event.js'
import { migrateToLatest } from './db/migrator.js'
import { backfill } from './lib/backfill.js'
import { initExpressApp } from './lib/express.js'
import { subscribe } from './lib/subscriber.js'

await migrateToLatest()

if (process.argv[2] === '--backfill') {
  initExpressApp()
  await backfill({})
} else {
  subscribe(await getLatestEvent())
}
