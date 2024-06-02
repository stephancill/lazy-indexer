import 'dotenv/config'

import { getLatestEvent } from './api/event.js'
import { backfill } from './lib/backfill.js'
import { initExpressApp } from './lib/express.js'
import { subscribe } from './lib/subscriber.js'

try {
  initExpressApp()
} catch (error) {
  console.error(
    'Failed to initialize express app, it may already be running.',
    error
  )
}

if (process.argv[2] === '--backfill') {
  await backfill({
    maxFid: Number(process.env.BACKFILL_MAX_FID) || undefined,
  })
} else {
  subscribe(await getLatestEvent())
}
