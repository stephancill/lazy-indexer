import 'dotenv/config'

import { getLatestEvent } from './api/event.js'
import { backfill, backfillQueue, backfillWorker } from './lib/backfill.js'
import { initExpressApp } from './lib/express.js'
import { log } from './lib/logger.js'
import { subscribe } from './lib/subscriber.js'

initExpressApp()

// Check the latest hub event we processed, if any
const latestEventId = await getLatestEvent()

if (process.argv[2] === '--backfill') {
  // TODO: add better logic for determining if a backfill should run
  await backfill({ maxFid: 100 })

  // Once backfill completes, start subscribing to new events
  let subscriberStarted = false
  backfillWorker.on('completed', async () => {
    if (subscriberStarted) return
    const queueSize = await backfillQueue.getActiveCount()

    if (queueSize === 0) {
      subscriberStarted = true
      log.info('Finished backfill')
      subscribe(latestEventId)
    }
  })
} else {
  subscribe(latestEventId)
}
