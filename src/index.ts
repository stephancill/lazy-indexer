import 'dotenv/config'

import { getLatestEvent } from './api/event.js'
import { backfill, backfillQueue, backfillWorker } from './lib/backfill.js'
import { initExpressApp } from './lib/express.js'
import { log } from './lib/logger.js'
import { subscribe } from './lib/subscriber.js'

initExpressApp()

if (process.argv[2] === '--backfill') {
  await backfill({ maxFid: 100 })

  // Once backfill completes, start subscribing to new events
  let subscriberStarted = false
  backfillWorker.on('completed', async () => {
    if (subscriberStarted) return
    const queueSize = await backfillQueue.getActiveCount()

    if (queueSize === 0) {
      subscriberStarted = true
      log.info('Finished backfill')
      subscribe(await getLatestEvent())
    }
  })
} else {
  subscribe(await getLatestEvent())
}
