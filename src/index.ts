import { EventRequest } from '@farcaster/hub-nodejs'
import 'dotenv/config'

import { getLatestEvent } from './api/event.js'
import { backfill } from './lib/backfill.js'
import { initExpressApp } from './lib/express.js'
import { hubClient } from './lib/hub-client.js'
import { log } from './lib/logger.js'
import { subscribe } from './lib/subscriber.js'

// Check the latest hub event we processed, if any
let latestEventId = await getLatestEvent()

// Hubs are expected to prune messages after 3 days
const latestEventRequest = EventRequest.create({ id: latestEventId })
const latestEvent = await hubClient.getEvent(latestEventRequest)

// If the last saved event is no longer available, we need to backfill from the beginning
if (!latestEvent.isOk()) {
  log.warn('Latest recorded event is no longer available')
  latestEventId = undefined
}

initExpressApp()

// TODO: add better logic for determining if a backfill should run
await backfill({ maxFid: 100 })
await subscribe(latestEventId)
