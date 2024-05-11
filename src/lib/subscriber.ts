import { HubEvent, HubEventType } from '@farcaster/hub-nodejs'

import { saveLatestEventId } from '../api/event.js'
import { createQueue, createWorker } from './bullmq.js'
import { handleEvent } from './event.js'
import { hubClient } from './hub-client.js'
import { log } from './logger.js'

export const streamQueue = createQueue<HubEvent>('stream')
createWorker<HubEvent>('stream', handleEvent, { concurrency: 1 })

/**
 * Listen for new events from a Hub
 */
export async function subscribe(fromEventId: number | undefined) {
  const result = await hubClient.subscribe({
    eventTypes: [
      HubEventType.MERGE_MESSAGE,
      HubEventType.PRUNE_MESSAGE,
      HubEventType.REVOKE_MESSAGE,
    ],
    fromId: fromEventId,
  })

  if (result.isErr()) {
    log.error(result.error, 'Error starting stream')
    return
  }

  result.match(
    (stream) => {
      log.info(
        `Subscribed to stream from ${fromEventId ? `event ${fromEventId}` : 'head'}`
      )

      stream.on('data', async (e: HubEvent) => {
        await streamQueue.add('stream', e)
        // TODO: we can probably remove the `hub:latest-event-id` key and just use the last event ID in the queue
        await saveLatestEventId(e.id)
      })

      stream.on('close', async () => {
        log.warn(`Hub stream closed`)
      })

      stream.on('end', async () => {
        log.warn(`Hub stream ended`)
      })
    },
    (e) => {
      log.error(e, 'Error streaming data.')
    }
  )
}
