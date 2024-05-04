import { HubEvent, HubEventType } from '@farcaster/hub-nodejs'

import { saveLatestEventId } from '../api/event.js'
import { createQueue, createWorker } from './bullmq.js'
import { handleEvents } from './event.js'
import { hubClient } from './hub-client.js'
import { log } from './logger.js'

export const streamQueue = createQueue<Buffer[]>('stream')
createWorker<Buffer[]>('stream', handleEvents)

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
        `Subscribed to stream ${fromEventId ? `from event ${fromEventId}` : ''}`
      )

      // Batch events in the queue
      const eventsToQueue: Buffer[] = new Array()

      stream.on('data', async (e: HubEvent) => {
        const encodedEvent = Buffer.from(HubEvent.encode(e).finish())
        eventsToQueue.push(encodedEvent)

        // Note: batches could get to be larger than 50 due to how hub events work
        if (eventsToQueue.length >= 50) {
          await streamQueue.add('stream', eventsToQueue)
          await saveLatestEventId(e.id)
          eventsToQueue.length = 0
        }
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
