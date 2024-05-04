import { HubEvent, HubEventType } from '@farcaster/hub-nodejs'

import { saveLatestEventId } from '../api/event.js'
import { createQueue, createWorker } from './bullmq.js'
import { handleEvents } from './event.js'
import { hubClient } from './hub-client.js'
import { log } from './logger.js'

export const streamQueue = createQueue<HubEvent[]>('stream')
createWorker<HubEvent[]>('stream', handleEvents)

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
      const eventsToQueue: HubEvent[] = new Array()

      stream.on('data', async (e: HubEvent) => {
        eventsToQueue.push(e)

        // Note: batches could get to be larger than 50 due to how hub events work
        if (eventsToQueue.length >= 50) {
          const lastEventId = eventsToQueue[eventsToQueue.length - 1].id

          // TODO: adding large objects can take a lot of memory, so we should consider
          // encoding with `HubEvent.encode(e).finish()` before adding it to the queue
          await streamQueue.add('stream', eventsToQueue)
          await saveLatestEventId(lastEventId)
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
