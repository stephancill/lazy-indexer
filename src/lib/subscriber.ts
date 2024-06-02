import { HubEventType } from '@farcaster/hub-nodejs'
import { ok } from 'neverthrow'

import { createQueue, createWorker } from './bullmq.js'
import { handleEvent, handleEventJob } from './event.js'
import { EventStreamConnection, HubEventStreamConsumer } from './eventStream.js'
import { hubClientWithHost } from './hub-client.js'
import { EventStreamHubSubscriber } from './hubSubscriber.js'
import { log } from './logger.js'
import { REDIS_URL, RedisClient } from './redis.js'

export const totalShards = parseInt(process.env['SHARDS'] || '0')
export const shardIndex = parseInt(process.env['SHARD_NUM'] || '0')

export const streamQueue = createQueue<Buffer>('stream')
createWorker<Buffer>('stream', handleEventJob, { concurrency: 1 })

const hubId = 'indexer'

/**
 * Listen for new events from a Hub
 */
export async function subscribe(fromEventId: number | undefined) {
  const eventTypes = [
    HubEventType.MERGE_MESSAGE,
    HubEventType.PRUNE_MESSAGE,
    HubEventType.REVOKE_MESSAGE,
    HubEventType.MERGE_ON_CHAIN_EVENT,
  ]

  const redis = RedisClient.create(REDIS_URL)
  const eventStreamForWrite = new EventStreamConnection(redis.client)
  const eventStreamForRead = new EventStreamConnection(redis.client)
  const shardKey = totalShards === 0 ? 'all' : `${shardIndex}`
  const hubSubscriber = new EventStreamHubSubscriber(
    hubId,
    hubClientWithHost,
    eventStreamForWrite,
    redis,
    shardKey,
    log,
    eventTypes,
    totalShards,
    shardIndex
  )
  const streamConsumer = new HubEventStreamConsumer(
    hubClientWithHost,
    eventStreamForRead,
    shardKey
  )

  await hubSubscriber.start()

  // Sleep 10 seconds to give the subscriber a chance to create the stream for the first time.
  await new Promise((resolve) => setTimeout(resolve, 10_000))

  log.info('Starting stream consumer')
  // Stream consumer reads from the redis stream and inserts them into postgres
  await streamConsumer.start(async (event) => {
    await handleEvent(event)
    return ok({ skipped: false })
  })
}
