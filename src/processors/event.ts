import { FARCASTER_EPOCH } from '@farcaster/hub-nodejs'

import { redis } from '../lib/redis.js'

const redisKey = 'hub:latest-event-id'

/**
 * Insert an event ID in the database
 * @param eventId Hub event ID
 */
export async function saveLatestEventId(eventId: number) {
  await redis.set(redisKey, eventId)
}

/**
 * Get the latest event ID from the database
 * @returns Latest event ID
 */
export async function getLatestEvent(): Promise<number | undefined> {
  const res = await redis.get(redisKey)
  return res ? parseInt(res) : undefined
}

export function makeLatestEventId() {
  const seq = 0
  const now = Date.now()
  const timestamp = now - FARCASTER_EPOCH
  const SEQUENCE_BITS = 12

  const binaryTimestamp = timestamp.toString(2)
  let binarySeq = seq.toString(2)
  if (binarySeq.length) {
    while (binarySeq.length < SEQUENCE_BITS) {
      binarySeq = `0${binarySeq}`
    }
  }

  return parseInt(binaryTimestamp + binarySeq, 2)
}
