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
