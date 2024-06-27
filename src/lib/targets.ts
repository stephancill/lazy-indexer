import { db } from '../db/kysely.js'
import { redis } from './redis.js'

export const rootTargetsKey = 'targets:root'
export const allTargetsKey = 'targets:all'

export async function getTargets(key: string) {
  const targetsStrings = await redis.smembers(key)
  const targetsNumbers = targetsStrings.map(parseInt)
  return new Set(targetsNumbers)
}

export async function isRootTarget(fid: number) {
  return isTarget(fid, rootTargetsKey)
}

export async function isTarget(fid: number, key: string = allTargetsKey) {
  const result = await redis.sismember(key, fid)
  return result === 1
}

export async function addRootTarget(fid: number) {
  await redis.sadd(rootTargetsKey, fid)
  await addTarget(fid)
}

export async function addTarget(fid: number) {
  await redis.sadd(allTargetsKey, fid)
}

export async function removeRootTarget(fid: number) {
  await redis.srem(rootTargetsKey, fid)
}

export async function removeTarget(fid: number) {
  await redis.srem(allTargetsKey, fid)
}

export const allTargets = getTargets(allTargetsKey)
export const rootTargets = getTargets(rootTargetsKey)
