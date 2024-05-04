import { Job } from 'bullmq'

import { insertCasts } from '../api/cast.js'
import { saveLatestEventId } from '../api/event.js'
import { insertHubs } from '../api/hub.js'
import { insertLinks } from '../api/link.js'
import { insertReactions } from '../api/reaction.js'
import { insertUserDatas } from '../api/user-data.js'
import { insertVerifications } from '../api/verification.js'
import { createQueue, createWorker } from '../lib/bullmq.js'
import { hubClient } from '../lib/hub-client.js'
import { log } from '../lib/logger.js'
import { getFullProfileFromHub } from '../lib/utils.js'
import { makeLatestEventId, saveCurrentEventId } from './event.js'

type BackfillJob = {
  fids: number[]
}

export const backfillQueue = createQueue<BackfillJob>('backfill')
export const backfillWorker = createWorker<BackfillJob>('backfill', handleJob)

async function addFidsToBackfillQueue(maxFid?: number) {
  const fids = (await getAllFids()).slice(0, maxFid)
  const batchSize = 100

  for (let i = 0; i < fids.length; i += batchSize) {
    const batch = fids.slice(i, i + batchSize)
    await backfillQueue.add('backfill', { fids: batch })
  }

  log.info('Added fids to queue')
}

/**
 * Backfill the database with data from a hub.
 */
export async function backfill({ maxFid }: { maxFid?: number | undefined }) {
  log.info('Starting backfill')

  // Save the latest event ID so we can subscribe from there after backfill completes
  const latestEventId = makeLatestEventId()
  await saveLatestEventId(latestEventId)

  await addFidsToBackfillQueue(maxFid)
  await getHubs()
}

/**
 * Get all fids
 * @returns array of fids
 */
async function getAllFids() {
  const maxFidResult = await hubClient.getFids({
    pageSize: 1,
    reverse: true,
  })

  if (maxFidResult.isErr()) {
    throw new Error('Unable to backfill', { cause: maxFidResult.error })
  }

  const maxFid = maxFidResult.value.fids[0]
  return Array.from({ length: Number(maxFid) }, (_, i) => i + 1)
}

/**
 * Get all hubs
 */
async function getHubs() {
  const hubs = await hubClient.getCurrentPeers({})

  if (hubs.isErr()) {
    throw new Error('Unable to backfill Hubs', { cause: hubs.error })
  }

  insertHubs(hubs.value.contacts)
}

async function handleJob(job: Job) {
  const { fids } = job.data

  for (let i = 0; i < fids.length; i++) {
    const fid = fids[i]

    const p = await getFullProfileFromHub(fid).catch((err) => {
      log.error(err, `Error getting profile for FID ${fid}`)
      return null
    })

    if (!p) continue

    await insertCasts(p.casts)
    await insertLinks(p.links)
    await insertReactions(p.reactions)
    await insertUserDatas(p.userData)
    await insertVerifications(p.verifications)
  }

  await job.updateProgress(100)
}
