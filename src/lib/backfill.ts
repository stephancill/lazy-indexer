import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'
import { Job } from 'bullmq'

import { insertCasts } from '../api/cast.js'
import { insertHubs } from '../api/hub.js'
import { insertLinks } from '../api/link.js'
import { insertReactions } from '../api/reaction.js'
import { insertUserDatas } from '../api/user-data.js'
import { insertVerifications } from '../api/verification.js'
import { createQueue, createWorker, serverAdapter } from './bullmq.js'
import { saveCurrentEventId } from './event.js'
import { hubClient } from './hub-client.js'
import { log } from './logger.js'
import { getFullProfileFromHub } from './utils.js'

type BackfillJob = {
  fids: number[]
}

const backfillQueue = createQueue<BackfillJob>('backfill')
createWorker<BackfillJob>('backfill', handleJob)

createBullBoard({
  queues: [new BullMQAdapter(backfillQueue)],
  serverAdapter,
})

/**
 * Backfill the database with data from a hub. This may take a while.
 */
export async function backfill({ maxFid }: { maxFid?: number | undefined }) {
  // Save the current event ID so we can start from there after backfilling
  await saveCurrentEventId()

  log.info('Backfilling...')
  await addFidsToBackfillQueue(maxFid)
  await getHubs()
}

async function addFidsToBackfillQueue(maxFid?: number) {
  const fids = (await getAllFids()).slice(0, maxFid)
  const batchSize = 100

  for (let i = 0; i < fids.length; i += batchSize) {
    const batch = fids.slice(i, i + batchSize)
    await backfillQueue.add('backfill', { fids: batch })
  }
}

async function getHubs() {
  const hubs = await hubClient.getCurrentPeers({})

  if (hubs.isErr()) {
    throw new Error('Unable to backfill Hubs', { cause: hubs.error })
  }

  insertHubs(hubs.value.contacts)
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

async function handleJob(job: Job) {
  const { fids } = job.data

  for (let i = 0; i < fids.length; i++) {
    const fid = fids[i]

    const profile = await getFullProfileFromHub(fid).catch((err) => {
      log.error(err, `Error getting profile for FID ${fid}`)
      return null
    })

    if (!profile) continue

    await insertCasts(profile.casts)
    await insertLinks(profile.links)
    await insertReactions(profile.reactions)
    await insertUserDatas(profile.userData)
    await insertVerifications(profile.verifications)

    await job.updateProgress((i + 1 / fids.length) * 100)
  }
}
