import { FlowProducer, Job, Queue } from 'bullmq'

import { deleteCasts, insertCasts } from '../api/cast.js'
import { saveLatestEventId } from '../api/event.js'
import { insertRegistrations } from '../api/fid.js'
import { deleteLinks, insertLinks } from '../api/link.js'
import { insertReactions } from '../api/reaction.js'
import { insertSigners } from '../api/signer.js'
import { insertStorage } from '../api/storage.js'
import { insertUserDatas } from '../api/user-data.js'
import {
  deleteVerifications,
  insertVerifications,
} from '../api/verification.js'
import { hubClient } from '../lib/hub-client.js'
import { log } from '../lib/logger.js'
import { checkMessages, getFullProfileFromHub } from '../lib/utils.js'
import { makeLatestEventId } from './event.js'
import { createQueue, createWorker } from './jobs.js'
import { getNetworkByFid } from './links-utils.js'
import { ExtraHubOptions } from './paginate.js'
import { redis } from './redis.js'
import { addRootTarget, addTarget, isTarget, removeTarget } from './targets.js'

type BackfillJob = {
  fid: number
  partial?: boolean
  removeMessages?: boolean
}

type RootBackfillJob = {
  fid: number
  backfillCount: number
}

const backfillQueueName = 'backfill'
const backfillJobName = 'backfill'
export const getBackfillQueue = () =>
  createQueue<BackfillJob>(backfillQueueName)
export const getBackfillWorker = () =>
  createWorker<BackfillJob>(backfillQueueName, handleBackfillJob)

const rootBackfillQueueName = 'rootBackfill'
export const rootBackfillJobName = 'rootBackfill'
export const getRootBackfillQueue = () =>
  createQueue<RootBackfillJob>(rootBackfillQueueName)
export const getRootBackfillWorker = () =>
  createWorker<RootBackfillJob>(rootBackfillQueueName, async (job) => {
    log.info(`Completed root backfill job for FID ${job.data.fid}`)
  })

const flowProducer = new FlowProducer({ connection: redis })

export function getBackfillPartialJobId(fid: number) {
  return `backfill:partial:${fid}`
}

export function getBackfillJobId(fid: number) {
  return `backfill:${fid}`
}

export function getRootBackfillJobId(fid: number) {
  return `backfill:root:${fid}`
}

/**
 * Backfill the database with data from a hub. This may take a while.
 */
export async function backfill({ maxFid }: { maxFid?: number | undefined }) {
  const backfillWorker = getBackfillWorker()
  backfillWorker.run()

  const rootBackfillWorker = getRootBackfillWorker()
  rootBackfillWorker.run()

  // Save the latest event ID so we can subscribe from there after backfill completes
  const latestEventId = makeLatestEventId()
  await saveLatestEventId(latestEventId)
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

export async function createRootBackfillJob(
  rootFid: number,
  childJobOptions: Omit<BackfillJob, 'fid'> = {}
) {
  const {
    linksByDepth: { ['1']: linksSet },
  } = await getNetworkByFid(rootFid, 2, {
    onProgress(message) {
      log.info(message)
    },
  })

  log.info(`Found ${linksSet.size} links for FID ${rootFid}`)

  addRootTarget(rootFid)

  const backfillFids = [rootFid, ...Array.from(linksSet)]

  const flow = await flowProducer.add({
    name: rootBackfillJobName,
    queueName: rootBackfillQueueName,
    data: { fid: rootFid, backfillCount: backfillFids.length },
    children: backfillFids.map((fid) => {
      return {
        queueName: backfillQueueName,
        name: backfillJobName,
        data: { fid, ...childJobOptions },
        opts: {
          jobId: getBackfillJobId(fid),
          priority: 100,
        },
      }
    }),
    opts: {
      jobId: getRootBackfillJobId(rootFid),
    },
  })

  return flow
}

// TODO: Consider separate queue for partial backfills
export async function queueBackfillJob(
  fid: number,
  queue: Queue<BackfillJob>,
  {
    priority = 100,
    partial,
    removeMessages,
  }: { priority?: number } & Omit<BackfillJob, 'fid'> = {}
) {
  const job = await queue.add(
    backfillJobName,
    { fid, partial, removeMessages },
    {
      jobId: partial ? getBackfillPartialJobId(fid) : getBackfillJobId(fid),
      priority,
    }
  )
  // log.info(
  //   `Queued backfill job for FID ${fid}: ${job.id} ${partial ? '(partial)' : ''}`
  // )
  return job
}

async function handleBackfillJob(job: Job<BackfillJob>) {
  const { fid, partial, removeMessages } = job.data

  let startTime = Date.now()

  const isAlreadyTarget = await isTarget(fid)

  if (!partial) addTarget(fid)

  await backfillFid(fid, {
    partial,
    hubOptions: { includeRemoveMessages: removeMessages },
  }).catch((err) => {
    // Undo the target if the backfill fails and it wasn't already a target
    if (!isAlreadyTarget) removeTarget(fid)

    log.error(err, `Error backfilling FID ${fid}`)
    throw err
  })

  log.info(
    `Backfill complete for FID ${fid}${partial ? ' (partial) ' : ''}${removeMessages ? ' (remove messages) ' : ''}in ${(Date.now() - startTime) / 1000}s`
  )
}

async function backfillFid(
  fid: number,
  {
    partial,
    hubOptions,
  }: { partial?: boolean; hubOptions?: ExtraHubOptions } = {}
) {
  if (partial) {
    // Partial backfill only gets user data
    const userData = await hubClient.getUserDataByFid({ fid })
    await insertUserDatas(checkMessages(userData, fid))
    return
  }

  const p = await getFullProfileFromHub(fid, hubOptions).catch((err) => {
    log.error(err, `Error getting profile for FID ${fid}`)
    return null
  })

  if (!p) return

  await insertCasts(p.casts)
  await insertLinks(p.links)
  await insertReactions(p.reactions)
  await insertUserDatas(p.userData)
  await insertVerifications(p.verifications)

  await insertRegistrations(await p.registrations)
  await insertSigners(await p.signers)
  await insertStorage(await p.storage)

  /** Also process deletes in case they were missed by syncing - only necessary after first backfill */
  // TODO: Only process deletes from when syncing was down
  if (hubOptions?.includeRemoveMessages) {
    await deleteCasts(p.casts)
    // await deleteReactions(p.reactions) // too slow to be worth it
    await deleteLinks(p.links)
    await deleteVerifications(p.verifications)
  }
}
