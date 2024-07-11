import { MessageType } from '@farcaster/hub-nodejs'
import { FlowProducer, Job, Queue } from 'bullmq'
import { hexToBytes } from 'viem'

import { deleteCasts, insertCasts } from '../api/cast.js'
import { makeLatestEventId, saveLatestEventId } from '../api/event.js'
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
import { createQueue, createWorker } from './jobs.js'
import { getNetworkByFid } from './links-utils.js'
import { ExtraHubOptions } from './paginate.js'
import { redis } from './redis.js'
import { addRootTarget, addTarget, isTarget, removeTarget } from './targets.js'

type IndexMessageJob = {
  hash: `0x${string}`
  fid: number
  type: MessageType
  jobType: 'indexMessage'
}

type BackfillJob = {
  fid: number
  partial?: boolean
  removeMessages?: boolean
  jobType: 'backfill'
}

type QueueJob = BackfillJob | IndexMessageJob

type RootBackfillJob = {
  fid: number
  backfillCount?: number
  placeholder?: boolean
  childOptions?: Omit<BackfillJob, 'fid' | 'jobType'>
}

const backfillQueueName = 'backfill'
const backfillJobName = 'backfill'
export const getBackfillQueue = () => createQueue<QueueJob>(backfillQueueName)
export const getBackfillWorker = () =>
  createWorker<QueueJob>(backfillQueueName, handleQueueJob)

const rootBackfillQueueName = 'rootBackfill'
export const rootBackfillJobName = 'rootBackfill'
export const getRootBackfillQueue = () =>
  createQueue<RootBackfillJob>(rootBackfillQueueName)
export const getRootBackfillWorker = () =>
  createWorker<RootBackfillJob>(rootBackfillQueueName, async (job) => {
    if (job.data.placeholder) {
      log.info(`Completed root backfill job for FID ${job.data.fid}`)
      return
    }
    await handleRootBackfillJob(job.data.fid)
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

export function getRootBackfillPlaceholderJobId(fid: number) {
  return `backfill:root:${fid}:placeholder`
}

export function getIndexMessageJobId(hash: string, type: MessageType) {
  return `indexMessage:${type}:${hash}`
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

export async function handleRootBackfillJob(
  rootFid: number,
  childJobOptions: Omit<BackfillJob, 'fid' | 'jobType'> = {}
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
    data: {
      fid: rootFid,
      backfillCount: backfillFids.length,
      placeholder: true,
    },
    children: backfillFids.map((fid) => {
      return {
        queueName: backfillQueueName,
        name: backfillJobName,
        data: { fid, ...childJobOptions, jobType: 'backfill' },
        opts: {
          jobId: getBackfillJobId(fid),
          priority: 100,
        },
      }
    }),
    opts: {
      jobId: getRootBackfillPlaceholderJobId(rootFid),
    },
  })

  return flow
}

export async function queueIndexMessageJob(
  hash: `0x${string}`,
  fid: number,
  type: MessageType,
  options?: { priority?: number }
) {
  const queue = getBackfillQueue()

  const job = queue.add(
    backfillJobName,
    { hash, type, fid, jobType: 'indexMessage' },
    { jobId: getIndexMessageJobId(hash, type), priority: options?.priority }
  )
  log.debug(`Queued index message job for hash ${hash}`)
  return job
}

// TODO: Consider separate queue for partial backfills
export async function queueBackfillJob(
  fid: number,
  {
    priority = 100,
    partial,
    removeMessages,
  }: { priority?: number } & Omit<BackfillJob, 'fid' | 'jobType'> = {}
) {
  const queue = getBackfillQueue()

  const job = await queue.add(
    backfillJobName,
    { fid, partial, removeMessages, jobType: 'backfill' },
    {
      jobId: partial ? getBackfillPartialJobId(fid) : getBackfillJobId(fid),
      priority,
    }
  )
  log.debug(
    `Queued backfill job for FID ${fid}: ${job.id} ${partial ? '(partial)' : ''}`
  )
  return job
}

export async function queueRootBackfillJob(
  fid: number,
  options?: Omit<RootBackfillJob, 'fid' | 'jobType'>
) {
  const queue = getRootBackfillQueue()

  const job = await queue.add(
    rootBackfillJobName,
    { fid, ...options },
    { jobId: getRootBackfillJobId(fid) }
  )
  log.info(`Queued root backfill job for FID ${fid}: ${job.id}`)
  return job
}

async function handleQueueJob(job: Job<QueueJob>) {
  if (job.data.jobType === 'backfill') {
    return handleBackfillJob(job as Job<BackfillJob>)
  } else if (job.data.jobType === 'indexMessage') {
    return handleIndexMessageJob(job as Job<IndexMessageJob>)
  }
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

async function handleIndexMessageJob(job: Job<IndexMessageJob>) {
  const { hash: hashString, fid, type } = job.data
  const hash = hexToBytes(hashString)

  switch (type) {
    case MessageType.CAST_ADD: {
      const castResult = await hubClient.getCast({ hash, fid })
      if (castResult.isErr()) throw new Error(castResult.error.message)
      await insertCasts([castResult.value])
      log.debug(`Indexed cast ${fid}:${hashString}`)
      break
    }
    default:
      log.warn(`Indexing message type ${type} not yet implemented`)
  }
}
