import { FlowProducer, Job, Queue } from 'bullmq'

import { insertCasts } from '../api/cast.js'
import { saveLatestEventId } from '../api/event.js'
import { insertRegistrations } from '../api/fid.js'
import { insertLinks } from '../api/link.js'
import { insertReactions } from '../api/reaction.js'
import { insertSigners } from '../api/signer.js'
import { insertStorage } from '../api/storage.js'
import { insertUserDatas } from '../api/user-data.js'
import { insertVerifications } from '../api/verification.js'
import { createQueue, createWorker } from '../lib/bullmq.js'
import { hubClient } from '../lib/hub-client.js'
import { log } from '../lib/logger.js'
import { getFullProfileFromHub } from '../lib/utils.js'
import { makeLatestEventId } from './event.js'
import { getNetworkByFid } from './links-utils.js'
import { redis } from './redis.js'
import { addRootTarget, addTarget, isTarget, removeTarget } from './targets.js'

type BackfillJob = {
  fid: number
}

type RootBackfillJob = {
  fid: number
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
  createQueue<RootBackfillJob>(rootBackfillQueueName, {
    defaultJobOptions: {
      removeOnComplete: false,
    },
  })
export const getRootBackfillWorker = () =>
  createWorker<RootBackfillJob>(rootBackfillQueueName, async (job) => {
    log.info(`Completed root backfill job for FID ${job.data.fid}`)
  })

const flowProducer = new FlowProducer({ connection: redis })

function getBackfillJobId(fid: number) {
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

  log.info('Starting backfill')

  // Save the latest event ID so we can subscribe from there after backfill completes
  const latestEventId = makeLatestEventId()
  await saveLatestEventId(latestEventId)
  // await getHubs()
  // await getDbInfo()
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

export async function createRootBackfillJob(rootFid: number) {
  const {
    linksByDepth: { ['1']: linksSet },
  } = await getNetworkByFid(rootFid, {
    onProgress(message) {
      log.info(message)
    },
  })

  log.info(`Found ${linksSet.size} links for FID ${rootFid}`)

  addRootTarget(rootFid)

  const links = [rootFid, ...Array.from(linksSet)]

  const flow = await flowProducer.add({
    name: rootBackfillJobName,
    queueName: rootBackfillQueueName,
    data: { fid: rootFid, links },
    children: links.map((fid) => ({
      queueName: backfillQueueName,
      name: backfillJobName,
      data: { fid },
      opts: {
        jobId: getBackfillJobId(rootFid),
      },
    })),
    opts: {
      jobId: getRootBackfillJobId(rootFid),
    },
  })

  return flow
}

export async function queueBackfillJob(
  fid: number,
  queue: Queue<BackfillJob>,
  priority: number | undefined = 100
) {
  const job = await queue.add(
    backfillJobName,
    { fid },
    { jobId: getBackfillJobId(fid), priority }
  )
  log.info(`Queued backfill job for FID ${fid}: ${job.id}`)
  return job
}

async function handleBackfillJob(job: Job<BackfillJob>) {
  const { fid } = job.data

  let startTime = Date.now()

  const isAlreadyTarget = await isTarget(fid)
  addTarget(fid)

  await backfillFid(fid).catch((err) => {
    // Undo the target if the backfill fails and it wasn't already a target
    if (!isAlreadyTarget) removeTarget(fid)

    log.error(err, `Error backfilling FID ${fid}`)
    throw err
  })

  log.info(
    `Backfill complete up to FID ${fid} in ${(Date.now() - startTime) / 1000}s`
  )
}

async function backfillFid(fid: number) {
  const p = await getFullProfileFromHub(fid).catch((err) => {
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
}
