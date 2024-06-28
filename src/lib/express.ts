import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'
import { ExpressAdapter } from '@bull-board/express'
import { extractEventTimestamp } from '@farcaster/hub-nodejs'
import express from 'express'
import humanizeDuration from 'humanize-duration'

import { getLatestEvent } from '../api/event.js'
import {
  createRootBackfillJob,
  getBackfillJobId,
  getBackfillQueue,
  getRootBackfillJobId,
  getRootBackfillQueue,
  queueBackfillJob,
} from './backfill.js'
import { log } from './logger.js'

export function initExpressApp() {
  const port = process.env.PORT || 3005
  const app = express()
  const serverAdapter = new ExpressAdapter()
  const backfillQueue = getBackfillQueue()
  const rootBackfillQueue = getRootBackfillQueue()

  async function getRootBackfillStatus(fid: number) {
    const rootBackfillJobId = getRootBackfillJobId(fid)
    const job = await rootBackfillQueue.getJob(rootBackfillJobId)

    if (!job) {
      return null
    }

    const allWaiting = await backfillQueue.getJobs(['waiting', 'prioritized'])
    const waitingChildren = allWaiting.filter(
      (job) => job.parent?.id === rootBackfillJobId
    )

    const startTime = job.timestamp
    const childCount = job.data.backfillCount
    const completedCount = childCount - waitingChildren.length
    const averageTimePerChild = (Date.now() - startTime) / completedCount
    const estimatedTimeRemaining = averageTimePerChild * waitingChildren.length
    const etaString = humanizeDuration(estimatedTimeRemaining, {
      round: true,
    })
    const isDone = waitingChildren.length === 0

    return {
      status: !isDone ? etaString : 'Done',
      completedCount,
      waitingCount: waitingChildren.length,
      childCount,
      done: isDone,
    }
  }

  app.listen(port, () => {
    log.info('Server started on http://localhost:' + port)
  })

  serverAdapter.setBasePath('/')
  app.use('/', serverAdapter.getRouter())

  app.get('/stats', async (req, res) => {
    let latestEventTimestamp
    const latestEventId = await getLatestEvent()

    const isBackfillActive = (await backfillQueue.getActiveCount()) > 0

    if (latestEventId) {
      latestEventTimestamp = extractEventTimestamp(latestEventId)
    }

    return res
      .status(200)
      .json({ latestEventId, latestEventTimestamp, isBackfillActive })
  })

  app.post('/root-backfill/:fid', async (req, res) => {
    const { fid: fidRaw } = req.params
    const { force } = req.query
    const fid = parseInt(fidRaw)

    const rootBackfillJobId = getRootBackfillJobId(fid)

    const job = await rootBackfillQueue.getJob(rootBackfillJobId)

    if (job && !force) {
      log.info(`Root backfill job already exists for FID ${fid}`)
      const jobStatus = await getRootBackfillStatus(fid)
      return res.status(409).json(jobStatus)
    }

    if (job) {
      log.info(`Removing existing root backfill job for FID ${fid}`)
      await job.remove({ removeChildren: true })
    }

    log.info(`Creating root backfill job for FID ${fid}`)

    const jobNode = await createRootBackfillJob(fid)

    return res.status(200).json({ jobId: rootBackfillJobId, jobNode })
  })

  app.get('/root-backfill/:fid', async (req, res) => {
    const { fid: fidRaw } = req.params
    const fid = parseInt(fidRaw)

    const jobStatus = await getRootBackfillStatus(fid)

    if (!jobStatus) {
      return res.status(404).json({ error: 'Job not found' })
    }

    return res.status(200).json(jobStatus)
  })

  app.post('/backfill/:fid', async (req, res) => {
    const { fid: fidRaw } = req.params
    const { force } = req.query
    const fid = parseInt(fidRaw)

    const backfillJobId = getBackfillJobId(fid)
    let job = await backfillQueue.getJob(backfillJobId)

    if (job && !force) {
      log.info(`Backfill job already exists for FID ${fid}`)
      return res.status(409).json({ job })
    }

    if (job) {
      log.info(`Removing existing backfill job for FID ${fid}`)
      await job.remove()
    }

    job = await queueBackfillJob(fid, backfillQueue, 1)

    return res.status(200).json(job)
  })

  createBullBoard({
    queues: [
      new BullMQAdapter(backfillQueue),
      new BullMQAdapter(rootBackfillQueue),
    ],
    serverAdapter,
  })
}
