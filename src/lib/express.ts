import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'
import { ExpressAdapter } from '@bull-board/express'
import express from 'express'
import humanizeDuration from 'humanize-duration'

import {
  getBackfillJobId,
  getBackfillQueue,
  getIndexMessageQueue,
  getRootBackfillJobId,
  getRootBackfillPlaceholderJobId,
  getRootBackfillQueue,
  handleRootBackfillJob,
  queueBackfillJob,
  queueRootBackfillJob,
} from './backfill.js'
import { log } from './logger.js'

export function initExpressApp() {
  const port = process.env.PORT || 3005
  const app = express()
  const serverAdapter = new ExpressAdapter()
  const backfillQueue = getBackfillQueue()
  const rootBackfillQueue = getRootBackfillQueue()
  const indexMessageQueue = getIndexMessageQueue()

  async function getRootBackfillStatus(fid: number) {
    const placeholderJobId = getRootBackfillPlaceholderJobId(fid)
    const placeholderJob = await rootBackfillQueue.getJob(placeholderJobId)

    if (!placeholderJob) {
      return null
    }

    const allWaiting = await backfillQueue.getJobs(['waiting', 'prioritized'])
    const waitingChildren = allWaiting.filter(
      (job) => job.parent?.id === placeholderJobId
    )

    const startTime = placeholderJob.timestamp
    const childCount = placeholderJob.data.backfillCount!
    const completedCount = childCount - waitingChildren.length
    const averageTimePerChild = (Date.now() - startTime) / completedCount
    const estimatedTimeRemaining = averageTimePerChild * waitingChildren.length
    const etaString = `${humanizeDuration(estimatedTimeRemaining, {
      round: true,
    })} remaining`
    const isDone = waitingChildren.length === 0

    return {
      status: !isDone ? etaString : 'Done',
      completedCount,
      waitingCount: waitingChildren.length,
      childCount,
      done: isDone,
      elapsed: humanizeDuration(
        (placeholderJob.finishedOn || Date.now()) - startTime,
        {
          round: true,
        }
      ),
    }
  }

  app.listen(port, () => {
    log.info('Server started on http://localhost:' + port)
  })

  serverAdapter.setBasePath('/')
  app.use('/', serverAdapter.getRouter())

  app.post('/root-backfill/:fid', async (req, res) => {
    const { fid: fidRaw } = req.params
    const { force, initial } = req.query
    const fid = parseInt(fidRaw)

    const rootBackfillJobPlaceholderId = getRootBackfillPlaceholderJobId(fid)
    const rootBackfillJobId = getRootBackfillJobId(fid)

    const job = await rootBackfillQueue.getJob(rootBackfillJobPlaceholderId)
    const rootBackfillJob = await rootBackfillQueue.getJob(rootBackfillJobId)

    if (job && !force) {
      log.info(`Root backfill job already exists for FID ${fid}`)
      const jobStatus = await getRootBackfillStatus(fid)
      return res.status(409).json(jobStatus)
    }

    if (job) {
      log.info(`Removing existing root backfill job for FID ${fid}`)
      try {
        await rootBackfillQueue.remove(getRootBackfillJobId(fid))
        await job.remove({ removeChildren: true })
      } catch (error) {
        log.error(error)
        return res.status(500).json({
          error:
            error instanceof Error ? error.message : 'Something went wrong',
        })
      }
    }

    log.info(`Creating root backfill job for FID ${fid}`)

    const jobNode = await queueRootBackfillJob(fid, rootBackfillQueue, {
      childOptions: {
        // TODO: detect if first backfill and remove messages
        removeMessages: initial !== 'true',
      },
    })

    return res
      .status(200)
      .json({ jobId: rootBackfillJobPlaceholderId, jobNode })
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

    job = await queueBackfillJob(fid, backfillQueue, { priority: 1 })

    return res.status(200).json(job)
  })

  createBullBoard({
    queues: [
      new BullMQAdapter(backfillQueue),
      new BullMQAdapter(rootBackfillQueue),
      new BullMQAdapter(indexMessageQueue),
    ],
    serverAdapter,
  })
}
