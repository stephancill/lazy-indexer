import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'
import { ExpressAdapter } from '@bull-board/express'
import { extractEventTimestamp } from '@farcaster/hub-nodejs'
import express from 'express'

import { getLatestEvent } from '../api/event.js'
import {
  createRootBackfillJob,
  getBackfillQueue,
  getRootBackfillJobId,
  getRootBackfillQueue,
  queueBackfillJob,
} from './backfill.js'
import { log } from './logger.js'

export function initExpressApp() {
  const app = express()
  const serverAdapter = new ExpressAdapter()
  const backfillQueue = getBackfillQueue()
  const rootBackfillQueue = getRootBackfillQueue()

  const port = process.env.PORT || 3005

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
    const fid = parseInt(fidRaw)

    const rootBackfillJobId = getRootBackfillJobId(fid)

    const job = await rootBackfillQueue.getJob(rootBackfillJobId)

    if (job) {
      return res.status(409).json({ job })
    }

    const jobNode = await createRootBackfillJob(fid)

    return res.status(200).json({ jobId: rootBackfillJobId, jobNode })
  })

  app.get('/root-backfill/:fid', async (req, res) => {
    const { fid: fidRaw } = req.params
    const fid = parseInt(fidRaw)

    const rootBackfillJobId = getRootBackfillJobId(fid)
    const job = await rootBackfillQueue.getJob(rootBackfillJobId)

    if (!job) {
      return res.status(404).json({ error: 'Job not found' })
    }

    const allWaiting = await backfillQueue.getWaiting()
    const waitingChildren = allWaiting.filter(
      (job) => job.parent?.id === rootBackfillJobId
    )

    return res.status(200).json({ job, childCount: waitingChildren.length })
  })

  app.post('/backfill/:fid', async (req, res) => {
    const { fid: fidRaw } = req.params
    const fid = parseInt(fidRaw)

    const job = await queueBackfillJob(fid, backfillQueue, 1)

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
