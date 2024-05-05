import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'
import { ExpressAdapter } from '@bull-board/express'
import express from 'express'

import { getLatestEvent } from '../api/event.js'
import { backfillQueue } from './backfill.js'
import { log } from './logger.js'
import { streamQueue } from './subscriber.js'

export function initExpressApp() {
  const app = express()
  const serverAdapter = new ExpressAdapter()

  app.listen(3001, () => {
    log.info('Server started on http://localhost:3001')
  })

  serverAdapter.setBasePath('/')
  app.use('/', serverAdapter.getRouter())

  app.get('/stats', async (req, res) => {
    const latestEventId = await getLatestEvent()
    const isBackfillActive = (await backfillQueue.getActiveCount()) > 0

    return res.status(200).json({ latestEventId, isBackfillActive })
  })

  createBullBoard({
    queues: [new BullMQAdapter(backfillQueue), new BullMQAdapter(streamQueue)],
    serverAdapter,
  })
}
