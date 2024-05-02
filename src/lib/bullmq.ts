import { ExpressAdapter } from '@bull-board/express'
import { Job, Queue, QueueOptions, Worker } from 'bullmq'
import express from 'express'

import { log } from './logger.js'
import { redis } from './redis.js'

const concurrency = Number(process.env.BACKFILL_CONCURRENCY || 5)

const options: QueueOptions = {
  connection: redis,
  prefix: 'hub',
}

export function createQueue<T>(name: string) {
  return new Queue<T>(name, options)
}

export function createWorker<T>(
  name: string,
  jobHandler: (job: Job) => Promise<void>
) {
  return new Worker<T>(name, jobHandler, {
    ...options,
    useWorkerThreads: concurrency > 1,
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 100 },
    concurrency,
  })
}

const app = express()
export const serverAdapter = new ExpressAdapter()

serverAdapter.setBasePath('/')
app.use('/', serverAdapter.getRouter())

app.listen(3001, () => {
  log.info('Server started on http://localhost:3001')
})
