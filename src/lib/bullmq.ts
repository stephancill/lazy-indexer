import { Job, Queue, QueueOptions, Worker } from 'bullmq'

import { log } from './logger.js'
import { redis } from './redis.js'

const bullMqOptions: QueueOptions = {
  connection: redis,
}

export function createQueue<T>(name: string, opts?: Partial<QueueOptions>) {
  return new Queue<T>(name, { ...bullMqOptions, ...opts })
}

export function createWorker<T>(
  name: string,
  jobHandler: (job: Job) => Promise<void>,
  opts?: WorkerOptions & {
    concurrency?: number
  }
) {
  const concurrency =
    opts?.concurrency || Number(process.env.WORKER_CONCURRENCY || 5)

  log.info(`Creating worker ${name} with concurrency ${concurrency}`)

  return new Worker<T>(name, jobHandler, {
    ...bullMqOptions,
    useWorkerThreads: concurrency > 1,
    autorun: false,
    ...opts,
  })
}
