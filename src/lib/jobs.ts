import { Job, Queue, QueueOptions, Worker, WorkerOptions } from 'bullmq'

import { log } from './logger.js'
import { redis } from './redis.js'

const workerOptions: WorkerOptions = {
  connection: redis,
}

const queueOptions: QueueOptions = {
  connection: redis,
  defaultJobOptions: {
    attempts: 3,
    removeOnComplete: true,
    priority: 100,
  },
}

export function createQueue<T>(name: string, opts?: Partial<QueueOptions>) {
  return new Queue<T>(name, {
    ...queueOptions,
    ...opts,
  })
}

export function createWorker<T>(
  name: string,
  jobHandler: (job: Job) => Promise<void>,
  opts?: Partial<WorkerOptions> & {
    concurrency?: number
  }
) {
  const concurrency =
    opts?.concurrency || Number(process.env.WORKER_CONCURRENCY || 5)

  log.info(`Creating worker ${name} with concurrency ${concurrency}`)

  return new Worker<T>(name, jobHandler, {
    ...workerOptions,
    useWorkerThreads: concurrency > 1,
    concurrency,
    autorun: false,
    ...opts,
  })
}
