import { Job, Queue, QueueOptions, Worker } from 'bullmq'

import { redis } from './redis.js'

const bullMqOptions: QueueOptions = {
  connection: redis,
  prefix: 'hub',
}

export function createQueue<T>(name: string) {
  return new Queue<T>(name, bullMqOptions)
}

export function createWorker<T>(
  name: string,
  jobHandler: (job: Job) => Promise<void>,
  opts?: {
    concurrency?: number
  }
) {
  const concurrency =
    opts?.concurrency || Number(process.env.WORKER_CONCURRENCY || 5)

  return new Worker<T>(name, jobHandler, {
    ...bullMqOptions,
    useWorkerThreads: concurrency > 1,
    removeOnComplete: { count: 100 },
    concurrency,
  })
}
