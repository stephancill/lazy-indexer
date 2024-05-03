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
  const _concurrency = opts?.concurrency || 1

  return new Worker<T>(name, jobHandler, {
    ...bullMqOptions,
    useWorkerThreads: _concurrency > 1,
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 100 },
    concurrency: _concurrency,
  })
}
