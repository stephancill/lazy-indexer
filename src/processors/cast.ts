import {
  Message,
  isCastAddMessage,
  isCastRemoveMessage,
} from '@farcaster/hub-nodejs'

import { db } from '../db/kysely.js'
import { getBackfillQueue, queueBackfillJob } from '../lib/backfill.js'
import { log } from '../lib/logger.js'
import {
  breakIntoChunks,
  farcasterTimeToDate,
  formatCasts,
} from '../lib/utils.js'

/**
 * Insert casts in the database
 * @param msgs Raw hub messages
 */
export async function insertCasts(msgs: Message[]) {
  const castAddMessages = msgs.filter(isCastAddMessage)

  const casts = formatCasts(castAddMessages)
  if (casts.length === 0) return
  const chunks = breakIntoChunks(casts, 1000)

  for (const chunk of chunks) {
    try {
      await db
        .insertInto('casts')
        .values(chunk)
        .onConflict((oc) => oc.column('hash').doNothing())
        .execute()

      log.debug(`CASTS INSERTED`)
    } catch (error) {
      log.error(error, 'ERROR INSERTING CAST')
      throw error
    }
  }

  if (casts.length > 0) {
    // Create mention partial backfill jobs - only for recent casts
    // TODO: Configurable cutoff timestamp
    const cutoffTimestamp = Date.now() - 1000 * 60 * 60 * 24 * 7 // 7 days ago
    const queue = getBackfillQueue()
    const mentionedFids = casts
      .filter((c) => c.timestamp.getTime() > cutoffTimestamp)
      .map((c) => c.mentions)
      .map((mentionsArray) => JSON.parse(mentionsArray) as number[])
      .flat()

    // TODO: How do we keep these up to date?
    mentionedFids.forEach((fid) =>
      queueBackfillJob(fid, queue, { partial: true, priority: 110 })
    )
  }
}
/**
 * Soft delete casts in the database
 * @param msgs Raw hub messages
 */
export async function deleteCasts(msgs: Message[]) {
  const castRemoveMessages = msgs.filter((msg) => isCastRemoveMessage(msg))

  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of castRemoveMessages) {
        const data = msg.data!

        await trx
          .updateTable('casts')
          .set({
            deletedAt: farcasterTimeToDate(data.timestamp),
          })
          .where('hash', '=', data.castRemoveBody?.targetHash!)
          .execute()
      }
    })

    log.debug(`CASTS DELETED`)
  } catch (error) {
    log.error(error, 'ERROR DELETING CAST')
    throw error
  }
}

/**
 * Soft prune casts in the database
 * @param msgs Raw hub messages
 */
export async function pruneCasts(msgs: Message[]) {
  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of msgs) {
        const data = msg.data!

        await trx
          .updateTable('casts')
          .set({
            prunedAt: farcasterTimeToDate(data.timestamp),
          })
          .where('fid', '=', data.fid)
          .where('text', '=', data.castAddBody!.text)
          .execute()
      }
    })

    log.debug(`CASTS PRUNED`)
  } catch (error) {
    log.error(error, 'ERROR PRUNING CAST')
    throw error
  }
}
