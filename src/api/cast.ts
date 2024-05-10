import { Message } from '@farcaster/hub-nodejs'

import { db } from '../db/kysely.js'
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
  const casts = formatCasts(msgs)
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
}
/**
 * Soft delete casts in the database
 * @param msgs Raw hub messages
 */
export async function deleteCasts(msgs: Message[]) {
  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of msgs) {
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
          .where('hash', '=', data.castAddBody!.parentCastId!.hash)
          .execute()
      }
    })

    log.debug(`CASTS PRUNED`)
  } catch (error) {
    log.error(error, 'ERROR PRUNING CAST')
    throw error
  }
}
