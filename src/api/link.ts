import { Message } from '@farcaster/hub-nodejs'

import { db } from '../db/kysely.js'
import { getBackfillQueue, queueBackfillJob } from '../lib/backfill.js'
import { log } from '../lib/logger.js'
import { isRootTarget } from '../lib/targets.js'
import {
  breakIntoChunks,
  farcasterTimeToDate,
  formatLinks,
} from '../lib/utils.js'

export async function insertLinks(msgs: Message[]) {
  const links = formatLinks(msgs)
  if (links.length === 0) return
  const chunks = breakIntoChunks(links, 1000)

  for (const chunk of chunks) {
    try {
      await db
        .insertInto('links')
        .values(chunk)
        .onConflict((oc) => oc.column('hash').doNothing())
        .execute()

      log.debug(`LINKS INSERTED`)
    } catch (error) {
      log.error(error, 'ERROR INSERTING LINKS')
      throw error
    }
  }

  const queue = getBackfillQueue()

  // Check if message fid is a root target, if so, queue a backfill job for link target
  for (const msg of msgs) {
    const data = msg.data!
    if (data.linkBody?.targetFid) {
      if (await isRootTarget(data.fid)) {
        queueBackfillJob(data.linkBody.targetFid, queue)
      }
    }
  }
}

export async function deleteLinks(msgs: Message[]) {
  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of msgs) {
        const data = msg.data!

        await trx
          .updateTable('links')
          .set({
            deletedAt: farcasterTimeToDate(data.timestamp),
          })
          .where('fid', '=', data.fid)
          .where('targetFid', '=', data.linkBody!.targetFid!)
          .execute()
      }
    })

    log.debug(`LINKS DELETED`)
  } catch (error) {
    log.error(error, 'ERROR DELETING LINKS')
    throw error
  }
}

export async function pruneLinks(msgs: Message[]) {
  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of msgs) {
        const data = msg.data!

        await trx
          .updateTable('links')
          .set({
            prunedAt: farcasterTimeToDate(data.timestamp),
          })
          .where('fid', '=', data.fid)
          .where('targetFid', '=', data.linkBody!.targetFid!)
          .execute()
      }
    })

    log.debug(`LINKS PRUNED`)
  } catch (error) {
    log.error(error, 'ERROR PRUNING LINKS')
    throw error
  }
}
