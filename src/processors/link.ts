import {
  Message,
  isLinkAddMessage,
  isLinkRemoveMessage,
} from '@farcaster/hub-nodejs'

import { db } from '../db/kysely.js'
import { getBackfillQueue, queueBackfillJob } from '../lib/backfill.js'
import { log } from '../lib/logger.js'
import { isRootTarget } from '../lib/targets.js'
import { CallSource } from '../lib/types.js'
import {
  breakIntoChunks,
  farcasterTimeToDate,
  formatLinks,
} from '../lib/utils.js'

export async function insertLinks(
  msgs: Message[],
  source: CallSource = 'backfill'
) {
  const addLinkMessages = msgs.filter(isLinkAddMessage)

  const links = formatLinks(addLinkMessages)
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
  if (source === 'stream') {
    for (const msg of msgs) {
      const data = msg.data!
      if (data.linkBody?.targetFid && (await isRootTarget(data.fid))) {
        queueBackfillJob(data.linkBody.targetFid, queue)
      }
    }
  }
}

export async function deleteLinks(msgs: Message[]) {
  const removeLinkMessages = msgs.filter(isLinkRemoveMessage)

  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of removeLinkMessages) {
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
