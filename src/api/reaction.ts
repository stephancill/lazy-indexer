import {
  Message,
  isReactionAddMessage,
  isReactionRemoveMessage,
} from '@farcaster/hub-nodejs'

import { db } from '../db/kysely.js'
import { log } from '../lib/logger.js'
import {
  breakIntoChunks,
  farcasterTimeToDate,
  formatReactions,
} from '../lib/utils.js'

/**
 * Insert a reaction in the database
 * @param msg Hub event in JSON format
 */
export async function insertReactions(msgs: Message[]) {
  const reactionAddMessages = msgs.filter(isReactionAddMessage)

  const reactions = formatReactions(reactionAddMessages)
  if (reactions.length === 0) return
  const chunks = breakIntoChunks(reactions, 1000)

  for (const chunk of chunks) {
    try {
      await db
        .insertInto('reactions')
        .values(chunk)
        .onConflict((oc) => oc.column('hash').doNothing())
        .execute()

      log.debug(`REACTIONS INSERTED`)
    } catch (error) {
      log.error(error, 'ERROR INSERTING REACTIONS')
      throw error
    }
  }
}

export async function deleteReactions(msgs: Message[]) {
  const removeReactionMessages = msgs.filter(isReactionRemoveMessage)

  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of removeReactionMessages) {
        const data = msg.data!
        const reaction = data.reactionBody!

        if (reaction.targetCastId) {
          await trx
            .updateTable('reactions')
            .set({ deletedAt: farcasterTimeToDate(data.timestamp) })
            .where('fid', '=', data.fid)
            .where('type', '=', reaction.type)
            .where('targetCastHash', '=', reaction.targetCastId.hash)
            .execute()
        } else if (reaction.targetUrl) {
          await trx
            .updateTable('reactions')
            .set({ deletedAt: farcasterTimeToDate(data.timestamp) })
            .where('fid', '=', data.fid)
            .where('type', '=', reaction.type)
            .where('targetUrl', '=', reaction.targetUrl)
            .execute()
        }
      }
    })

    log.debug(`REACTIONS DELETED`)
  } catch (error) {
    log.error(error, 'ERROR DELETING REACTIONS')
    throw error
  }
}

export async function pruneReactions(msgs: Message[]) {
  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of msgs) {
        const data = msg.data!
        const reaction = data.reactionBody!

        if (reaction.targetCastId) {
          await trx
            .updateTable('reactions')
            .set({ prunedAt: farcasterTimeToDate(data.timestamp) })
            .where('fid', '=', data.fid)
            .where('type', '=', reaction.type)
            .where('targetCastHash', '=', reaction.targetCastId.hash)
            .execute()
        } else if (reaction.targetUrl) {
          await trx
            .updateTable('reactions')
            .set({ prunedAt: farcasterTimeToDate(data.timestamp) })
            .where('fid', '=', data.fid)
            .where('type', '=', reaction.type)
            .where('targetUrl', '=', reaction.targetUrl)
            .execute()
        }
      }
    })

    log.debug(`REACTIONS PRUNED`)
  } catch (error) {
    log.error(error, 'ERROR PRUNING REACTIONS')
    throw error
  }
}
