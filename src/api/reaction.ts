import {
  Message,
  MessageType,
  isReactionAddMessage,
  isReactionRemoveMessage,
} from '@farcaster/hub-nodejs'
import { bytesToHex } from 'viem'

import { db } from '../db/kysely.js'
import { queueIndexMessageJob } from '../lib/backfill.js'
import { log } from '../lib/logger.js'
import { redis } from '../lib/redis.js'
import { allTargetsKey } from '../lib/targets.js'
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

  if (reactions.length > 0) {
    // Create mention partial backfill jobs - only for recent casts
    // TODO: Configurable cutoff timestamp
    const cutoffTimestamp = Date.now() - 1000 * 60 * 60 * 24 * 1 // 1 day ago
    const recentReactions = reactions.filter(
      (r) => r.timestamp.getTime() > cutoffTimestamp
    )

    // Check if target fid is not in targets set
    recentReactions
      .filter((r) => r.timestamp.getTime() > cutoffTimestamp)
      .forEach(async (reaction) => {
        if (!reaction.targetCastHash || !reaction.targetCastFid) return
        if (await redis.sismember(allTargetsKey, reaction.targetCastFid)) return

        queueIndexMessageJob(
          bytesToHex(reaction.targetCastHash),
          reaction.targetCastFid,
          MessageType.CAST_ADD,
          { priority: 110 }
        )
      })
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
