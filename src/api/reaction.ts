import { Message, fromFarcasterTime } from '@farcaster/hub-nodejs'

import { db } from '../db/kysely.js'
import { log } from '../lib/logger.js'
import { breakIntoChunks, formatReactions } from '../lib/utils.js'

/**
 * Insert a reaction in the database
 * @param msg Hub event in JSON format
 */
export async function insertReactions(msgs: Message[]) {
  const reactions = formatReactions(msgs)
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
  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of msgs) {
        const data = msg.data!
        const reaction = data.reactionBody!

        if (reaction.targetCastId) {
          await trx
            .updateTable('reactions')
            .set({
              deletedAt: new Date(
                fromFarcasterTime(data.timestamp)._unsafeUnwrap()
              ),
            })
            .where('fid', '=', data.fid)
            .where('type', '=', reaction.type)
            .where('targetCastHash', '=', reaction.targetCastId.hash)
            .execute()
        } else if (reaction.targetUrl) {
          await trx
            .updateTable('reactions')
            .set({
              deletedAt: new Date(
                fromFarcasterTime(data.timestamp)._unsafeUnwrap()
              ),
            })
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
