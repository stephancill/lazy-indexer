import {
  FARCASTER_EPOCH,
  HubEventType,
  Message,
  MessageType,
} from '@farcaster/hub-nodejs'
import { Job } from 'bullmq'

import { deleteCasts, insertCasts, pruneCasts } from '../api/cast.js'
import { deleteLinks, insertLinks, pruneLinks } from '../api/link.js'
import {
  deleteReactions,
  insertReactions,
  pruneReactions,
} from '../api/reaction.js'
import { insertUserDatas } from '../api/user-data.js'
import {
  deleteVerifications,
  insertVerifications,
} from '../api/verification.js'
import { hubClient } from './hub-client.js'
import { log } from './logger.js'

/**
 * Update the database based on the event type
 * @param job Job to add to the `stream` queue
 */
export async function handleEvents(job: Job<number[]>) {
  const events = await Promise.all(
    job.data.map(async (id) =>
      (await hubClient.getEvent({ id }))._unsafeUnwrap()
    )
  )

  const castAdds = new Array<Message>()
  const castRemoves = new Array<Message>()
  const castPrunes = new Array<Message>()
  const verificationAdds = new Array<Message>()
  const verificationRemoves = new Array<Message>()
  const userDataAdds = new Array<Message>()
  const reactionAdds = new Array<Message>()
  const reactionRemoves = new Array<Message>()
  const reactionPrunes = new Array<Message>()
  const linkAdds = new Array<Message>()
  const linkRemoves = new Array<Message>()
  const linkPrunes = new Array<Message>()

  for (const event of events) {
    switch (event.type) {
      case HubEventType.MERGE_MESSAGE: {
        const msg = event.mergeMessageBody!.message!
        const msgType = msg.data!.type

        switch (msgType) {
          case MessageType.CAST_ADD: {
            castAdds.push(msg)
            break
          }
          case MessageType.CAST_REMOVE: {
            castRemoves.push(msg)
            break
          }
          case MessageType.VERIFICATION_ADD_ETH_ADDRESS: {
            verificationAdds.push(msg)
            break
          }
          case MessageType.VERIFICATION_REMOVE: {
            verificationRemoves.push(msg)
            break
          }
          case MessageType.USER_DATA_ADD: {
            userDataAdds.push(msg)
            break
          }
          case MessageType.REACTION_ADD: {
            reactionAdds.push(msg)
            break
          }
          case MessageType.REACTION_REMOVE: {
            reactionRemoves.push(msg)
            break
          }
          case MessageType.LINK_ADD: {
            linkAdds.push(msg)
            break
          }
          case MessageType.LINK_REMOVE: {
            linkRemoves.push(msg)
            break
          }
          default: {
            log.debug('UNHANDLED MERGE_MESSAGE EVENT', event.id)
          }
        }

        break
      }
      case HubEventType.PRUNE_MESSAGE: {
        const msg = event.pruneMessageBody!.message!
        const msgType = msg.data!.type

        switch (msgType) {
          case MessageType.CAST_ADD: {
            castPrunes.push(msg)
            break
          }
          case MessageType.REACTION_ADD: {
            reactionPrunes.push(msg)
            break
          }
          case MessageType.LINK_ADD: {
            linkPrunes.push(msg)
            break
          }
          default: {
            log.debug(msg.data, 'UNHANDLED PRUNE_MESSAGE EVENT')
          }
        }

        break
      }
      case HubEventType.REVOKE_MESSAGE: {
        // Events are emitted when a signer that was used to create a message is removed
        // TODO: handle revoking messages
        break
      }
      case HubEventType.MERGE_ON_CHAIN_EVENT: {
        // TODO: index signers, storage, and fids
        break
      }
      default: {
        log.debug('UNHANDLED HUB EVENT', event.id)
        break
      }
    }
  }

  await insertCasts(castAdds)
  await deleteCasts(castRemoves)
  await pruneCasts(castPrunes)
  await insertVerifications(verificationAdds)
  await deleteVerifications(verificationRemoves)
  await insertUserDatas(userDataAdds)
  await insertReactions(reactionAdds)
  await deleteReactions(reactionRemoves)
  await pruneReactions(reactionPrunes)
  await insertLinks(linkAdds)
  await deleteLinks(linkRemoves)
  await pruneLinks(linkPrunes)

  await job.updateProgress(100)
}

export function makeLatestEventId() {
  const seq = 0
  const now = Date.now()
  const timestamp = now - FARCASTER_EPOCH
  const SEQUENCE_BITS = 12

  const binaryTimestamp = timestamp.toString(2)
  let binarySeq = seq.toString(2)
  if (binarySeq.length) {
    while (binarySeq.length < SEQUENCE_BITS) {
      binarySeq = `0${binarySeq}`
    }
  }

  return parseInt(binaryTimestamp + binarySeq, 2)
}
