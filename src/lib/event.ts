import {
  FARCASTER_EPOCH,
  HubEvent,
  HubEventType,
  Message,
  MessageType,
} from '@farcaster/hub-nodejs'
import { Job } from 'bullmq'

import { deleteCasts, insertCasts } from '../api/cast.js'
import { deleteLinks, insertLinks } from '../api/link.js'
import { deleteReactions, insertReactions } from '../api/reaction.js'
import { insertUserDatas } from '../api/user-data.js'
import {
  deleteVerifications,
  insertVerifications,
} from '../api/verification.js'
import { log } from './logger.js'

/**
 * Update the database based on the event type
 * @param job Job to add to the `stream` queue
 */
export async function handleEvents(job: Job<Buffer[]>) {
  const encodedEvents = job.data
  const events = encodedEvents.map((e) => HubEvent.decode(Buffer.from(e)))

  const castAdds: Message[] = new Array()
  const castRemoves: Message[] = new Array()
  const verificationAdds: Message[] = new Array()
  const verificationRemoves: Message[] = new Array()
  const userDataAdds: Message[] = new Array()
  const reactionAdds: Message[] = new Array()
  const reactionRemoves: Message[] = new Array()
  const linkAdds: Message[] = new Array()
  const linkRemoves: Message[] = new Array()

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
        // TODO: Mark the relevant row as `pruned` in the db but don't delete it
        // Not important right now because I don't want to prune data for my applications
        break
      }
      case HubEventType.REVOKE_MESSAGE: {
        // Events are emitted when a signer that was used to create a message is removed
        // TODO: handle revoking messages
        break
      }
      case HubEventType.MERGE_ON_CHAIN_EVENT: {
        // TODO: index signers (storage and fids are less relevant for now)
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
  await insertVerifications(verificationAdds)
  await deleteVerifications(verificationRemoves)
  await insertUserDatas(userDataAdds)
  await insertReactions(reactionAdds)
  await deleteReactions(reactionRemoves)
  await insertLinks(linkAdds)
  await deleteLinks(linkRemoves)
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
