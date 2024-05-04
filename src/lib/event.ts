import {
  FARCASTER_EPOCH,
  HubEvent,
  HubEventType,
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
 * @param event Hub event in JSON format
 */
export async function handleEvent(job: Job<HubEvent>) {
  const event = job.data

  // Handle each event type: MERGE_MESSAGE, PRUNE_MESSAGE, REVOKE_MESSAGE (3), MERGE_ID_REGISTRY_EVENT (4), MERGE_NAME_REGISTRY_EVENT (5)
  switch (event.type) {
    case HubEventType.MERGE_MESSAGE: {
      event.mergeMessageBody?.message?.data?.type
      event.mergeMessageBody!
      const msg = event.mergeMessageBody!.message!
      const msgType = event.mergeMessageBody!.message!.data!.type

      switch (msgType) {
        case MessageType.CAST_ADD: {
          return await insertCasts([msg])
        }
        case MessageType.CAST_REMOVE: {
          return await deleteCasts([msg])
        }
        case MessageType.VERIFICATION_ADD_ETH_ADDRESS: {
          return await insertVerifications([msg])
        }
        case MessageType.VERIFICATION_REMOVE: {
          return await deleteVerifications([msg])
        }
        case MessageType.USER_DATA_ADD: {
          return await insertUserDatas([msg])
        }
        case MessageType.REACTION_ADD: {
          return await insertReactions([msg])
        }
        case MessageType.REACTION_REMOVE: {
          return await deleteReactions([msg])
        }
        case MessageType.LINK_ADD: {
          return await insertLinks([msg])
        }
        case MessageType.LINK_REMOVE: {
          return await deleteLinks([msg])
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
