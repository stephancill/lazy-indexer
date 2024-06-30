import {
  HubEvent,
  HubEventType,
  MessageType,
  OnChainEventType,
  isMergeOnChainHubEvent,
  isSignerOnChainEvent,
} from '@farcaster/hub-nodejs'
import { Job } from 'bullmq'

import { deleteCasts, insertCasts, pruneCasts } from '../api/cast.js'
import { insertRegistrations } from '../api/fid.js'
import { deleteLinks, insertLinks, pruneLinks } from '../api/link.js'
import {
  deleteReactions,
  insertReactions,
  pruneReactions,
} from '../api/reaction.js'
import { decodeSignedKeyRequestMetadata, insertSigners } from '../api/signer.js'
import { insertStorage } from '../api/storage.js'
import { insertUserDatas } from '../api/user-data.js'
import {
  deleteVerifications,
  insertVerifications,
} from '../api/verification.js'
import { getRootBackfillQueue, queueRootBackfillJob } from './backfill.js'
import { log } from './logger.js'
import { allTargetsKey, isTarget } from './targets.js'

const { TARGET_SIGNER_FID, TARGET_SIGNER_PUBLIC_KEY } = process.env

/**
 * Update the database based on the event type
 * @param job Job to add to the `stream` queue
 */
export async function handleEventJob(job: Job<Buffer>) {
  const encodedEvent = job.data
  const event = HubEvent.decode(Buffer.from(encodedEvent))

  await handleEvent(event)

  await job.updateProgress(100)
}

export async function handleEvent(event: HubEvent) {
  if (
    isMergeOnChainHubEvent(event) &&
    isSignerOnChainEvent(event.mergeOnChainEventBody.onChainEvent)
  ) {
    const { requestFid: appFid, requestSigner: appSigner } =
      decodeSignedKeyRequestMetadata(
        event.mergeOnChainEventBody.onChainEvent.signerEventBody.metadata
      )
    const fid = event.mergeOnChainEventBody.onChainEvent.fid

    const matchesSignerFid =
      TARGET_SIGNER_FID && appFid === BigInt(TARGET_SIGNER_FID)
    const matchesSignerPublicKey =
      TARGET_SIGNER_PUBLIC_KEY && appSigner === TARGET_SIGNER_PUBLIC_KEY

    if (matchesSignerFid || matchesSignerPublicKey) {
      // Queue root backfill job
      const queue = getRootBackfillQueue()
      queueRootBackfillJob(fid, queue)
    }
  }

  const fid =
    event.mergeMessageBody?.message?.data?.fid ||
    event.mergeOnChainEventBody?.onChainEvent?.fid ||
    event.mergeUsernameProofBody?.usernameProof?.fid ||
    event.pruneMessageBody?.message?.data?.fid ||
    event.revokeMessageBody?.message?.data?.fid

  if (fid && !(await isTarget(fid, allTargetsKey))) {
    // log.debug('Skipping event for unknown FID ' + fid)
    return
  }

  log.debug('Processing event for known FID ' + fid)

  switch (event.type) {
    case HubEventType.MERGE_MESSAGE: {
      const msg = event.mergeMessageBody!.message!
      const msgType = msg.data!.type

      switch (msgType) {
        case MessageType.CAST_ADD: {
          await insertCasts([msg])
          break
        }
        case MessageType.CAST_REMOVE: {
          await deleteCasts([msg])
          break
        }
        case MessageType.VERIFICATION_ADD_ETH_ADDRESS: {
          await insertVerifications([msg])
          break
        }
        case MessageType.VERIFICATION_REMOVE: {
          await deleteVerifications([msg])
          break
        }
        case MessageType.USER_DATA_ADD: {
          await insertUserDatas([msg])
          break
        }
        case MessageType.REACTION_ADD: {
          await insertReactions([msg])
          break
        }
        case MessageType.REACTION_REMOVE: {
          await deleteReactions([msg])
          break
        }
        case MessageType.LINK_ADD: {
          await insertLinks([msg], 'stream')
          break
        }
        case MessageType.LINK_REMOVE: {
          await deleteLinks([msg])
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
          await pruneCasts([msg])
          break
        }
        case MessageType.REACTION_ADD: {
          await pruneReactions([msg])
          break
        }
        case MessageType.LINK_ADD: {
          await pruneLinks([msg])
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
      const onChainEvent = event.mergeOnChainEventBody!.onChainEvent!

      switch (onChainEvent.type) {
        case OnChainEventType.EVENT_TYPE_ID_REGISTER: {
          await insertRegistrations([onChainEvent])
          break
        }
        case OnChainEventType.EVENT_TYPE_SIGNER: {
          await insertSigners([onChainEvent])
          break
        }
        case OnChainEventType.EVENT_TYPE_STORAGE_RENT: {
          await insertStorage([onChainEvent])
          break
        }
      }

      break
    }
    default: {
      log.debug('UNHANDLED HUB EVENT', event.id)
      break
    }
  }
}
