import { HubEvent, HubEventType, MessageType } from '@farcaster/hub-nodejs'

import { insertEvent } from '../api/event.js'
import {
  castAddBatcher,
  castRemoveBatcher,
  linkAddBatcher,
  linkRemoveBatcher,
  reactionAddBatcher,
  reactionRemoveBatcher,
  userDataAddBatcher,
  verificationAddBatcher,
  verificationRemoveBatcher,
} from './batch.js'
import { hubClient } from './hub-client.js'
import { log } from './logger.js'

/**
 * Update the database based on the event type
 * @param event Hub event in JSON format
 */
export async function handleEvent(event: HubEvent) {
  // Handle each event type: MERGE_MESSAGE, PRUNE_MESSAGE, REVOKE_MESSAGE (3), MERGE_ID_REGISTRY_EVENT (4), MERGE_NAME_REGISTRY_EVENT (5)
  switch (event.type) {
    case HubEventType.MERGE_MESSAGE: {
      event.mergeMessageBody?.message?.data?.type
      event.mergeMessageBody!
      const msg = event.mergeMessageBody!.message!
      const msgType = event.mergeMessageBody!.message!.data!.type

      switch (msgType) {
        case MessageType.CAST_ADD: {
          return castAddBatcher.add(msg)
        }
        case MessageType.CAST_REMOVE: {
          return castRemoveBatcher.add(msg)
        }
        case MessageType.VERIFICATION_ADD_ETH_ADDRESS: {
          return verificationAddBatcher.add(msg)
        }
        case MessageType.VERIFICATION_REMOVE: {
          return verificationRemoveBatcher.add(msg)
        }
        case MessageType.USER_DATA_ADD: {
          return userDataAddBatcher.add(msg)
        }
        case MessageType.REACTION_ADD: {
          return reactionAddBatcher.add(msg)
        }
        case MessageType.REACTION_REMOVE: {
          return reactionRemoveBatcher.add(msg)
        }
        case MessageType.LINK_ADD: {
          return linkAddBatcher.add(msg)
        }
        case MessageType.LINK_REMOVE: {
          return linkRemoveBatcher.add(msg)
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

export async function saveCurrentEventId() {
  let triggered = false

  const result = await hubClient.subscribe({
    eventTypes: [
      HubEventType.NONE,
      HubEventType.MERGE_MESSAGE,
      HubEventType.PRUNE_MESSAGE,
      HubEventType.REVOKE_MESSAGE,
      HubEventType.MERGE_USERNAME_PROOF,
      HubEventType.MERGE_ON_CHAIN_EVENT,
    ],
  })

  if (result.isErr()) {
    log.error(result.error, 'Error starting stream')
    return
  }

  result.match(
    (stream) => {
      stream.on('data', async (e: HubEvent) => {
        if (triggered) return

        triggered = true

        // Save the latest event ID to the database so we can resume from here
        await insertEvent(e.id)
        stream.cancel()
      })
    },
    (e) => {
      log.error(e, 'Error streaming data.')
    }
  )
}
