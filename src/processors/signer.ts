import {
  OnChainEvent,
  OnChainEventType,
  SignerEventType,
  isSignerOnChainEvent,
} from '@farcaster/hub-nodejs'
import { bytesToHex, decodeAbiParameters } from 'viem'

import { db } from '../db/kysely.js'
import { hubClient } from '../lib/hub-client.js'
import { getOnChainEventsByFidInBatchesOf } from '../lib/paginate.js'
import { MAX_PAGE_SIZE } from '../lib/utils.js'

const signedKeyRequestAbi = [
  {
    components: [
      {
        name: 'requestFid',
        type: 'uint256',
      },
      {
        name: 'requestSigner',
        type: 'address',
      },
      {
        name: 'signature',
        type: 'bytes',
      },
      {
        name: 'deadline',
        type: 'uint256',
      },
    ],
    name: 'SignedKeyRequest',
    type: 'tuple',
  },
] as const

export function decodeSignedKeyRequestMetadata(metadata: Uint8Array) {
  return decodeAbiParameters(signedKeyRequestAbi, bytesToHex(metadata))[0]
}

export async function getAllSignersByFid(fid: number) {
  let signerEvents: OnChainEvent[] = []

  for await (const events of getOnChainEventsByFidInBatchesOf(hubClient, {
    fid,
    pageSize: MAX_PAGE_SIZE,
    eventTypes: [OnChainEventType.EVENT_TYPE_SIGNER],
  })) {
    signerEvents = signerEvents.concat(...events)
  }

  // Since there could be many events, ensure we process them in sorted order
  const sortedEventsForFid = signerEvents.sort((a, b) =>
    a.blockNumber === b.blockNumber
      ? a.logIndex - b.logIndex
      : a.blockNumber - b.blockNumber
  )

  return sortedEventsForFid
}

export async function insertSigners(signers: OnChainEvent[]) {
  for (const signer of signers) {
    if (!isSignerOnChainEvent(signer))
      throw new Error(`Invalid SignerOnChainEvent: ${signer}`)

    const body = signer.signerEventBody
    const timestamp = new Date(signer.blockTimestamp * 1000)

    switch (body.eventType) {
      case SignerEventType.ADD: {
        const signedKeyRequestMetadata = decodeSignedKeyRequestMetadata(
          body.metadata
        )
        const metadataJson = {
          requestFid: Number(signedKeyRequestMetadata.requestFid),
          requestSigner: signedKeyRequestMetadata.requestSigner,
          signature: signedKeyRequestMetadata.signature,
          deadline: Number(signedKeyRequestMetadata.deadline),
        }

        await db
          .insertInto('signers')
          .values({
            addedAt: timestamp,
            fid: signer.fid,
            requesterFid: metadataJson.requestFid,
            key: body.key,
            keyType: body.keyType,
            metadata: JSON.stringify(metadataJson),
            metadataType: body.metadataType,
          })
          .onConflict((oc) =>
            oc.columns(['fid', 'key']).doUpdateSet(({ ref }) => ({
              // Update all other fields in case this was a replayed transaction from a block re-org
              addedAt: ref('excluded.addedAt'),
              requesterFid: ref('excluded.requesterFid'),
              keyType: ref('excluded.keyType'),
              metadata: JSON.stringify(metadataJson),
              metadataType: ref('excluded.metadataType'),
              updatedAt: new Date(),
            }))
          )
          .execute()

        break
      }
      case SignerEventType.REMOVE: {
        // Smart contract ensures there will always be an add before a remove, so we know we can update in-place
        db.updateTable('signers')
          .set({
            removedAt: timestamp,
            updatedAt: new Date(),
          })
          .where('fid', '=', signer.fid)
          .where('key', '=', body.key)
          .execute()

        break
      }
    }
  }
}
