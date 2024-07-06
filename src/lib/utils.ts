import {
  ContactInfoContentBody,
  FidRequest,
  HubResult,
  Message,
  MessagesResponse,
  OnChainEventResponse,
  fromFarcasterTime,
} from '@farcaster/hub-nodejs'
import { Insertable } from 'kysely'
import { bytesToHex } from 'viem'

import { Tables } from '../db/db.types.js'
import { getAllRegistrationsByFid } from '../processors/fid.js'
import { getAllSignersByFid } from '../processors/signer.js'
import { getAllStorageByFid } from '../processors/storage.js'
import { hubClient } from './hub-client.js'
import { log } from './logger.js'
import {
  ExtraHubOptions,
  getAllCastsByFid,
  getAllLinksByFid,
  getAllReactionsByFid,
} from './paginate.js'

export const MAX_PAGE_SIZE = 10_000

export const NULL_ETH_ADDRESS = Uint8Array.from(
  Buffer.from('0000000000000000000000000000000000000000', 'hex')
)

export function farcasterTimeToDate(time: number): Date {
  const result = fromFarcasterTime(time)
  if (result.isErr()) throw result.error
  return new Date(result.value)
}

export function formatCasts(msgs: Message[]) {
  return msgs.map((msg) => {
    const data = msg.data!
    const castAddBody = data.castAddBody!

    const embeds = castAddBody.embeds.map((embed) =>
      'castId' in embed && embed.castId
        ? {
            castId: {
              fid: embed.castId.fid,
              hash: bytesToHex(embed.castId.hash),
            },
          }
        : embed
    )

    return {
      timestamp: farcasterTimeToDate(data.timestamp),
      fid: data.fid,
      parentFid: castAddBody.parentCastId?.fid,
      hash: msg.hash,
      parentHash: castAddBody.parentCastId?.hash,
      parentUrl: castAddBody.parentUrl,
      text: castAddBody.text,
      embeds: JSON.stringify(embeds),
      mentions: JSON.stringify(castAddBody.mentions),
      mentionsPositions: JSON.stringify(castAddBody.mentionsPositions),
      signer: msg.signer,
    } satisfies Insertable<Tables['casts']>
  })
}

export function formatReactions(msgs: Message[]) {
  return msgs.map((msg) => {
    const data = msg.data!
    const reaction = data.reactionBody!

    return {
      timestamp: farcasterTimeToDate(data.timestamp),
      fid: data.fid,
      targetCastFid: reaction.targetCastId?.fid,
      type: reaction.type,
      hash: msg.hash,
      targetCastHash: reaction.targetCastId?.hash,
      targetUrl: reaction.targetUrl,
      signer: msg.signer,
    } satisfies Insertable<Tables['reactions']>
  })
}

export function formatUserDatas(msgs: Message[]) {
  // Users can submit multiple messages with the same `userDataAddBody.type` within the batch period
  // We reconcile this by using the value of the last message with the same type from that fid
  const userDataMap = new Map<string, Message>()

  for (const msg of msgs) {
    const data = msg.data!
    const userDataAddBody = data.userDataBody!
    userDataMap.set(`fid:${data.fid}-type:${userDataAddBody.type}`, msg)
  }

  return Array.from(userDataMap.values()).map((msg) => {
    const data = msg.data!
    const userDataAddBody = data.userDataBody!

    return {
      timestamp: farcasterTimeToDate(data.timestamp),
      fid: data.fid,
      type: userDataAddBody.type,
      hash: msg.hash,
      value: userDataAddBody.value,
      signer: msg.signer,
    } satisfies Insertable<Tables['userData']>
  })
}

export function formatVerifications(msgs: Message[]) {
  return msgs.map((msg) => {
    const data = msg.data!
    const addAddressBody = data.verificationAddAddressBody!

    return {
      timestamp: farcasterTimeToDate(data.timestamp),
      fid: data.fid,
      hash: msg.hash,
      signerAddress: addAddressBody.address,
      blockHash: addAddressBody.blockHash,
      signature: addAddressBody.claimSignature,
    } satisfies Insertable<Tables['verifications']>
  })
}

export function formatLinks(msgs: Message[]) {
  return msgs.map((msg) => {
    const data = msg.data!
    const link = data.linkBody!

    return {
      timestamp: farcasterTimeToDate(data.timestamp),
      fid: data.fid,
      targetFid: link.targetFid,
      displayTimestamp: link.displayTimestamp
        ? farcasterTimeToDate(link.displayTimestamp)
        : null,
      type: link.type,
      hash: msg.hash,
      signer: msg.signer,
    } satisfies Insertable<Tables['links']>
  })
}

export function formatHubs(contacts: ContactInfoContentBody[]) {
  return contacts.map(
    (c) =>
      ({
        gossipAddress: JSON.stringify(c.gossipAddress),
        rpcAddress: JSON.stringify(c.rpcAddress),
        excludedHashes: c.excludedHashes,
        count: c.count,
        hubVersion: c.hubVersion,
        network: c.network.toString(),
        appVersion: c.appVersion,
        timestamp: c.timestamp,
      }) satisfies Insertable<Tables['hubs']>
  )
}

export function breakIntoChunks<T>(array: T[], size: number) {
  const chunks = []
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size))
  }
  return chunks
}

export function checkMessages(
  messages: HubResult<MessagesResponse>,
  fid: number
) {
  if (messages.isErr()) {
    // This happens consistently for the same fids for an unknown reason, but still saves their relevant data
    log.debug(messages.error, `Error fetching messages for FID ${fid}`)
  }

  return messages.isOk() ? messages.value.messages : []
}

export function checkOnchainEvent(
  event: HubResult<OnChainEventResponse>,
  fid: number
) {
  if (event.isErr()) {
    log.warn(event.error, `Error fetching onchain event for FID ${fid}`)
  }

  return event.isOk() ? event.value : null
}

/**
 * Index all messages from a profile
 * @param fid Farcaster ID
 */
export async function getFullProfileFromHub(
  _fid: number,
  options?: ExtraHubOptions
) {
  const fid = FidRequest.create({ fid: _fid })

  const userData = await hubClient.getUserDataByFid(fid)
  const verifications = options?.includeRemoveMessages
    ? await hubClient.getAllVerificationMessagesByFid(fid)
    : await hubClient.getVerificationsByFid(fid)

  return {
    casts: await getAllCastsByFid(fid, options),
    reactions: await getAllReactionsByFid(fid, options),
    links: await getAllLinksByFid(fid, options),
    userData: checkMessages(userData, _fid),
    verifications: checkMessages(verifications, _fid),

    // Onchain events
    registrations: getAllRegistrationsByFid(_fid),
    signers: getAllSignersByFid(_fid),
    storage: getAllStorageByFid(_fid),
  }
}

export async function inBatchesOf<T>(
  items: T[],
  batchSize: number,
  fn: (batch: T[]) => any
) {
  let offset = 0
  while (offset < items.length) {
    const batch = items.slice(offset, offset + batchSize)
    await fn(batch)
    offset += batchSize
  }
}

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
