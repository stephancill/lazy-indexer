import {
  HubRpcClient,
  Message,
  ReactionType,
  UserDataType,
  base58ToBytes,
  isLinkAddMessage,
  isLinkCompactStateMessage,
  isReactionAddMessage,
  isUserDataAddMessage,
  reactionTypeToJSON,
} from '@farcaster/hub-nodejs'
import { Queue } from 'bullmq'
import fastq from 'fastq'
import { createPublicClient, http, parseAbi } from 'viem'
import { optimism } from 'viem/chains'

import { getAllLinksByFid } from './paginate.js'

const HUB_URL = process.env.HUB_REST_URL!

export function getPopulateNetworkJobId(fid: number) {
  return `refreshNetwork-${fid}`
}

export function getPopulateFollowersJobId(fid: number) {
  return `populateFollowers-${fid}`
}

export function getAllLinksByFidKey(fid: number) {
  return `linksByFid:${fid}`
}

export function getAllLinksByTargetKey(fid: number) {
  return `linksByTarget:${fid}`
}

export function getNetworkByFidKey(fid: number) {
  return `network:${fid}`
}

export function getCastEndpointCacheKey(
  castId: { fid: number; hash: string },
  viewerFid: number
) {
  return `castEndpoint:${castId.fid}:${castId.hash}:${viewerFid}`
}

export function getFollowersEndpointCacheKey(fid: number, viewerFid: number) {
  return `followersEndpoint:${fid}:${viewerFid}`
}

export async function findJobPosition(jobId: string, queue: Queue) {
  const waitingJobs = await queue.getWaiting()
  const position = waitingJobs.findIndex((job) => job.id === jobId)

  return position !== -1 ? position : null
}

export async function getFidCount() {
  const client = createPublicClient({
    transport: http(),
    chain: optimism,
  })
  const fidCount = await client.readContract({
    abi: parseAbi(['function idCounter() view returns (uint256)']),
    address: '0x00000000Fc6c5F01Fc30151999387Bb99A9f489b',
    functionName: 'idCounter',
  })

  const fidCountNumber = Number(fidCount)

  return fidCountNumber
}

export async function getNetworkByFid(
  fid: number,
  maxDepth: number,
  {
    onProgress = () => {},
    forceRefresh,
  }: {
    onProgress?: (message: string) => void
    forceRefresh?: boolean
  }
) {
  let startTime = Date.now()

  // Breadth-first search to get all links up to N levels deep using getAllLinksByFid
  const allLinks = new Set<number>()
  const linksByDepth: Record<number, Set<number>> = {}
  const occurrancesByFid: Record<number, number> = {}
  let linksToIndex = new Set<number>([fid])
  let depth = 0

  while (depth < maxDepth && linksToIndex.size > 0) {
    /**
     * This loop gets links for each fid in linksToIndex, and then adds them to nextLinks
     */

    onProgress(`Searching ${linksToIndex.size} links at depth ${depth}`)

    const nextLinks = new Array<number[]>()
    linksByDepth[depth] = new Set()
    const linksToIndexArray = Array.from(linksToIndex)

    /** Fetch cached links and then fetch remaining */

    const uncachedLinks: number[] = []

    // Get existing links for fids from cache
    let startTime = Date.now()

    // Index fids
    linksToIndexArray.forEach((link, idx) => {
      // If it's the first time we've seen this link, index it in linksByDepth
      if (!allLinks.has(link)) {
        linksByDepth[depth].add(link)
      } else {
      }

      allLinks.add(link)
      uncachedLinks.push(link)
    })

    // If we're at the last depth, don't fetch any more links
    if (depth === maxDepth - 1) {
      depth++
      break
    }

    onProgress(
      `Populating ${uncachedLinks.length} uncached links at depth ${depth}`
    )

    // Fetch links worker
    let completed = 0
    const queue = fastq.promise(async (fid: number) => {
      const startTime = Date.now()
      const result = await _getAllLinksByFid(fid, { hubUrl: HUB_URL })
      nextLinks.push(result)
      completed += 1
      const duration = Date.now() - startTime
      if (completed % 200 === 0)
        onProgress(
          `Populated uncached links at depth ${depth}: ${completed.toLocaleString()}/${uncachedLinks.length.toLocaleString()} ${
            Math.round((duration / 1000) * 100) / 100
          }s`
        )
    }, 50)

    // Populate fetch links worker queue
    startTime = Date.now()
    for (const fid of uncachedLinks) {
      queue.push(fid)
    }

    // Wait for all links to be fetched
    await queue.drained()

    onProgress(
      `Populated uncached links at depth ${depth}: ${uncachedLinks.length} ${
        Math.round(((Date.now() - startTime) / 1000) * 100) / 100
      }s`
    )

    linksToIndex = new Set<number>()

    startTime = Date.now()

    // j = 0;
    for (const links of nextLinks) {
      links.forEach((fid) => {
        linksToIndex.add(fid)

        // Count occurrances of each fid
        if (!occurrancesByFid[fid]) {
          occurrancesByFid[fid] = 0
        }
        occurrancesByFid[fid] += 1
      })
    }

    console.log(`Processing nextLinks ${Date.now() - startTime}ms`)

    depth++
  }

  const linksByDepthArrays = Object.entries(linksByDepth).reduce(
    (acc, [k, v]) => {
      acc[parseInt(k)] = Array.from(v)
      return acc
    },
    {} as Record<number, number[]>
  )

  onProgress(`Done`)

  return { allLinks, linksByDepth, popularityByFid: occurrancesByFid }
}

export async function _getAllLinksByFid(
  fid: number,
  {
    hubUrl,
    hubClient,
  }:
    | { hubUrl: string; hubClient?: never }
    | { hubUrl?: never; hubClient: HubRpcClient }
) {
  const linksMessages = hubUrl
    ? await getAllMessagesFromHubEndpoint({
        endpoint: '/v1/linksByFid',
        hubUrl,
        params: {
          fid: fid.toString(),
        },
      })
    : await getAllLinksByFid({ fid })

  const linksSet = new Set<number>()

  linksMessages.forEach((linkMessage) =>
    isLinkAddMessage(linkMessage) && linkMessage.data.linkBody.targetFid
      ? linksSet.add(linkMessage.data.linkBody.targetFid)
      : isLinkCompactStateMessage(linkMessage)
        ? linkMessage.data.linkCompactStateBody.targetFids.forEach((t) =>
            linksSet.add(t)
          )
        : null
  )

  const linksArray = Array.from(linksSet)

  return linksArray
}

export async function getAllLikersByCast(
  castId: { fid: number; hash: string },
  { hubUrl }: { hubUrl: string }
) {
  const reactions = await getAllMessagesFromHubEndpoint({
    endpoint: '/v1/reactionsByCast',
    params: {
      target_fid: castId.fid.toString(),
      target_hash: castId.hash,
      reaction_type: reactionTypeToJSON(ReactionType.LIKE),
    },
    hubUrl,
  })

  console.log(`Got ${reactions.length} reaction messages`)

  const fids = reactions.filter(isReactionAddMessage).map((r) => r.data.fid)

  return fids
}

export async function getUserDataByFid(
  fid: number,
  { hubUrl }: { hubUrl: string }
) {
  const userData = await getAllMessagesFromHubEndpoint({
    endpoint: '/v1/userDataByFid',
    hubUrl,
    params: {
      fid: fid.toString(),
    },
  })
  return userData.reduce(
    (acc, message) => {
      if (!isUserDataAddMessage(message)) {
        return acc
      }

      return {
        ...acc,
        [message.data.userDataBody.type]: message.data.userDataBody.value,
      }
    },
    {} as Record<UserDataType, string>
  )
}

export const MAX_PAGE_SIZE = 1000

export async function getAllMessagesFromHubEndpoint({
  endpoint,
  hubUrl,
  params,
  limit,
  debug,
  onProgress,
}: {
  endpoint: string
  hubUrl: string
  params: Record<string, string>
  limit?: number
  debug?: boolean
  onProgress?: (message: string) => void
}) {
  const messages: Message[] = new Array()
  let nextPageToken: string | undefined

  while (true) {
    const _params = new URLSearchParams({
      pageSize: MAX_PAGE_SIZE.toString(),
    })

    for (const [key, value] of Object.entries(params)) {
      _params.append(key, value)
    }

    if (nextPageToken) {
      _params.append('pageToken', nextPageToken)
    }

    const url = `${hubUrl}${endpoint}?${_params}`

    const res = await fetch(url)

    if (!res.ok) {
      throw new Error(`Failed to fetch messages from ${url}`)
    }

    const { messages: resMessages, nextPageToken: _nextPageToken } =
      await res.json()

    nextPageToken = _nextPageToken

    const transformedMessages = resMessages
      .map(transformHashReverse)
      .map(Message.fromJSON)

    messages.push(...transformedMessages)

    if (debug) {
      console.log(
        `Total messages ${messages.length.toLocaleString()} from ${url}`
      )
    }

    onProgress?.(
      `Fetched ${messages.length.toLocaleString()} messages from ${endpoint}`
    )

    // Only fetch one page in development
    if (process.env.NEXT_PUBLIC_NODE_ENV === 'development') {
      console.log(`Breaking after fetching one page from ${url}`)
      break
    }

    if (
      resMessages.length < MAX_PAGE_SIZE ||
      (limit && messages.length >= limit)
    ) {
      // console.log(`Breaking after fetching ${messages.length} messages`);
      break
    }
  }

  return messages
}

// Map of current key names to old key names that we want to preserve for backwards compatibility reasons
// If you are renaming a protobuf field, add the current name as the key, and the old name as the value, and we
// will copy the contents of the current field to the old field
const BACKWARDS_COMPATIBILITY_MAP: Record<string, string> = {
  verificationAddAddressBody: 'verificationAddEthAddressBody',
  claimSignature: 'ethSignature',
}

/**
 * The protobuf format specifies encoding bytes as base64 strings, but we want to return hex strings
 * to be consistent with the rest of the API, so we need to convert the base64 strings to hex strings
 * before returning them.
 */
// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export function transformHashReverse(objRaw: any): any {
  const obj = structuredClone(objRaw)

  if (obj === null || typeof obj !== 'object') {
    return obj
  }

  // These are the target keys that are base64 encoded, which should be converted to hex
  const toHexKeys = [
    'hash',
    'signer',
    'transactionHash',
    'key',
    'owner',
    'to',
    'from',
    'recoveryAddress',
  ]

  // Convert these target keys to strings
  const toStringKeys = ['name']

  const toHexOrBase58Keys = ['address', 'blockHash']

  for (const key in obj) {
    // biome-ignore lint/suspicious/noPrototypeBuiltins: <explanation>
    if (obj.hasOwnProperty(key)) {
      if (toHexKeys.includes(key) && typeof obj[key] === 'string') {
        // obj[key] = convertB64ToHex(obj[key]);
        // Reverse: convert hex to base64
        obj[key] = Buffer.from(obj[key].slice(2), 'hex').toString('base64')
      } else if (toStringKeys.includes(key) && typeof obj[key] === 'string') {
        // obj[key] = Buffer.from(obj[key], "base64").toString("utf-8");
        // Reverse: convert string to base64
        obj[key] = Buffer.from(obj[key]).toString('base64')
      } else if (
        toHexOrBase58Keys.includes(key) &&
        typeof obj[key] === 'string'
      ) {
        // We need to convert solana related bytes to base58
        if (obj['protocol'] === 'PROTOCOL_SOLANA') {
          // obj[key] = convertB64ToB58(obj[key]);
          // Reverse: convert base58 to base64
          obj[key] = Buffer.from(
            base58ToBytes(obj[key]).unwrapOr(new Uint8Array())
          ).toString('base64')
        } else {
          // obj[key] = convertB64ToHex(obj[key]);
          // Reverse: convert hex to base64
          obj[key] = Buffer.from(obj[key].slice(2), 'hex').toString('base64')
        }
      } else if (typeof obj[key] === 'object') {
        obj[key] = transformHashReverse(obj[key])
      }

      const backwardsCompatibleName = BACKWARDS_COMPATIBILITY_MAP[key]
      if (backwardsCompatibleName) {
        obj[backwardsCompatibleName] = obj[key]
      }
    }
  }

  return obj
}
