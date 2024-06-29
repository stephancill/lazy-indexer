// TODO: Clean up the functions in this file, it's very repetitive
import {
  FidRequest,
  HubResult,
  HubRpcClient,
  Message,
  OnChainEvent,
  OnChainEventType,
} from '@farcaster/hub-nodejs'

import { hubClient } from './hub-client.js'
import { MAX_PAGE_SIZE, checkMessages } from './utils.js'

export type ExtraHubOptions = { includeRemoveMessages?: boolean } | undefined

export async function getAllCastsByFid(
  fid: FidRequest,
  options?: ExtraHubOptions
) {
  const casts: Message[] = new Array()
  let nextPageToken: Uint8Array | undefined

  const f = options?.includeRemoveMessages
    ? hubClient.getAllCastMessagesByFid
    : hubClient.getCastsByFid

  while (true) {
    const res = await f({
      ...fid,
      pageSize: MAX_PAGE_SIZE,
      pageToken: nextPageToken,
    })

    const messages = checkMessages(res, fid.fid)
    casts.push(...messages)

    if (messages.length < MAX_PAGE_SIZE) {
      break
    }

    nextPageToken = res._unsafeUnwrap().nextPageToken
  }

  return casts
}

export async function getAllReactionsByFid(
  fid: FidRequest,
  options?: ExtraHubOptions
) {
  const reactions: Message[] = new Array()
  let nextPageToken: Uint8Array | undefined

  const f = options?.includeRemoveMessages
    ? hubClient.getAllReactionMessagesByFid
    : hubClient.getReactionsByFid

  while (true) {
    const res = await f({
      ...fid,
      pageSize: MAX_PAGE_SIZE,
      pageToken: nextPageToken,
    })

    const messages = checkMessages(res, fid.fid)
    reactions.push(...messages)

    if (messages.length < MAX_PAGE_SIZE) {
      break
    }

    nextPageToken = res._unsafeUnwrap().nextPageToken
  }

  return reactions
}

export async function getAllLinksByFid(
  fid: FidRequest,
  options?: ExtraHubOptions
) {
  const links: Message[] = new Array()
  let nextPageToken: Uint8Array | undefined

  const f = options?.includeRemoveMessages
    ? hubClient.getAllLinkMessagesByFid
    : hubClient.getLinksByFid

  while (true) {
    const res = await f({
      ...fid,
      pageSize: MAX_PAGE_SIZE,
      pageToken: nextPageToken,
    })

    const messages = checkMessages(res, fid.fid)
    links.push(...messages)

    if (messages.length < MAX_PAGE_SIZE) {
      break
    }

    nextPageToken = res._unsafeUnwrap().nextPageToken
  }

  return links
}

// TODO: refactor this to be more consistent with the other functions
// Currently its a rip from the old replicator
export async function* getOnChainEventsByFidInBatchesOf(
  hub: HubRpcClient,
  {
    fid,
    pageSize,
    eventTypes,
  }: {
    fid: number
    pageSize: number
    eventTypes: OnChainEventType[]
  }
) {
  for (const eventType of eventTypes) {
    let result = await retryHubCallWithExponentialBackoff(() =>
      hub.getOnChainEvents({ pageSize, fid, eventType })
    )
    for (;;) {
      if (result.isErr()) {
        throw new Error(
          `Unable to backfill events for FID ${fid} of type ${eventType}`,
          { cause: result.error }
        )
      }

      const { events, nextPageToken: pageToken } = result.value
      yield events as OnChainEvent[]

      if (!pageToken?.length) break
      result = await retryHubCallWithExponentialBackoff(() =>
        hub.getOnChainEvents({ pageSize, pageToken, fid, eventType })
      )
    }
  }
}

async function retryHubCallWithExponentialBackoff<T>(
  fn: () => Promise<HubResult<T>>,
  attempt = 1,
  maxAttempts = 10,
  baseDelayMs = 100
): Promise<HubResult<T>> {
  let currentAttempt = attempt
  try {
    const result = await fn()
    if (result.isErr()) {
      throw new Error(`maybe retryable error : ${JSON.stringify(result.error)}`)
    }
    return result
  } catch (error) {
    if (currentAttempt >= maxAttempts) {
      throw error
    }

    const delayMs = baseDelayMs * 2 ** currentAttempt

    await new Promise((resolve) => setTimeout(resolve, delayMs))

    currentAttempt++
    return retryHubCallWithExponentialBackoff(
      fn,
      currentAttempt,
      maxAttempts,
      delayMs
    )
  }
}
