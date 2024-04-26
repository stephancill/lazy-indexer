// TODO: Clean up the functions in this file, it's very repetitive
import { FidRequest, Message } from '@farcaster/hub-nodejs'

import { hubClient } from './hub-client.js'
import { checkMessages } from './utils.js'

export async function getAllCastsByFid(fid: FidRequest) {
  const pageSize = 10_000
  const casts: Message[] = new Array()
  let nextPageToken: Uint8Array | undefined

  while (true) {
    const res = await hubClient.getCastsByFid({
      ...fid,
      pageSize,
      pageToken: nextPageToken,
    })

    const messages = checkMessages(res, fid.fid)
    casts.push(...messages)

    if (messages.length < pageSize) {
      break
    }

    nextPageToken = res._unsafeUnwrap().nextPageToken
  }

  return casts
}

export async function getAllReactionsByFid(fid: FidRequest) {
  const pageSize = 10_000
  const reactions: Message[] = new Array()
  let nextPageToken: Uint8Array | undefined

  while (true) {
    const res = await hubClient.getReactionsByFid({
      ...fid,
      pageSize,
      pageToken: nextPageToken,
    })

    const messages = checkMessages(res, fid.fid)
    reactions.push(...messages)

    if (messages.length < pageSize) {
      break
    }

    nextPageToken = res._unsafeUnwrap().nextPageToken
  }

  return reactions
}
