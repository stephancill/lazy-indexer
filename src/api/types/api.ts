import {
  Channel,
  FarcasterCastV1,
  FarcasterUserV1,
  UrlContentResponse,
} from './farcaster'

export type GetFarcasterChannelRequest = {
  id: string
}

export type GetFarcasterChannelsRequest = {
  channelIds?: string[]
  parentUrls?: string[]
}

export type GetFarcasterChannelsResponse = {
  data: Channel[]
  nextCursor?: string
}

export type GetFarcasterUserRequest = {
  fid: string
}

export type GetFarcasterUsersRequest = {
  fids?: string[]
  addresses?: string[]
  filter?: {} //UserFilter
}

export type GetFarcasterCastRequest = {
  hash: string
}

export type GetFarcasterCastRepliesRequest = {
  hash: string
}

export type GetFarcasterCastClientRequest = {
  hash: string
}

export type GetFarcasterCastsRequest = {
  hashes: string[]
}

export type GetFarcasterCastsResponse = {
  data: FarcasterCastV1[]
  nextCursor?: string
}

export type GetFarcasterUsersResponse = {
  data: FarcasterUserV1[]
  nextCursor?: string
}

export type GetContentRequest = {
  uri: string
  cached?: boolean
}

export type GetContentsRequest = {
  uris: string[]
  cached?: boolean
}

export type GetContentsResponse = {
  data: UrlContentResponse[]
}

export type GetFarcasterUserFollowersRequest = {
  fid: string
}

export type GetSignerResponse = {
  publicKey: `0x${string}`
  token?: string
  deeplinkUrl?: string
  state?: string
  requestFid?: string
  requestAddress?: `0x${string}`
  signature?: `0x${string}`
  deadline?: number
}

export type PendingSignerResponse = {
  publicKey: `0x${string}`
  requestFid: string
  deadline: number
  signature: `0x${string}`
  requestAddress: `0x${string}`
}

export type ValidateSignerResponse = {
  state: string
}

export type SubmitCastAddRequest = {
  text: string
  parentUrl?: string
  parentFid?: string
  parentHash?: string
  castEmbedFid?: string
  castEmbedHash?: string
  embeds?: string[]
  parsedEmbeds?: string[]
}

export type PendingCastRequest = SubmitCastAddRequest & {
  id: string
  scheduledFor: string | null
}

// export type PendingCastResponse = {
//   data: PendingCast[]
//   nextCursor?: string
// }

export type SubmitCastRemoveRequest = {
  hash: string
}

export type SubmitReactionAddRequest = {
  reactionType: number
  targetFid: string
  targetHash: string
}

export type SubmitReactionRemoveRequest = {
  reactionType: number
  targetFid: string
  targetHash: string
}

export type SubmitLinkAddRequest = {
  linkType: string
  targetFid: string
  username?: string
}

export type SubmitLinkRemoveRequest = {
  linkType: string
  targetFid: string
  username?: string
}

export type SubmitUserDataAddRequest = {
  type: number
  value: string
}

export type SubmitMessageResponse = {
  hashes?: string[]
  hash: string
  trustedBytes?: string
}

export type SubmitMessageError = {
  message: string
}

export type SubmitFrameActionRequest = {
  url: string
  castFid: string
  castHash: string
  postUrl: string
  action?: string
  inputText?: string
  buttonIndex: number
  state?: string
  address?: `0x${string}`
  transactionId?: `0x${string}`
}

export type FramePayload = {
  untrustedData: {
    fid: number
    url: string
    messageHash: string
    timestamp: number
    network: number
    buttonIndex: number
    inputText?: string
    state?: string
    castId: {
      fid: number
      hash: string
    }
    address?: `0x${string}`
    transactionId?: `0x${string}`
  }
  trustedData: {
    messageBytes: string
  }
}

export type FetchCastsResponse = {
  data: FarcasterCastV1[]
  nextCursor?: string
}

export type FetchUsersResponse = {
  data: FarcasterUserV1[]
  nextCursor?: string
}

export type FetchChannelsResponse = {
  data: Channel[]
  nextCursor?: string
}
