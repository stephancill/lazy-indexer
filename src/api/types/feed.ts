export type FarcasterFeed = {
  api: string
  filter: FarcasterFeedFilter
}

export type FarcasterFeedFilter = {
  text?: string[]
  embeds?: string[]
  contentTypes?: string[]
  includeReplies?: boolean
  onlyReplies?: boolean
  onlyFrames?: boolean
}

export type FeedContext = {
  viewerFid?: string
  mutedWords?: string[]
  mutedUsers?: string[]
  mutedChannels?: string[]
}

export type FarcasterFeedRequest = {
  api?: string
  filter: FarcasterFeedFilter
  context?: FeedContext
  cursor?: string
  limit?: number
}

export enum Display {
  CASTS = 'CASTS',
  REPLIES = 'REPLIES',
  MEDIA = 'MEDIA',
  GRID = 'GRID',
  MASONRY = 'MASONRY',
  FRAMES = 'FRAMES',
  EMBEDS = 'EMBEDS',
  LIST = 'LIST',
  NOTIFICATION = 'NOTIFICATION',
}
