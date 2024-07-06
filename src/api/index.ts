import { ReactionType } from '@farcaster/hub-nodejs'
import cors from 'cors'
import 'dotenv/config'
import express from 'express'
import { bytesToHex } from 'viem'

import { db } from '../db/kysely.js'
import { FetchCastsResponse } from './types/api'
import { FarcasterCastV1, FarcasterUserV1 } from './types/farcaster'
import { FarcasterFeedFilter } from './types/feed'

const app = express()
const port = process.env.PORT || 3000

app.use(cors())
app.use(express.json())

async function resolveCasts(
  hashes: `0x${string}`[],
  options: { depth?: number; maxDepth?: number } = { maxDepth: 1 }
): Promise<{
  castsMap: Record<`0x${string}`, FarcasterCastV1>
  usersMap: Record<string, FarcasterUserV1>
}> {
  if (options.depth === options.maxDepth) return { castsMap: {}, usersMap: {} }

  let query = db
    .selectFrom('casts')
    .selectAll()
    .where('casts.parentHash', 'is', null)
    .where('casts.deletedAt', 'is', null)
    .where('casts.prunedAt', 'is', null)
    .where(
      'casts.hash',
      'in',
      hashes.map((hash) => Buffer.from(hash.slice(2), 'hex'))
    )
    .select((eb) => [
      eb
        .selectFrom('reactions')
        .select((eb) => eb.fn.count('reactions.hash').as('reactionsCount'))
        .where('reactions.type', '=', ReactionType.LIKE)
        .whereRef('reactions.targetCastHash', '=', 'casts.hash')
        .as('likesCount'),
      eb
        .selectFrom('reactions')
        .select((eb) => eb.fn.count('reactions.hash').as('reactionsCount'))
        .where('reactions.type', '=', ReactionType.RECAST)
        .whereRef('reactions.targetCastHash', '=', 'casts.hash')
        .as('recastsCount'),
      eb
        .selectFrom('casts')
        .select((eb) => eb.fn.count('casts.hash').as('repliesCount'))
        .whereRef('casts.parentHash', '=', 'casts.hash')
        .as('repliesCount'),
    ])
    .leftJoin('signers', 'casts.signer', 'signers.key')
    .select('signers.requesterFid')

  // Order by timestamp in descending order (reverse chronological)
  query = query.orderBy('timestamp', 'desc')

  const casts = await query.execute()

  const fids = casts.reduce((acc, cur) => {
    acc.add(cur.fid)
    if (cur.parentFid) acc.add(cur.parentFid)
    ;(cur.mentions as number[])?.forEach((mention) => acc.add(mention))
    return acc
  }, new Set<number>())

  const users = await db
    .selectFrom('users')
    .selectAll('users')
    .select((eb) => [
      eb
        .selectFrom('links')
        .select((eb) => eb.fn.count('links.fid').as('followingCount'))
        .whereRef('links.fid', '=', 'users.fid')
        .as('followingCount'),
      eb
        .selectFrom('links')
        .select((eb) => eb.fn.count('links.fid').as('followersCount'))
        .whereRef('links.targetFid', '=', 'users.fid')
        .as('followersCount'),
    ])
    .where('users.fid', 'in', Array.from(fids))
    .execute()

  const usersMap = users.reduce<Record<number, FarcasterUserV1>>(
    (acc, user) => {
      if (!acc[user.fid]) {
        acc[user.fid] = {
          fid: user.fid.toString(),
          pfp: user.pfp || undefined,
          displayName: user.display || undefined,
          bio: user.bio || undefined,
          url: user.url || undefined,
          username: user.username || undefined,
          badges: {
            powerBadge: false,
          },
          engagement: {
            followers: parseInt(user.followersCount as string),
            following: parseInt(user.followingCount as string),
          },
          verifiedAddresses: [],
        }
      }

      return acc
    },
    {}
  )

  const quotedCastHashes = casts.reduce<Set<`0x${string}`>>((acc, cast) => {
    cast.embeds
      .filter((embed) => 'castId' in embed)
      .forEach((embed) => 'castId' in embed && acc.add(embed.castId.hash))
    return acc
  }, new Set())

  const quotedCasts = await resolveCasts(Array.from(quotedCastHashes), {
    depth: options.depth ? options.depth + 1 : 1,
    maxDepth: options.maxDepth,
  })

  const formattedCasts = casts.map<FarcasterCastV1>((cast) => {
    const urlEmbeds = cast.embeds.filter((embed) => 'url' in embed) as {
      url: string
    }[]
    const castEmbeds = cast.embeds.filter((embed) => 'castId' in embed) as {
      castId: {
        fid: number
        hash: `0x${string}`
      }
    }[]

    const mentions = cast.mentions.map((mention, i) => ({
      user: usersMap[mention] || {
        fid: mention.toString(),
        pfp: undefined,
        displayName: undefined,
        bio: undefined,
        url: undefined,
        username: undefined,
        badges: {
          powerBadge: false,
        },
        engagement: {
          followers: 0,
          following: 0,
        },
        verifiedAddresses: [],
      },
      position: cast.mentionsPositions[i].toString(),
    }))

    return {
      text: cast.text,
      appFid: (cast.requesterFid || 0).toString(),
      channelMentions: [],
      embedCasts: castEmbeds
        .map((e) => quotedCasts.castsMap[e.castId.hash])
        .filter((c) => Boolean(c)),
      embedHashes: castEmbeds.map((e) => e.castId.hash),
      embeds: urlEmbeds.map((e) => ({
        uri: e.url,
      })),
      embedUrls: urlEmbeds.map((e) => e.url),
      mentions: mentions,
      engagement: {
        reactions:
          parseInt(cast.likesCount as string) +
          parseInt(cast.recastsCount as string),
        replies: parseInt(cast.repliesCount as string),
        reposts: parseInt(cast.recastsCount as string),
        likes: parseInt(cast.likesCount as string),
        quotes: 0,
        recasts: parseInt(cast.recastsCount as string),
      },
      hash: bytesToHex(cast.hash),
      signer: bytesToHex(cast.signer),
      timestamp: cast.timestamp.getTime(),
      user: usersMap[cast.fid],
    } as FarcasterCastV1
  })

  const castsByHash = casts.reduce<Record<`0x${string}`, FarcasterCastV1>>(
    (acc, cast) => {
      acc[bytesToHex(cast.hash)] = formattedCasts.find(
        (c) => c.hash === bytesToHex(cast.hash)
      )!
      return acc
    },
    {}
  )

  return { castsMap: castsByHash, usersMap }
}

// Function to fetch cast feed from the database
async function fetchCastFeed(
  filter: FarcasterFeedFilter,
  cursor?: string,
  limit: number = 20
): Promise<FetchCastsResponse> {
  let query = db
    .selectFrom('casts')
    .selectAll()
    .where('casts.parentHash', 'is', null)
    .where('casts.deletedAt', 'is', null)
    .where('casts.prunedAt', 'is', null)

  // Apply filters
  // if (filter.fid) query = query.where('fid', '=', filter.fid)
  // if (filter.parentUrl) query = query.where('parentUrl', '=', filter.parentUrl)
  // if (filter.searchQuery)
  //   query = query.where('text', 'ilike', `%${filter.searchQuery}%`)

  // Apply cursor-based pagination
  if (cursor) {
    const cursorTimestamp = parseInt(cursor)
    query = query.where('timestamp', '<=', new Date(cursorTimestamp))
  }

  // Order by timestamp in descending order (reverse chronological)
  query = query.orderBy('timestamp', 'desc').limit(limit + 1)

  const rootCasts = await query.execute()

  const rootCastHashes = rootCasts.map((cast) => bytesToHex(cast.hash))

  const { castsMap, usersMap } = await resolveCasts(rootCastHashes, {
    maxDepth: 1,
  })

  const resolvedCasts = rootCasts.map((cast) => {
    const formattedCast = castsMap[bytesToHex(cast.hash)]
    return formattedCast
  })

  const hasNextPage = rootCasts.length > limit
  const data = hasNextPage ? resolvedCasts.slice(0, limit) : resolvedCasts
  const nextCursor = hasNextPage
    ? data[data.length - 1].timestamp.toString()
    : undefined

  return { data, nextCursor }
}

// Endpoint for fetching cast feed
app.post('/api/cast-feed', async (req, res) => {
  try {
    // const {filter, cursor, limit} = req.body as { filter: FarcasterFeedFilter; cursor?: string; limit?: number }

    const filter: FarcasterFeedFilter = req.body.filter
    const cursor = req.body.cursor as string | undefined
    const limit = parseInt(req.body.limit as string) || 20

    const data = await fetchCastFeed(filter, cursor, limit)
    res.json(data)
  } catch (error) {
    console.error('Error fetching cast feed:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.listen(port, () => {
  console.log(`Server is running on port ${port}`)
})
