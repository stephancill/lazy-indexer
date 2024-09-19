import {
  OnChainEvent,
  OnChainEventType,
  isStorageRentOnChainEvent,
} from '@farcaster/hub-nodejs'

import { db } from '../db/db.js'
import { hubClient } from '../lib/hub-client.js'
import { getOnChainEventsByFidInBatchesOf } from '../lib/paginate.js'
import { MAX_PAGE_SIZE, farcasterTimeToDate } from '../lib/utils.js'

export async function getAllStorageByFid(fid: number) {
  let storageEvents: OnChainEvent[] = []

  for await (const events of getOnChainEventsByFidInBatchesOf(hubClient, {
    fid,
    pageSize: MAX_PAGE_SIZE,
    eventTypes: [OnChainEventType.EVENT_TYPE_STORAGE_RENT],
  })) {
    storageEvents = storageEvents.concat(...events)
  }

  // Since there could be many events, ensure we process them in sorted order
  const sortedEventsForFid = storageEvents.sort((a, b) =>
    a.blockNumber === b.blockNumber
      ? a.logIndex - b.logIndex
      : a.blockNumber - b.blockNumber
  )

  return sortedEventsForFid
}

export async function insertStorage(storageEvents: OnChainEvent[]) {
  for (const storage of storageEvents) {
    if (!isStorageRentOnChainEvent(storage))
      throw new Error(`Invalid SignerOnChainEvent: ${storage}`)

    const body = storage.storageRentEventBody
    const timestamp = new Date(storage.blockTimestamp * 1000)

    await db
      .insertInto('storage')
      .values({
        fid: storage.fid,
        units: body.units,
        payer: body.payer,
        rentedAt: timestamp,
        expiresAt: farcasterTimeToDate(body.expiry),
      })
      .onConflict((oc) =>
        oc.columns(['fid', 'expiresAt']).doUpdateSet(({ ref }) => ({
          units: ref('excluded.units'),
          payer: ref('excluded.payer'),
          expiresAt: ref('excluded.expiresAt'),
          rentedAt: ref('excluded.rentedAt'),
          updatedAt: new Date(),
        }))
      )
      .execute()
  }
}
