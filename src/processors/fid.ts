import {
  IdRegisterEventType,
  OnChainEvent,
  OnChainEventType,
  isIdRegisterOnChainEvent,
} from '@farcaster/hub-nodejs'

import { db } from '../db/kysely.js'
import { hubClient } from '../lib/hub-client.js'
import { getOnChainEventsByFidInBatchesOf } from '../lib/paginate.js'
import { MAX_PAGE_SIZE, NULL_ETH_ADDRESS } from '../lib/utils.js'

export async function getAllRegistrationsByFid(fid: number) {
  let registrationEvents: OnChainEvent[] = []

  for await (const events of getOnChainEventsByFidInBatchesOf(hubClient, {
    fid,
    pageSize: MAX_PAGE_SIZE,
    eventTypes: [OnChainEventType.EVENT_TYPE_ID_REGISTER],
  })) {
    registrationEvents = registrationEvents.concat(...events)
  }

  // Since there could be many events, ensure we process them in sorted order
  const sortedEventsForFid = registrationEvents.sort((a, b) =>
    a.blockNumber === b.blockNumber
      ? a.logIndex - b.logIndex
      : a.blockNumber - b.blockNumber
  )

  return sortedEventsForFid
}

export async function insertRegistrations(registrationEvents: OnChainEvent[]) {
  for (const registration of registrationEvents) {
    if (!isIdRegisterOnChainEvent(registration))
      throw new Error(`Invalid Registration Event: ${registration}`)

    const body = registration.idRegisterEventBody
    const custodyAddress = body.to.length ? body.to : NULL_ETH_ADDRESS
    const recoveryAddress = body.recoveryAddress.length
      ? body.recoveryAddress
      : NULL_ETH_ADDRESS

    switch (body.eventType) {
      case IdRegisterEventType.REGISTER: {
        await db
          .insertInto('fids')
          .values({
            fid: registration.fid,
            registeredAt: new Date(registration.blockTimestamp * 1000),
            custodyAddress,
            recoveryAddress,
          })
          .onConflict((oc) =>
            oc.column('fid').doUpdateSet(({ ref }) => ({
              registeredAt: ref('excluded.registeredAt'),
              custodyAddress: ref('excluded.custodyAddress'),
              recoveryAddress: ref('excluded.recoveryAddress'),
              updatedAt: new Date(),
            }))
          )
          .execute()
        break
      }
      case IdRegisterEventType.TRANSFER: {
        await db
          .updateTable('fids')
          .where('fid', '=', registration.fid)
          .set({
            custodyAddress,
            updatedAt: new Date(),
          })
          .execute()
        break
      }
      case IdRegisterEventType.CHANGE_RECOVERY: {
        await db
          .updateTable('fids')
          .where('fid', '=', registration.fid)
          .set({
            recoveryAddress,
            updatedAt: new Date(),
          })
          .execute()
        break
      }
      case IdRegisterEventType.NONE:
        throw new Error(`Invalid IdRegisterEventType: ${body.eventType}`)
      default:
        // If we're getting a type error on the line below, it means we've missed a case above.
        // Did we add a new event type?
        throw new Error(`Invalid IdRegisterEventType: ${body.eventType}`)
    }
  }
}
