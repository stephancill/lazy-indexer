import { ContactInfoContentBody } from '@farcaster/hub-nodejs'

import { db } from '../db/kysely.js'
import { log } from '../lib/logger.js'
import { breakIntoChunks } from '../lib/utils.js'

/**
 * Insert hubs in the database
 * @param msg List of connected peers
 */
export async function insertHubs(hubs: ContactInfoContentBody[]) {
  if (hubs.length === 0) return

  const chunks = breakIntoChunks(hubs, 1000)

  for (const chunk of chunks) {
    try {
      await db
        .insertInto('hubs')
        .values(chunk)
        .onConflict((oc) => oc.column('id').doNothing())
        .execute()

      log.debug(`HUBS INSERTED`)
    } catch (error) {
      log.error(error, 'ERROR INSERTING HUBS')
    }
  }
}
