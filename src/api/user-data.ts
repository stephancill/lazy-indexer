import { Message } from '@farcaster/hub-nodejs'

import { db } from '../db/db.js'
import { log } from '../lib/logger.js'
import { formatUserDatas } from '../lib/utils.js'

export async function insertUserDatas(msgs: Message[]) {
  const userDatas = formatUserDatas(msgs)
  if (userDatas.length === 0) return

  try {
    await db
      .insertInto('userData')
      .values(userDatas)
      .onConflict((oc) =>
        oc.columns(['fid', 'type']).doUpdateSet((eb) => ({
          hash: eb.ref('excluded.hash'),
          value: eb.ref('excluded.value'),
        }))
      )
      .execute()

    log.debug(`USER DATA INSERTED`)
  } catch (error) {
    log.error(error, 'ERROR INSERTING USER DATA')
    throw error
  }
}
