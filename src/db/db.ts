import 'dotenv/config'
import { CamelCasePlugin, Kysely, PostgresDialect } from 'kysely'
import pg from 'pg'

import { Tables } from './db.types'

const { Pool } = pg

export const getDbClient = (
  connectionString: string | undefined = process.env.DATABASE_URL
) => {
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set')
  }

  return new Kysely<Tables>({
    dialect: new PostgresDialect({
      pool: new Pool({
        max: 20,
        connectionString,
      }),
    }),
    plugins: [new CamelCasePlugin()],
    log: ['error'],
  })
}

export const db = getDbClient()

// export const db = new Kysely<Tables>({
//   dialect: new PostgresDialect({
//     pool: new Pool({
//       connectionString: process.env.DATABASE_URL,
//       log(...messages) {
//         console.log('DB:', ...messages)
//       },
//     }),
//   }),
//   plugins: [new CamelCasePlugin()],
// })
