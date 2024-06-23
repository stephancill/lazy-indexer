import { Kysely, sql } from 'kysely'

export const up = async (db: Kysely<any>) => {
  // SYNC TARGETS (not used yet)
  await db.schema
    .createTable('targets')
    .ifNotExists()
    .addColumn('id', 'uuid', (col) =>
      col.defaultTo(sql`generate_ulid()`).primaryKey()
    )
    .addColumn('createdAt', 'timestamptz', (col) =>
      col.notNull().defaultTo(sql`current_timestamp`)
    )
    .addColumn('updatedAt', 'timestamptz', (col) =>
      col.notNull().defaultTo(sql`current_timestamp`)
    )
    .addColumn('fid', 'bigint', (col) => col.notNull())
    .addUniqueConstraint('target_fid_unique', ['fid'])
    .execute()
}

export const down = async (db: Kysely<any>) => {
  await db.schema.dropTable('targets').ifExists().execute()
}
