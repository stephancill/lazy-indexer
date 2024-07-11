import { Kysely, sql } from 'kysely'

export const up = async (db: Kysely<any>) => {
  // Users view
  await db.schema
    .createView('users')
    .orReplace()
    .as(
      sql`
        SELECT 
            ud.fid,
            MAX(CASE WHEN ud.type = 1 THEN ud.value END) AS pfp,
            MAX(CASE WHEN ud.type = 2 THEN ud.value END) AS display,
            MAX(CASE WHEN ud.type = 3 THEN ud.value END) AS bio,
            MAX(CASE WHEN ud.type = 5 THEN ud.value END) AS url,
            MAX(CASE WHEN ud.type = 6 THEN ud.value END) AS username
        FROM 
            user_data ud
        GROUP BY 
            ud.fid;
      `
    )
    .execute()

  // Casts with metadata view
  await db.schema
    .createView('casts_enhanced')
    .orReplace()
    .as(
      sql`
        WITH p AS (
          SELECT
            fid,
            MAX(CASE WHEN type = 1 THEN value END) AS pfp,
            MAX(CASE WHEN type = 2 THEN value END) AS display,
            MAX(CASE WHEN type = 6 THEN value END) AS username
          FROM user_data
          GROUP BY fid
        )
        SELECT
          c.fid,
          c.hash,
          c.parent_fid,
          c.parent_url,
          c.parent_hash,
          c.root_parent_url,
          c.root_parent_hash,
          c.timestamp,
          c.text,
          c.embeds,
          c.mentions,
          c.mentions_positions,
          p.pfp AS author_pfp,
          p.display AS author_display,
          p.username AS author_username
        FROM
          casts c
          JOIN p ON c.fid = p.fid
        WHERE c.deleted_at IS NULL
      `
    )
    .execute()
}

export const down = async (db: Kysely<any>) => {
  await db.schema.dropView('users').ifExists().execute()
  await db.schema.dropView('casts_enhanced').ifExists().execute()
}
