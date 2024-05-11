-- Opinionated additions to the schema for Searchcaster

-- Generate a tsvector column for the casts table
ALTER TABLE casts
	ADD COLUMN fts tsvector GENERATED always AS (
to_tsvector('english', text)) stored;

-- Create an index on the fts column for faster searching
CREATE INDEX casts_fts ON casts
USING GIN (fts);

-- Update the casts_enhanced view to include the fts column
CREATE OR REPLACE VIEW casts_enhanced AS (
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
		p.username AS author_username,
		c.fts
	FROM
		casts c
		JOIN p ON c.fid = p.fid
	WHERE c.deleted_at IS NULL
);

-- Create a search function
CREATE OR REPLACE FUNCTION search_casts (query text)
	RETURNS SETOF casts_enhanced
	AS $$
BEGIN
	RETURN QUERY
	SELECT
		*
	FROM
		casts_enhanced
	WHERE
		fts @@ to_tsquery(query);
END;
$$
LANGUAGE plpgsql;
