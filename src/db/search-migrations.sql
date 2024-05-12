-- Opinionated additions to the schema for Searchcaster

-- Generate a tsvector column for the casts table
ALTER TABLE casts
	ADD COLUMN fts tsvector GENERATED always AS (
to_tsvector('english', text)) stored;

-- Create an index on the fts column for faster searching
CREATE INDEX casts_fts ON casts
USING GIN (fts);

-- Create a search function
CREATE OR REPLACE FUNCTION search_casts (query text)
	RETURNS SETOF casts
	AS $$
BEGIN
	RETURN QUERY
	SELECT
		*
	FROM
		casts
	WHERE
		fts @@ to_tsquery(query);
END;
$$
LANGUAGE plpgsql;
