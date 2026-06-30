ALTER TABLE dev.rappelconso_raw
    ADD COLUMN IF NOT EXISTS deduplication_key TEXT;

UPDATE dev.rappelconso_raw
SET deduplication_key = COALESCE(rappel_guid, numero_fiche, CAST(api_id AS TEXT))
WHERE deduplication_key IS NULL;

DELETE FROM dev.rappelconso_raw
WHERE deduplication_key IS NOT NULL
  AND db_id NOT IN (
      SELECT DISTINCT ON (deduplication_key) db_id
      FROM dev.rappelconso_raw
      WHERE deduplication_key IS NOT NULL
      ORDER BY deduplication_key, numero_version DESC NULLS LAST, db_id DESC
  );

CREATE UNIQUE INDEX IF NOT EXISTS rappelconso_raw_dedup_key_idx
    ON dev.rappelconso_raw (deduplication_key);

CREATE INDEX IF NOT EXISTS rappelconso_raw_publication_date_idx
    ON dev.rappelconso_raw (date_publication DESC);

CREATE INDEX IF NOT EXISTS rappelconso_raw_category_idx
    ON dev.rappelconso_raw (categorie_produit);
