CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS snapshots;

-- Bronze: append-only raw events. No unique constraint on deduplication_key:
-- every message (including older versions and replays) becomes its own row.
CREATE TABLE IF NOT EXISTS bronze.rappelconso_events (
    bronze_id SERIAL PRIMARY KEY,
    deduplication_key TEXT,
    api_id INTEGER,
    rappel_guid TEXT,
    numero_fiche TEXT,
    numero_version INTEGER,
    date_publication TIMESTAMPTZ,
    raw_json JSONB NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT
);

CREATE INDEX IF NOT EXISTS rappelconso_events_dedup_key_idx
    ON bronze.rappelconso_events (deduplication_key);

CREATE INDEX IF NOT EXISTS rappelconso_events_ingested_at_idx
    ON bronze.rappelconso_events (_ingested_at DESC);

-- Silver: cleaned, typed, deduplicated current-state (ODS semantics),
-- same shape as the former dev.rappelconso_raw plus silver_updated_at,
-- which the dbt snapshot's timestamp strategy uses to detect changes.
CREATE TABLE IF NOT EXISTS silver.rappelconso_clean (
    silver_id SERIAL PRIMARY KEY,
    api_id INTEGER,
    rappel_guid TEXT,
    numero_fiche TEXT,
    numero_version INTEGER,
    date_publication TIMESTAMPTZ,
    nature_juridique_rappel TEXT,
    categorie_produit TEXT,
    sous_categorie_produit TEXT,
    marque_produit TEXT,
    libelle TEXT,
    modeles_ou_references TEXT,
    identification_produits JSONB,
    conditionnements TEXT,
    date_debut_commercialisation DATE,
    date_fin_commercialisation DATE,
    temperature_conservation TEXT,
    marque_salubrite TEXT,
    informations_complementaires TEXT,
    zone_geographique_de_vente TEXT,
    distributeurs TEXT,
    motif_rappel TEXT,
    risques_encourus TEXT,
    preconisations_sanitaires TEXT,
    description_complementaire_risque TEXT,
    conduites_a_tenir_par_le_consommateur TEXT,
    numero_contact TEXT,
    modalites_de_compensation TEXT,
    date_de_fin_de_la_procedure_de_rappel DATE,
    informations_complementaires_publiques TEXT,
    liens_vers_les_images TEXT,
    lien_vers_la_liste_des_produits TEXT,
    lien_vers_la_liste_des_distributeurs TEXT,
    lien_vers_affichette_pdf TEXT,
    lien_vers_la_fiche_rappel TEXT,
    raw_data JSONB,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deduplication_key TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS rappelconso_clean_dedup_key_idx
    ON silver.rappelconso_clean (deduplication_key);

CREATE INDEX IF NOT EXISTS rappelconso_clean_publication_date_idx
    ON silver.rappelconso_clean (date_publication DESC);

CREATE INDEX IF NOT EXISTS rappelconso_clean_category_idx
    ON silver.rappelconso_clean (categorie_produit);

-- Pipeline run audit log (bronze/silver/gold row counts, status, duration).
CREATE TABLE IF NOT EXISTS audit_pipeline_runs (
    audit_id SERIAL PRIMARY KEY,
    dag_run_id TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration_seconds NUMERIC,
    status TEXT NOT NULL,
    bronze_row_count BIGINT,
    silver_row_count BIGINT,
    gold_row_count BIGINT,
    details JSONB
);

CREATE INDEX IF NOT EXISTS audit_pipeline_runs_started_at_idx
    ON audit_pipeline_runs (started_at DESC);
