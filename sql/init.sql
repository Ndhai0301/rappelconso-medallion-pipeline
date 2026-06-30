CREATE SCHEMA IF NOT EXISTS dev;

CREATE TABLE IF NOT EXISTS dev.rappelconso_raw (
    db_id SERIAL PRIMARY KEY,
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
    deduplication_key TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS rappelconso_raw_dedup_key_idx
    ON dev.rappelconso_raw (deduplication_key);

CREATE INDEX IF NOT EXISTS rappelconso_raw_publication_date_idx
    ON dev.rappelconso_raw (date_publication DESC);

CREATE INDEX IF NOT EXISTS rappelconso_raw_category_idx
    ON dev.rappelconso_raw (categorie_produit);
