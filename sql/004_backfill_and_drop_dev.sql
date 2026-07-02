-- One-time migration of the former dev.rappelconso_raw table into
-- bronze/silver, then drop it. Guarded by to_regclass so it is a permanent
-- no-op after the first successful run (and a harmless no-op on a brand-new
-- volume, where dev.rappelconso_raw is created empty by init.sql just before
-- this file runs).
DO $$
BEGIN
    IF to_regclass('dev.rappelconso_raw') IS NOT NULL THEN
        INSERT INTO bronze.rappelconso_events (
            deduplication_key, api_id, rappel_guid, numero_fiche,
            numero_version, date_publication, raw_json, _ingested_at, _batch_id
        )
        SELECT
            deduplication_key, api_id, rappel_guid, numero_fiche,
            numero_version, date_publication, COALESCE(raw_data, '{}'::jsonb),
            inserted_at, 'backfill_dev'
        FROM dev.rappelconso_raw;

        INSERT INTO silver.rappelconso_clean (
            api_id, rappel_guid, numero_fiche, numero_version, date_publication,
            nature_juridique_rappel, categorie_produit, sous_categorie_produit,
            marque_produit, libelle, modeles_ou_references, identification_produits,
            conditionnements, date_debut_commercialisation, date_fin_commercialisation,
            temperature_conservation, marque_salubrite, informations_complementaires,
            zone_geographique_de_vente, distributeurs, motif_rappel, risques_encourus,
            preconisations_sanitaires, description_complementaire_risque,
            conduites_a_tenir_par_le_consommateur, numero_contact, modalites_de_compensation,
            date_de_fin_de_la_procedure_de_rappel, informations_complementaires_publiques,
            liens_vers_les_images, lien_vers_la_liste_des_produits,
            lien_vers_la_liste_des_distributeurs, lien_vers_affichette_pdf,
            lien_vers_la_fiche_rappel, raw_data, inserted_at, silver_updated_at,
            deduplication_key
        )
        SELECT
            api_id, rappel_guid, numero_fiche, numero_version, date_publication,
            nature_juridique_rappel, categorie_produit, sous_categorie_produit,
            marque_produit, libelle, modeles_ou_references, identification_produits,
            conditionnements, date_debut_commercialisation, date_fin_commercialisation,
            temperature_conservation, marque_salubrite, informations_complementaires,
            zone_geographique_de_vente, distributeurs, motif_rappel, risques_encourus,
            preconisations_sanitaires, description_complementaire_risque,
            conduites_a_tenir_par_le_consommateur, numero_contact, modalites_de_compensation,
            date_de_fin_de_la_procedure_de_rappel, informations_complementaires_publiques,
            liens_vers_les_images, lien_vers_la_liste_des_produits,
            lien_vers_la_liste_des_distributeurs, lien_vers_affichette_pdf,
            lien_vers_la_fiche_rappel, raw_data, inserted_at, inserted_at,
            deduplication_key
        FROM dev.rappelconso_raw
        ON CONFLICT (deduplication_key) DO NOTHING;

        DROP TABLE dev.rappelconso_raw;
        DROP SCHEMA dev;
    END IF;
END $$;
