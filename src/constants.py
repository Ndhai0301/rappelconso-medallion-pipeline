import os


PATH_LAST_PROCESSED = "./data/last_processed.json"
MAX_LIMIT = int(os.getenv("API_PAGE_SIZE", "100"))
MAX_OFFSET = int(os.getenv("API_MAX_OFFSET", "10000"))

URL_API = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "rappelconso-v2-gtin-espaces/records"
    f"?limit={MAX_LIMIT}"
    "&where=date_publication%20%3E%20'{}'"
    "&order_by=date_publication%20ASC"
    "&offset={}"
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "rappelconso")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "dev.rappelconso_raw")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin123"),
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

DB_FIELDS = [
    "api_id",
    "rappel_guid",
    "numero_fiche",
    "numero_version",
    "date_publication",
    "nature_juridique_rappel",
    "categorie_produit",
    "sous_categorie_produit",
    "marque_produit",
    "libelle",
    "modeles_ou_references",
    "identification_produits",
    "conditionnements",
    "date_debut_commercialisation",
    "date_fin_commercialisation",
    "temperature_conservation",
    "marque_salubrite",
    "informations_complementaires",
    "zone_geographique_de_vente",
    "distributeurs",
    "motif_rappel",
    "risques_encourus",
    "preconisations_sanitaires",
    "description_complementaire_risque",
    "conduites_a_tenir_par_le_consommateur",
    "numero_contact",
    "modalites_de_compensation",
    "date_de_fin_de_la_procedure_de_rappel",
    "informations_complementaires_publiques",
    "liens_vers_les_images",
    "lien_vers_la_liste_des_produits",
    "lien_vers_la_liste_des_distributeurs",
    "lien_vers_affichette_pdf",
    "lien_vers_la_fiche_rappel",
    "raw_data",
]
