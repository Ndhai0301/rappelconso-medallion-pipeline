from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType


RAPPELCONSO_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("rappel_guid", StringType()),
        StructField("numero_fiche", StringType()),
        StructField("numero_version", IntegerType()),
        StructField("date_publication", StringType()),
        StructField("nature_juridique_rappel", StringType()),
        StructField("categorie_produit", StringType()),
        StructField("sous_categorie_produit", StringType()),
        StructField("marque_produit", StringType()),
        StructField("libelle", StringType()),
        StructField("modeles_ou_references", StringType()),
        StructField("identification_produits", ArrayType(StringType())),
        StructField("conditionnements", StringType()),
        StructField("date_debut_commercialisation", StringType()),
        StructField("date_date_fin_commercialisation", StringType()),
        StructField("temperature_conservation", StringType()),
        StructField("marque_salubrite", StringType()),
        StructField("informations_complementaires", StringType()),
        StructField("zone_geographique_de_vente", StringType()),
        StructField("distributeurs", StringType()),
        StructField("motif_rappel", StringType()),
        StructField("risques_encourus", StringType()),
        StructField("preconisations_sanitaires", StringType()),
        StructField("description_complementaire_risque", StringType()),
        StructField("conduites_a_tenir_par_le_consommateur", StringType()),
        StructField("numero_contact", StringType()),
        StructField("modalites_de_compensation", StringType()),
        StructField("date_de_fin_de_la_procedure_de_rappel", StringType()),
        StructField("informations_complementaires_publiques", StringType()),
        StructField("liens_vers_les_images", StringType()),
        StructField("lien_vers_la_liste_des_produits", StringType()),
        StructField("lien_vers_la_liste_des_distributeurs", StringType()),
        StructField("lien_vers_affichette_pdf", StringType()),
        StructField("lien_vers_la_fiche_rappel", StringType()),
        StructField("raw_ingested_at_utc", StringType()),
    ]
)

TEXT_COLUMNS = [
    "nature_juridique_rappel",
    "categorie_produit",
    "sous_categorie_produit",
    "marque_produit",
    "libelle",
    "modeles_ou_references",
    "conditionnements",
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
    "modalites_de_compensation",
    "informations_complementaires_publiques",
]


def parse_kafka_json(dataframe: DataFrame) -> DataFrame:
    json_df = dataframe.select(F.col("value").cast("string").alias("raw_json"))
    parsed_df = json_df.withColumn(
        "data", F.from_json(F.col("raw_json"), RAPPELCONSO_SCHEMA)
    )
    return parsed_df.select("data.*", "raw_json").where(F.col("id").isNotNull())


def normalize_text_columns(dataframe: DataFrame) -> DataFrame:
    normalized_df = dataframe
    for column in TEXT_COLUMNS:
        trimmed = F.trim(F.col(column))
        normalized_df = normalized_df.withColumn(
            column,
            F.when(F.col(column).isNull() | (trimmed == ""), None).otherwise(
                F.lower(trimmed)
            ),
        )
    return normalized_df


def cast_date_columns(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.withColumn("date_publication", F.to_timestamp("date_publication"))
        .withColumn("raw_ingested_at_utc", F.to_timestamp("raw_ingested_at_utc"))
        .withColumn(
            "date_debut_commercialisation",
            F.to_date("date_debut_commercialisation"),
        )
        .withColumn(
            "date_fin_commercialisation",
            F.to_date("date_date_fin_commercialisation"),
        )
        .withColumn(
            "date_de_fin_de_la_procedure_de_rappel",
            F.to_date("date_de_fin_de_la_procedure_de_rappel"),
        )
        .drop("date_date_fin_commercialisation")
    )


def prepare_for_postgres(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn(
        "identification_produits", F.to_json("identification_produits")
    )


def transform_rappelconso_data(dataframe: DataFrame) -> DataFrame:
    parsed_df = parse_kafka_json(dataframe)
    normalized_df = normalize_text_columns(parsed_df)
    dated_df = cast_date_columns(normalized_df)
    return prepare_for_postgres(dated_df)
