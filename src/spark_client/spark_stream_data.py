import logging
import os
import uuid

import psycopg2
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.constants import (
    BRONZE_TABLE,
    DB_FIELDS,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_PROPERTIES,
    POSTGRES_URL,
    SILVER_TABLE,
)
from src.postgres_client.migrate import run_migrations
from src.spark_client.transformation import transform_rappelconso_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s",
)

APP_NAME = "france-rappelconso-spark"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rappelconso_raw")
STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "earliest")
CHECKPOINT_LOCATION = os.getenv(
    "SPARK_CHECKPOINT_LOCATION",
    "./data/checkpoints/rappelconso_to_postgres",
)
DEDUPLICATION_KEY = "deduplication_key"
JSONB_COLUMNS = frozenset({"identification_produits", "raw_data"})
RUN_ID = uuid.uuid4().hex[:12]


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.5.4,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    logging.info("Spark session created successfully")
    return spark


def create_initial_dataframe(spark: SparkSession) -> DataFrame:
    try:
        dataframe = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", STARTING_OFFSETS)
            .load()
        )
        logging.info("Kafka streaming dataframe created successfully")
        return dataframe
    except Exception:
        logging.exception("Initial dataframe could not be created")
        raise


def create_final_dataframe(dataframe: DataFrame) -> DataFrame:
    transformed_df = transform_rappelconso_data(dataframe)
    return transformed_df.select(
        F.col("id").alias("api_id"),
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
        "raw_json",
        F.col("raw_json").alias("raw_data"),
    )


def add_deduplication_key(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn(
        DEDUPLICATION_KEY,
        F.coalesce(
            F.col("rappel_guid"),
            F.col("numero_fiche"),
            F.col("api_id").cast("string"),
        ),
    ).where(F.col(DEDUPLICATION_KEY).isNotNull())


def select_latest_versions(dataframe: DataFrame) -> DataFrame:
    window = Window.partitionBy(DEDUPLICATION_KEY).orderBy(
        F.col("numero_version").desc_nulls_last(),
        F.col("date_publication").desc_nulls_last(),
        F.col("api_id").desc_nulls_last(),
    )
    return (
        dataframe.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def _pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_PROPERTIES["user"],
        password=POSTGRES_PROPERTIES["password"],
    )


def write_bronze(batch_df: DataFrame, batch_id: int) -> None:
    bronze_df = batch_df.select(
        DEDUPLICATION_KEY,
        F.col("api_id"),
        "rappel_guid",
        "numero_fiche",
        "numero_version",
        "date_publication",
        "raw_json",
        F.lit(f"{RUN_ID}_{batch_id}").alias("_batch_id"),
    )
    bronze_df.write.jdbc(
        POSTGRES_URL,
        BRONZE_TABLE,
        mode="append",
        properties=POSTGRES_PROPERTIES,
    )
    logging.info("Batch %s appended to %s", batch_id, BRONZE_TABLE)


def _drop_staging(staging: str) -> None:
    try:
        with _pg_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {staging};")
    except Exception:
        logging.exception("Could not clean up staging table %s", staging)


def build_upsert_sql(staging: str) -> str:
    write_columns = list(DB_FIELDS) + [DEDUPLICATION_KEY]
    column_list = ", ".join(write_columns)
    select_list = ", ".join(
        f"{column}::jsonb" if column in JSONB_COLUMNS else column
        for column in write_columns
    )
    update_set = ", ".join(
        f"{column} = EXCLUDED.{column}" for column in DB_FIELDS
    )
    update_set += ", silver_updated_at = now()"
    return f"""
        INSERT INTO {SILVER_TABLE} ({column_list})
        SELECT {select_list} FROM {staging}
        ON CONFLICT ({DEDUPLICATION_KEY}) DO UPDATE SET {update_set}
        WHERE {SILVER_TABLE}.numero_version IS NULL
           OR (EXCLUDED.numero_version IS NOT NULL
               AND EXCLUDED.numero_version >= {SILVER_TABLE}.numero_version);
    """


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    batch_with_key = add_deduplication_key(batch_df)
    if batch_with_key.isEmpty():
        logging.info("Batch %s is empty", batch_id)
        return

    staging = f"silver.rappelconso_staging_{RUN_ID}_{batch_id}"
    write_columns = list(DB_FIELDS) + [DEDUPLICATION_KEY]

    try:
        write_bronze(batch_with_key, batch_id)

        batch_deduped = select_latest_versions(batch_with_key)
        batch_deduped.select(*write_columns).write.jdbc(
            POSTGRES_URL,
            staging,
            mode="overwrite",
            properties=POSTGRES_PROPERTIES,
        )
        with _pg_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(build_upsert_sql(staging))
        logging.info("Batch %s upserted to %s", batch_id, SILVER_TABLE)
    except Exception:
        logging.exception("Batch %s failed", batch_id)
        raise
    finally:
        _drop_staging(staging)


def start_streaming(dataframe: DataFrame) -> None:
    logging.info("Starting Kafka to PostgreSQL stream")
    query = (
        dataframe.writeStream.foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    logging.info("Kafka to PostgreSQL stream finished")


def write_to_postgres() -> None:
    run_migrations()
    spark = create_spark_session()
    try:
        kafka_df = create_initial_dataframe(spark)
        final_df = create_final_dataframe(kafka_df)
        start_streaming(final_df)
    finally:
        spark.stop()


if __name__ == "__main__":
    write_to_postgres()
