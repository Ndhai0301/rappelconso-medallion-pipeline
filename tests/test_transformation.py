import json

from src.spark_client.transformation import transform_rappelconso_data


def test_transform_rappelconso_payload(spark):
    payload = {
        "id": 42,
        "rappel_guid": "guid-42",
        "numero_fiche": "2026-0042",
        "numero_version": 2,
        "date_publication": "2026-06-29T12:30:00+00:00",
        "categorie_produit": "  Alimentation  ",
        "libelle": "Crème Brûlée",
        "identification_produits": ["123", "LOT-A"],
        "date_debut_commercialisation": "2026-01-02",
        "date_date_fin_commercialisation": "2026-03-04",
        "raw_ingested_at_utc": "2026-06-29T12:31:00+00:00",
    }
    dataframe = spark.createDataFrame(
        [(json.dumps(payload),), ("null",), ("not-json",)], ["value"]
    )

    rows = transform_rappelconso_data(dataframe).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.categorie_produit == "alimentation"
    assert row.libelle == "crème brûlée"
    assert row.identification_produits == '["123","LOT-A"]'
    assert str(row.date_fin_commercialisation) == "2026-03-04"
    assert row.raw_json == json.dumps(payload)


def test_blank_text_becomes_null(spark):
    payload = {"id": 1, "categorie_produit": "   "}
    dataframe = spark.createDataFrame([(json.dumps(payload),)], ["value"])
    row = transform_rappelconso_data(dataframe).collect()[0]
    assert row.categorie_produit is None
