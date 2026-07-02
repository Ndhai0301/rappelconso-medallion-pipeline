"""Idempotently complete Metabase's first-run setup and wire up the Gold
schema: creates the admin account and site name (if not already done),
connects a Postgres database scoped to the gold schema, and publishes a
starter dashboard so BI is usable immediately after `docker compose up`.
"""

import logging
import os
import sys

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000")
METABASE_ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@example.com")
METABASE_ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "Rappelconso123!")
METABASE_SITE_NAME = os.getenv("METABASE_SITE_NAME", "RappelConso Warehouse")
METABASE_DB_NAME = "RappelConso Gold"
DASHBOARD_NAME = "RappelConso - Vue d'ensemble"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "rappelconso")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

CARDS = [
    {
        "name": "Rappels par catégorie",
        "sql": (
            "SELECT COALESCE(p.categorie_produit, 'Unknown') AS categorie, "
            "COUNT(*) AS nb_rappels "
            "FROM gold.fact_rappel f "
            "JOIN gold.dim_produit p ON p.produit_key = f.produit_key "
            "WHERE f.is_current "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 15"
        ),
        "display": "bar",
        "visualization_settings": {
            "graph.dimensions": ["categorie"],
            "graph.metrics": ["nb_rappels"],
        },
    },
    {
        "name": "Évolution des rappels par mois",
        "sql": (
            "SELECT d.year, d.month, COUNT(*) AS nb_rappels "
            "FROM gold.fact_rappel f "
            "JOIN gold.dim_date d ON d.date_key = f.date_key "
            "WHERE f.is_current "
            "GROUP BY 1, 2 ORDER BY 1, 2"
        ),
        "display": "line",
        "visualization_settings": {
            "graph.dimensions": ["year", "month"],
            "graph.metrics": ["nb_rappels"],
        },
    },
    {
        "name": "Top 10 distributeurs par nombre de rappels",
        "sql": (
            "SELECT dist.distributeurs, COUNT(*) AS nb_rappels "
            "FROM gold.fact_rappel f "
            "JOIN gold.dim_distributeur dist ON dist.distributeur_key = f.distributeur_key "
            "WHERE f.is_current AND dist.distributeurs <> 'Unknown' "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
        ),
        "display": "bar",
        "visualization_settings": {
            "graph.dimensions": ["distributeurs"],
            "graph.metrics": ["nb_rappels"],
        },
    },
    {
        "name": "Total rappels actifs",
        "sql": "SELECT COUNT(*) AS total_rappels FROM gold.fact_rappel WHERE is_current",
        "display": "scalar",
        "visualization_settings": {},
    },
]

DASHCARD_POSITIONS = [
    {"col": 0, "row": 0, "size_x": 6, "size_y": 4},
    {"col": 6, "row": 0, "size_x": 6, "size_y": 4},
    {"col": 0, "row": 4, "size_x": 8, "size_y": 4},
    {"col": 8, "row": 4, "size_x": 4, "size_y": 4},
]


def wait_for_metabase(timeout_seconds: int = 120) -> None:
    import time

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise RuntimeError(f"Metabase did not become healthy within {timeout_seconds}s")


def get_session_properties() -> dict:
    response = requests.get(f"{METABASE_URL}/api/session/properties", timeout=10)
    response.raise_for_status()
    return response.json()


def ensure_admin_session() -> str:
    properties = get_session_properties()
    if not properties.get("has-user-setup"):
        logging.info("Metabase not yet set up; running first-run setup")
        response = requests.post(
            f"{METABASE_URL}/api/setup",
            json={
                "token": properties["setup-token"],
                "user": {
                    "first_name": "Admin",
                    "last_name": "User",
                    "email": METABASE_ADMIN_EMAIL,
                    "password": METABASE_ADMIN_PASSWORD,
                    "site_name": METABASE_SITE_NAME,
                },
                "prefs": {
                    "site_name": METABASE_SITE_NAME,
                    "site_locale": "en",
                    "allow_tracking": False,
                },
            },
            timeout=30,
        )
        response.raise_for_status()
        logging.info("Metabase admin account and site created")
        return response.json()["id"]

    logging.info("Metabase already set up; logging in")
    response = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_ADMIN_EMAIL, "password": METABASE_ADMIN_PASSWORD},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()["id"]


def ensure_database(session: requests.Session) -> int:
    response = session.get(f"{METABASE_URL}/api/database", timeout=15)
    response.raise_for_status()
    for database in response.json().get("data", []):
        if database["name"] == METABASE_DB_NAME:
            logging.info("Database '%s' already connected (id=%s)", METABASE_DB_NAME, database["id"])
            return database["id"]

    logging.info("Connecting Postgres gold schema as '%s'", METABASE_DB_NAME)
    response = session.post(
        f"{METABASE_URL}/api/database",
        json={
            "engine": "postgres",
            "name": METABASE_DB_NAME,
            "details": {
                "host": POSTGRES_HOST,
                "port": POSTGRES_PORT,
                "dbname": POSTGRES_DB,
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": "gold",
                "ssl": False,
            },
            "is_full_sync": True,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["id"]


def ensure_dashboard(session: requests.Session, database_id: int) -> None:
    response = session.get(f"{METABASE_URL}/api/dashboard", timeout=15)
    response.raise_for_status()
    if any(d["name"] == DASHBOARD_NAME for d in response.json()):
        logging.info("Dashboard '%s' already exists", DASHBOARD_NAME)
        return

    card_ids = []
    for card in CARDS:
        response = session.post(
            f"{METABASE_URL}/api/card",
            json={
                "name": card["name"],
                "dataset_query": {
                    "type": "native",
                    "native": {"query": card["sql"]},
                    "database": database_id,
                },
                "display": card["display"],
                "visualization_settings": card["visualization_settings"],
            },
            timeout=30,
        )
        response.raise_for_status()
        card_ids.append(response.json()["id"])
        logging.info("Created card '%s'", card["name"])

    response = session.post(
        f"{METABASE_URL}/api/dashboard",
        json={"name": DASHBOARD_NAME, "description": "Dashboard demo sur le Gold layer"},
        timeout=15,
    )
    response.raise_for_status()
    dashboard_id = response.json()["id"]

    dashcards = [
        {"id": -1 - index, "card_id": card_id, **position}
        for index, (card_id, position) in enumerate(zip(card_ids, DASHCARD_POSITIONS))
    ]
    response = session.put(
        f"{METABASE_URL}/api/dashboard/{dashboard_id}",
        json={"dashcards": dashcards},
        timeout=15,
    )
    response.raise_for_status()
    logging.info("Dashboard '%s' created with %s cards", DASHBOARD_NAME, len(card_ids))


def main() -> None:
    wait_for_metabase()
    token = ensure_admin_session()

    session = requests.Session()
    session.headers.update({"X-Metabase-Session": token})

    database_id = ensure_database(session)
    ensure_dashboard(session, database_id)
    logging.info("Metabase setup complete: %s", METABASE_URL)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Metabase setup failed")
        sys.exit(1)
