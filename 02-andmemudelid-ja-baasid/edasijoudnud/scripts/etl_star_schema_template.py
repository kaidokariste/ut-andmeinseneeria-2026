"""
Ulesanne 3: Python ETL - OLTP allikast star schemasse

See skript laadib denormaliseeritud muugiandmed CSV failist,
transformeerib need dimensioonideks ja faktideks ning laadib
PostgreSQL star schemasse.

Nouded:
  - Skript peab olema idempotentne (korduv kaivitamine ei tekita duplikaate)
  - ETL jooksude logimine etl_log tabelisse
  - Andmebaasi uhenduse parameetrid keskkonnamuutujatest
  - Veakasitlus: vead logitakse etl_log tabelisse

Kaivitamine:
  docker exec -it praktikum-python-02 python /scripts/etl_star_schema_template.py
"""

import csv
import os
import time
from datetime import datetime

import psycopg2


# --- Seadistus ---

CSV_PATH = "/data/source_sales.csv"

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "db"),
    "dbname": os.environ.get("POSTGRES_DB", "praktikum"),
    "user": os.environ.get("POSTGRES_USER", "praktikum"),
    "password": os.environ.get("POSTGRES_PASSWORD", "praktikum"),
}


def get_connection():
    """Loo andmebaasi uhendus."""
    return psycopg2.connect(**DB_CONFIG)


def extract(csv_path: str) -> list[dict]:
    """
    EXTRACT: Loe CSV fail ja tagasta list of dict.

    Vihje: kasuta csv.DictReader moodulit.
    Tagasta koik read listina, kus iga element on dict.

    Naide:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    """
    # TEIE LAHENDUS SIIA
    pass


def transform_dimensions(rows: list[dict]) -> dict:
    """
    TRANSFORM: Eralda unikaalsed dimensioonide vaartused.

    Tagasta dict kujul:
    {
        'dates': [{'date_key': 20250901, 'full_date': '2025-09-01', 'year': 2025, 'quarter': 3, 'month': 9, 'day': 1, 'day_of_week': 'Monday', 'day_of_year': 244, 'week_of_year': 35, 'month_name': 'September', 'is_weekend': False}, ...],
        'stores': [{'store_name': '...', 'city': '...', 'region': '...'}, ...],
        'products': [{'product_name': '...', 'category': '...', 'brand': '...'}, ...],
        'customers': [{'customer_id': 1, 'first_name': '...', 'last_name': '...', 'segment': '...', 'city': '...'}, ...],
        'payments': [{'payment_type': '...'}, ...],
    }

    Vihjed:
      - Kasuta set() voi dict() unikaalsuse tagamiseks
      - customer_name tuleb jagada first_name ja last_name osadeks (split)
      - Kuupaeva nadalapaev: kasuta datetime moodulit
    """
    # TEIE LAHENDUS SIIA
    pass


def transform_facts(rows: list[dict]) -> list[dict]:
    """
    TRANSFORM: Valmista ette faktitabeli read.

    Tagasta list of dict kujul:
    [
        {
            'order_date': '2025-09-01',
            'store_name': '...',
            'product_name': '...',
            'customer_id': 1,
            'payment_type': '...',
            'quantity': 5,
            'unit_price': 1.20,
            'total_amount': 6.00,
        },
        ...
    ]

    Vihje: teisenda stringid oigeteks tuupideks (int, float).
    """
    # TEIE LAHENDUS SIIA
    pass


def load(conn, dimensions: dict, facts: list[dict]) -> int:
    """
    LOAD: Laadi dimensioonid ja faktid PostgreSQL-i.

    Sammud:
      1. TRUNCATE koik tabelid (idempotentsus)
      2. INSERT dimensioonid (DimDate, DimStore, DimProduct, DimCustomer, DimPayment)
      3. Loe tagasi surrogate key'd (SELECT ... FROM Dim...)
      4. INSERT faktid (FactSales) - asenda allikvoeertused surrogate key'dega
      5. Tagasta laaditud faktiridade arv

    Vihjed:
      - Kasuta cur.executemany() mitme rea sisestamiseks
      - VÕI kasuta cur.execute() tsuklis
      - Pärast dimensioonide laadimist loe surrogate key'd:
          cur.execute("SELECT CustomerKey, CustomerID FROM DimCustomer")
          customer_keys = {row[1]: row[0] for row in cur.fetchall()}
      - conn.commit() peale koiki INSERT lauseid
    """
    # TEIE LAHENDUS SIIA
    pass


def ensure_etl_log_table(conn):
    """Loo etl_log tabel kui see puudub."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds NUMERIC(10,2),
            rows_loaded INT,
            status VARCHAR(20),
            error_message TEXT
        )
    """)
    conn.commit()
    cur.close()


def log_etl_run(conn, start_time, end_time, rows_loaded, status, error_message=None):
    """Logi ETL jooks etl_log tabelisse."""
    duration = round((end_time - start_time).total_seconds(), 2)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO etl_log (start_time, end_time, duration_seconds, rows_loaded, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (start_time, end_time, duration, rows_loaded, status, error_message),
    )
    conn.commit()
    cur.close()


def main():
    """Peamine ETL voog."""
    print("=" * 50)
    print("ETL: OLTP allikas -> Star Schema")
    print("=" * 50)

    conn = get_connection()
    ensure_etl_log_table(conn)
    start_time = datetime.now()
    rows_loaded = 0

    try:
        # Extract
        print("\n[1/4] EXTRACT: loen CSV faili...")
        raw_rows = extract(CSV_PATH)
        print(f"      Loetud {len(raw_rows)} rida")

        # Transform
        print("\n[2/4] TRANSFORM: eraldan dimensioonid...")
        dimensions = transform_dimensions(raw_rows)
        for dim_name, dim_rows in dimensions.items():
            print(f"      {dim_name}: {len(dim_rows)} unikaalset kirjet")

        print("\n[3/4] TRANSFORM: valmistan ette faktid...")
        facts = transform_facts(raw_rows)
        print(f"      {len(facts)} faktirida")

        # Load
        print("\n[4/4] LOAD: laadin andmebaasi...")
        rows_loaded = load(conn, dimensions, facts)
        print(f"      Laaditud {rows_loaded} faktirida")

        end_time = datetime.now()
        log_etl_run(conn, start_time, end_time, rows_loaded, "success")

        print(f"\nETL VALMIS! Kestus: {(end_time - start_time).total_seconds():.2f}s")

    except Exception as e:
        end_time = datetime.now()
        log_etl_run(conn, start_time, end_time, rows_loaded, "error", str(e))
        print(f"\nETL VIGA: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
