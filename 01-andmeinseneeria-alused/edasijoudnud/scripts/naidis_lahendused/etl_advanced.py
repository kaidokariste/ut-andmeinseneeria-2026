"""
Keerukam ETL lahendus:
- Laeb andmed kahest regioonist (Europe, Asia)
- Arvutab rahvastiku tiheduse
- Loob TOP 20 tihedaima asustusega riikide tabeli
- Logib iga ETL jooksu
"""

import json
import urllib.request
import psycopg2
import time
import os
from datetime import datetime

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}

REGIONS = ["europe", "asia"]


def extract(region):
    """Extract: loeme REST API-st andmed."""
    url = f"https://restcountries.com/v3.1/region/{region}?fields=name,capital,population,area"
    print(f"  Extracting from {url} ...")
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    print(f"    -> Saadud {len(data)} riiki")
    return data


def transform(raw_data, region):
    """Transform: puhastame, normaliseerime ja arvutame tiheduse."""
    rows = []
    for item in raw_data:
        name = item.get("name", {}).get("common", "Unknown")
        capitals = item.get("capital", [])
        capital = capitals[0] if capitals else None
        population = item.get("population", 0)
        area = int(item.get("area", 0)) if item.get("area") else 0

        # Arvuta tihedus
        density = round(population / area, 2) if area > 0 else 0

        rows.append((name, capital, population, area, region.capitalize(), density))

    rows.sort(key=lambda r: r[5], reverse=True)
    print(f"    -> Transformeeritud {len(rows)} rida ({region})")
    return rows


def load(all_rows, conn):
    """Load: kirjutame andmed PostgreSQL tabelisse."""
    cur = conn.cursor()

    # Pohi tabel
    cur.execute("""
        CREATE TABLE IF NOT EXISTS all_countries (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            capital VARCHAR(100),
            population BIGINT,
            area_km2 BIGINT,
            continent VARCHAR(50),
            density NUMERIC(10,2),
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Ranking tabel
    cur.execute("""
        CREATE TABLE IF NOT EXISTS population_density_ranking (
            rank INT,
            name VARCHAR(100) NOT NULL,
            capital VARCHAR(100),
            population BIGINT,
            area_km2 BIGINT,
            continent VARCHAR(50),
            density NUMERIC(10,2),
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Idempotentne laadimine
    cur.execute("TRUNCATE TABLE all_countries RESTART IDENTITY")
    cur.execute("TRUNCATE TABLE population_density_ranking")

    for row in all_rows:
        cur.execute(
            """INSERT INTO all_countries (name, capital, population, area_km2, continent, density)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            row,
        )

    # TOP 20 tihedaima asustusega
    top20 = sorted(all_rows, key=lambda r: r[5], reverse=True)[:20]
    for i, row in enumerate(top20, 1):
        cur.execute(
            """INSERT INTO population_density_ranking (rank, name, capital, population, area_km2, continent, density)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (i, *row),
        )

    conn.commit()
    print(f"  -> Laaditud {len(all_rows)} rida tabelisse all_countries")
    print(f"  -> Laaditud {len(top20)} rida tabelisse population_density_ranking")

    return len(all_rows)


def log_etl_run(conn, start_time, end_time, rows_loaded, status="success", error_msg=None):
    """Logi ETL jooksu info."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL,
            duration_seconds NUMERIC(10,2),
            rows_loaded INT,
            status VARCHAR(20),
            error_message TEXT
        )
    """)
    duration = (end_time - start_time).total_seconds()
    cur.execute(
        """INSERT INTO etl_log (start_time, end_time, duration_seconds, rows_loaded, status, error_message)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (start_time, end_time, duration, rows_loaded, status, error_msg),
    )
    conn.commit()
    print(f"  -> ETL jooks logitud ({duration:.1f}s, {rows_loaded} rida, staatus: {status})")


def main():
    print("=== Keerukam ETL protsess ===\n")
    start_time = datetime.now()
    conn = psycopg2.connect(**DB_CONFIG)
    rows_loaded = 0

    try:
        all_rows = []
        for region in REGIONS:
            print(f"\n--- Regioon: {region} ---")
            raw = extract(region)
            rows = transform(raw, region)
            all_rows.extend(rows)

        print(f"\nKokku {len(all_rows)} riiki\n")
        rows_loaded = load(all_rows, conn)

        end_time = datetime.now()
        log_etl_run(conn, start_time, end_time, rows_loaded)

    except Exception as e:
        end_time = datetime.now()
        log_etl_run(conn, start_time, end_time, rows_loaded, "error", str(e))
        raise

    finally:
        conn.close()

    print("\n=== ETL lõpetatud ===")


if __name__ == "__main__":
    time.sleep(3)
    main()
