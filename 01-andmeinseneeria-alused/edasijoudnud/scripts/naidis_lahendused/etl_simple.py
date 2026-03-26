"""
Lihtne ETL skript: loeb JSON API-st andmeid ja laeb need PostgreSQL andmebaasi.
Demonstreerib Extract-Transform-Load protsessi.
"""

import json
import urllib.request
import psycopg2
import time
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}


def extract():
    """Extract: loeme REST API-st riikide andmed."""
    url = "https://restcountries.com/v3.1/region/europe?fields=name,capital,population,area"
    print(f"Extracting data from {url} ...")
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    print(f"  -> Saadud {len(data)} riiki")
    return data


def transform(raw_data):
    """Transform: puhastame ja normaliseerime andmed."""
    rows = []
    for item in raw_data:
        name = item.get("name", {}).get("common", "Unknown")
        capitals = item.get("capital", [])
        capital = capitals[0] if capitals else None
        population = item.get("population", 0)
        area = int(item.get("area", 0))
        rows.append((name, capital, population, area, "Europe"))
    # Sorteerime rahvaarvu järgi kahanevalt
    rows.sort(key=lambda r: r[2], reverse=True)
    print(f"  -> Transformeeritud {len(rows)} rida")
    return rows


def load(rows):
    """Load: kirjutame andmed PostgreSQL tabelisse."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS europe_countries (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            capital VARCHAR(100),
            population BIGINT,
            area_km2 BIGINT,
            continent VARCHAR(50),
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Truncate enne uut laadimist (idempotentne laadimine)
    cur.execute("TRUNCATE TABLE europe_countries RESTART IDENTITY")

    for row in rows:
        cur.execute(
            """INSERT INTO europe_countries (name, capital, population, area_km2, continent)
               VALUES (%s, %s, %s, %s, %s)""",
            row,
        )

    conn.commit()
    print(f"  -> Laaditud {len(rows)} rida tabelisse europe_countries")

    # Kontrolli tulemust
    cur.execute("SELECT COUNT(*) FROM europe_countries")
    count = cur.fetchone()[0]
    print(f"  -> Tabelis kokku {count} rida")

    cur.close()
    conn.close()


def main():
    print("=== ETL protsess ===")
    print()

    # Extract
    raw = extract()
    print(f"Extracted: {len(raw)} kirjet\n")

    # Transform
    rows = transform(raw)
    print(f"Transformed: {len(rows)} rida\n")

    # Load
    load(rows)
    print()
    print("=== ETL lõpetatud ===")


if __name__ == "__main__":
    # Oota kuni andmebaas on valmis
    time.sleep(3)
    main()
