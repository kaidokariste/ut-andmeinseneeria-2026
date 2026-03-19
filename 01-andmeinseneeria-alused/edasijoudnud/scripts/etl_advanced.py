"""
ETL skripti mall: loeb REST API-st andmeid ja laeb need PostgreSQL andmebaasi.

Ülesanne: täida extract(), transform() ja load() funktsioonid.
"""

import requests
import psycopg2
import os
from dotenv import load_dotenv
import time
from datetime import datetime

# See rida loeb .env faili sisu operatsioonisüsteemi muutujateks
load_dotenv()

# Andmebaasi ühenduse seaded (loetakse keskkonnamuutujatest)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", 5433)),
    "dbname": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}


def extract():
    """
    Extract: loe REST API-st riikide andmed.

    Tagasta JSON andmed listina.

    Näide kuidas API-st andmeid pärida:
        response = requests.get("https://mingi-api.com/andmed")
        data = response.json()  # tagastab Pythoni listi/dict'i
    """

    """
    Pärib andmed Aasia ja Euroopa regioonide kohta ning tagastab need ühe listina.
    """
    urls = [
        "https://restcountries.com/v3.1/region/asia?fields=name,capital,population,area,region",
        "https://restcountries.com/v3.1/region/europe?fields=name,capital,population,area,region"
    ]
    all_data = []

    for url in urls:
        response = requests.get(url)
        # Kasutame .extend(), et lisada listi elemendid, mitte listi ennast
        all_data.extend(response.json())

    return all_data


def transform(raw_data):
    """
    Transform: puhasta ja normaliseeri andmed.

    Sisend: JSON list API-st (iga element on dict)
    Väljund: list tuple'itest kujul (name, capital, population, area, continent)

    Näide kuidas JSON-ist andmeid võtta:
        item = {"name": {"common": "Estonia"}, "capital": ["Tallinn"], "population": 1331057}
        nimi = item["name"]["common"]           # -> "Estonia"
        pealinn = item["capital"][0]            # -> "Tallinn"
        rahvaarv = item["population"]           # -> 1331057

    Sorteeri tulemus rahvaarvu järgi kahanevalt:
        rows.sort(key=lambda r: r[2], reverse=True)
    """
    # TODO: käi raw_data üle, võta igast elemendist vajalikud väljad, tagasta list tuple'itest

    rows = []

    for item in raw_data:
        # Võtame andmed vastavalt sinu näitele
        nimi = item["name"]["common"]

        # Kuna capital on list, võtame esimese elemendi [0]
        # Lisame kontrolli, et skript katki ei läheks, kui list on tühi
        pealinn = item["capital"][0] if item.get("capital") else "Puudub"

        rahvaarv = item["population"]
        pindala = item["area"]
        continent = item["region"]

        # Arvutame tiheduse, vältides jagamist nulliga
        tihedus = round(rahvaarv / pindala, 2) if pindala > 0 else 0

        # Lisame andmed tuple-ina listi
        rows.append((nimi, pealinn, rahvaarv, pindala, tihedus, continent))

    # Sorteeri tulemus rahvastiku tiheduse (indeks 4) järgi kahanevalt
    rows.sort(key=lambda r: r[4], reverse=True)

    return rows

def load(rows):
    """
    Load: kirjuta andmed PostgreSQL tabelisse europe_countries.

    Tabel peab sisaldama: id, name, capital, population, area_km2, continent, loaded_at
    Laadimine peab olema idempotentne (TRUNCATE enne laadimist).

    Näide kuidas PostgreSQL-iga ühenduda ja andmeid sisestada:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, name TEXT)")
        cur.execute("INSERT INTO test (name) VALUES (%s)", ("väärtus",))
        conn.commit()
        cur.close()
        conn.close()
    """
    # TODO: loo tabel, tühjenda see (TRUNCATE), sisesta andmed, kinnita (commit)
    """
        Load: kirjuta andmed PostgreSQL tabelisse europe_countries.
        """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # 1. Loo tabel õigete väljadega
        cur.execute("""
            CREATE TABLE IF NOT EXISTS population_density_ranking (
                rank SERIAL PRIMARY KEY,
                name TEXT,
                capital TEXT,
                population BIGINT,
                area_km2 FLOAT,
                density NUMERIC,
                continent TEXT,               
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. Idempotentsus: tühjenda tabel enne uute andmete lisamist
        cur.execute("TRUNCATE TABLE population_density_ranking RESTART IDENTITY")

        # 3. Sisesta andmed (rows on list tuple'itest)
        # Kasutame %s märke, psycopg2 tegeleb andmetüüpidega ise
        # Võtame ainult esimesed 20 rida (Top 20 tiheduse järgi)
        top_rows = rows[:20]
        insert_query = """INSERT INTO population_density_ranking (name, capital, population, area_km2, density, continent) VALUES (%s, %s, %s, %s, %s, %s) """
        cur.executemany(insert_query, top_rows)

        # Võta sisestatud ridade arv kätte siit:
        rows_loaded = cur.rowcount

        # 4. Kinnita muudatused
        conn.commit()
        print(f"Laadimine edukas: {rows_loaded} rida lisatud.")

    except Exception as e:
        conn.rollback()
        print(f"Viga andmete laadimisel: {e}")
    finally:
        cur.close()
        conn.close()

    return rows_loaded

def save_etl_log(start_time, duration, rows, status):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
                    CREATE TABLE IF NOT EXISTS etl_log(
                    id SERIAL PRIMARY KEY,
                    start_time TIMESTAMP,
                    duration_seconds FLOAT,
                    rows_loaded INTEGER,
                    status TEXT
                    )
                """)

    cur.execute("""
        INSERT INTO etl_log (start_time, duration_seconds, rows_loaded, status)
        VALUES (%s, %s, %s, %s)
    """, (start_time, duration, rows, status))
    conn.commit()
    cur.close()
    conn.close()


def main():
    start_dt = datetime.now()
    start_time = time.time()
    status = "Success"
    rows_count = 0

    print("=== ETL protsess ===\n")

    try:
        # Extract
        raw = extract()
        print(f"Extracted: {len(raw)} kirjet\n")

        # Transform
        rows = transform(raw)
        print(f"Transformed: {len(rows)} rida\n")

        # Load
        rows_count = load(rows)

    except Exception as e:
        status = f"Failed: {str(e)}"
        print(f"Viga: {e}")
    finally:
        duration = round(time.time() - start_time, 2)

        # Logi salvestamine andmebaasi
        save_etl_log(start_dt, duration, rows_count, status)
        print(f"\n=== ETL lõpetatud ({status}, kestus: {duration}s) ===")

if __name__ == "__main__":
    main()
