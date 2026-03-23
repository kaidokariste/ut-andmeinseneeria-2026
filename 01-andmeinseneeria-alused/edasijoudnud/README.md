# Praktikum 1: Andmeinseneeria alused (edasijõudnud)

## Õpiväljundid

- Mõistab, mis on ETL ja andmevoog
- Suudab jooksutada Dockerit ja PostgreSQL andmebaasi
- Teeb esimese lihtsa andmete liigutamise harjutuse

## Ülevaade

| Osa | Kestus | Sisu |
|-----|--------|------|
| Demo | 30-40 min | Docker Compose, PostgreSQL, CSV laadimine SQL-iga, Python näited |
| Ülesanded | 40-50 min | Python ETL skript: API -> Transform -> PostgreSQL |

---

## Keskkond

Kasutame `pgduckdb/pgduckdb:18-v1.1.1` andmebaasi, mis toetab nii PostgreSQL kui DuckDB päringuid.

Postgres on laialt kasutatav ja üks populaarsemaid vabavaralisi andmebaasi tarkvarasid maailmas. https://www.postgresql.org/docs/

DuckDB on viimaste aastate üks populaarseimaid mälus-jooksvaid andmebaase ning andmebaasi mootoreid, mis on ehitatud just analüütiliste päringute tarbeks. https://duckdb.org/docs/stable/

pg_duckdb: https://github.com/duckdb/pg_duckdb

### Seadistamine

1. Kopeeri `.env.example` failist `.env`:

```bash
cp .env.example .env
```

2. Vajadusel muuda `.env` failis kasutajanimed ja paroolid.

3. Käivita teenused:

```bash
docker compose up -d
```

> **NB!** `.env` fail sisaldab paroole ja **ei tohi** satuda Giti repositooriumisse. Fail on lisatud `.gitignore`-sse. Repositooriumis on ainult `.env.example` näidisväärtustega.

### Teenused

| Teenus | Konteiner | Kirjeldus |
|--------|-----------|-----------|
| PostgreSQL | `praktikum-db` | Andmebaas (pgduckdb) |
| Python | `praktikum-python` | Python 3.13 koos `psycopg2` ja `requests` teekidega |
| pgAdmin | `praktikum-pgadmin` | Veebipõhine andmebaasihaldur |

### Ühendused

Vaikimisi väärtused (`.env.example` põhjal):

| Teenus | Kasutaja | Parool | Port |
|--------|----------|--------|------|
| PostgreSQL | `praktikum` | `praktikum` | 5432 |
| pgAdmin | `admin@example.com` | `admin` | 5050 |

pgAdmin: [http://localhost:5050](http://localhost:5050)

pgAdminis andmebaasiga ühenduse lisamiseks kasuta hosti `db` (Docker sisevõrk).

> Andmebaasiga ühendumiseks võib kasutada ka **DBeaver**, **DataGrip** või muud eelistatud SQL klienti. Sel juhul ühenda otse `localhost:5432` kaudu.

---

## Demo: Docker ja CSV laadimine SQL-iga

### 1. Docker Compose ülevaade

Vaata `compose.yml` faili. Selgita:
- Mis on teenus (service) ja kuidas nad omavahel suhtlevad
- Mis on volume ja miks me neid kasutame
- Mis on healthcheck ja miks see oluline on
- Kuidas `.env` fail annab väärtused `${POSTGRES_USER}` jms muutujatele

### 2. Ühenda andmebaasiga

```bash
docker exec -it praktikum-db psql -U praktikum
```

Kui muutsid kasutajat ja andmebaasi nime, siis kasuta

```bash
docker exec -it praktikum-db psql -U kasutaja -d andmebaas
```

### 3. Loo tabel ja laadi andmed

```sql
-- Tabeli loomine
CREATE TABLE IF NOT EXISTS countries (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    capital VARCHAR(100),
    population BIGINT,
    area_km2 BIGINT,
    continent VARCHAR(50)
);

-- CSV failist laadimine
\copy countries (id, name, capital, population, area_km2, continent)
FROM '/data/countries.csv'
WITH (FORMAT csv, HEADER true);
```

### 4. Kontrolli tulemust

```sql
SELECT * FROM countries ORDER BY population DESC;

SELECT continent, COUNT(*) AS riike, SUM(population) AS rahvaarv
FROM countries
GROUP BY continent;
```

Avanenud ekraanivaatest (ingl keeles _Pager_, sarnane linuxi käsuga _less_) väljumiseks vajuta `q`. 

### 5. Loo vaade (view)

```sql
CREATE VIEW large_countries AS
SELECT name, capital, population, area_km2
FROM countries
WHERE population > 5000000
ORDER BY population DESC;

SELECT * FROM large_countries;
```

### 6. Selgita ETL mõiste

- **Extract**: andmete hankimine allikast (CSV fail, API, andmebaas)
- **Transform**: andmete puhastamine, teisendamine, rikastamine
- **Load**: andmete laadimine sihtandmebaasi

Mida me just tegime oli sisuliselt EL (Extract-Load). Kus oli Transform?

### 7. Python näited samm-sammult

Ava Python konteiner:

```bash
docker exec -it praktikum-python python
```

**API päring:**

```python
import requests

response = requests.get("https://restcountries.com/v3.1/region/europe?fields=name,capital,population,area")
data = response.json()
print(f"Saadud {len(data)} riiki")

# Vaata esimest elementi
print(data[0])
```

**JSON-ist andmete võtmine:**

```python
item = data[0]
nimi = item["name"]["common"]
pealinn = item["capital"][0]
rahvaarv = item["population"]
pindala = item["area"]
print(f"{nimi}: pealinn {pealinn}, rahvaarv {rahvaarv}, pindala {pindala} km²")
```

**PostgreSQL-iga ühendamine:**

```python
import psycopg2
import os

conn = psycopg2.connect(
    host="db", 
    dbname=os.environ["POSTGRES_DB"], 
    user=os.environ["POSTGRES_USER"], 
    password=os.environ["POSTGRES_PASSWORD"])
cur = conn.cursor()

# Andmete pärimine
cur.execute("SELECT COUNT(*) FROM countries")
print(f"Tabelis countries on {cur.fetchone()[0]} rida")

# cur.close()
# conn.close()
```

**Andmete sisestamine tabelisse:**

```python
cur.execute("""
    CREATE TABLE IF NOT EXISTS test_insert (
        id SERIAL PRIMARY KEY,
        name TEXT,
        population BIGINT
    )
""")

cur.execute("INSERT INTO test_insert (name, population) VALUES (%s, %s)", ("Estonia", 1331057))
cur.execute("INSERT INTO test_insert (name, population) VALUES (%s, %s)", ("Latvia", 1842226))
conn.commit()

cur.execute("SELECT * FROM test_insert")
for row in cur.fetchall():
    print(row)

# Koristame ära
cur.execute("DROP TABLE test_insert")
conn.commit()
cur.close()
conn.close()
```

> Demo lõpp! Aeg ülesannete lahendamiseks.

---

## Ülesanne 1: Python ETL skript (lihtne)

Ava fail `scripts/etl_template.py` — seal on funktsioonide struktuur ette antud. Täida `extract()`, `transform()` ja `load()` funktsioonid.

Skript peab:

1. **Extract**: lugema REST API-st Euroopa riikide andmed (`https://restcountries.com/v3.1/region/europe`)
2. **Transform**: puhastama ja normaliseerima andmed (nimi, pealinn, rahvaarv, pindala)
3. **Load**: kirjutama tulemuse PostgreSQL tabelisse `europe_countries`

### Nõuded

- Skript peab olema **idempotentne** (korduv käivitamine ei tekita duplikaate)
- Tabel peab sisaldama `loaded_at` ajatemplit
- Andmed sorteeritakse rahvaarvu järgi
- Andmebaasi ühenduse parameetrid loetakse keskkonnamuutujatest (mitte hardcode)

### Vihjed

- `etl_template.py` failis on iga funktsiooni juures näited, kuidas vajalikke teeke kasutada
- Demo osas proovisid juba kõiki vajalikke operatsioone (API päring, JSON parsimine, SQL INSERT)
- Idempotentsuse saavutamiseks kasuta `TRUNCATE` enne uut laadimist

### Käivitamine

```bash
docker exec -it praktikum-python sh -c "python /scripts/etl_template.py"
```

### Kontroll

```sql
-- Kontrolli kas andmed laaditi
SELECT * FROM europe_countries ORDER BY population DESC LIMIT 10;

-- Kontrolli ridade arvu
SELECT COUNT(*) FROM europe_countries;

-- Kontrolli et loaded_at on täidetud
SELECT name, loaded_at FROM europe_countries LIMIT 3;
```

---

## Ülesanne 2: ETL laiendamine (keerukam)

Loo uus fail `scripts/etl_advanced.py` (või kopeeri ja laienda oma eelmist lahendust).

Laienda ETL skripti nii, et:

1. **Mitu allikat**: laadi andmed **kahest erinevast regioonist** (nt Europe ja Asia)
2. **Transformatsioon**: lisa samm, mis arvutab **rahvastiku tiheduse** (population / area_km2) ja ümardab 2 kohani
3. **Ranking-tabel**: loo eraldi tabel `population_density_ranking`, kuhu laetakse TOP 20 tihedaima asustusega riiki koos järjekorranumbriga (rank)
4. **Logimine**: lisa tabel `etl_log`, kuhu kirjutatakse iga ETL jooksu:
   - `start_time` ja `end_time`
   - `duration_seconds`
   - `rows_loaded`
   - `status` (success/error)
   - `error_message` (kui midagi läheks valesti)

### Vihjed

- Kasuta `try/except` plokki, et veaolukorrad logitaks `etl_log` tabelisse
- `population_density_ranking` tabelis peaks olema `rank` veerg (1-20)
- Mõtle, kuidas tagada, et kogu protsess on idempotentne (ka mitmele tabelile)
- Tiheduse arvutamiseks: `density = round(population / area, 2)` — ära unusta kontrollida, et `area > 0`

### Käivitamine

```bash
docker exec -it praktikum-python sh -c "python /scripts/etl_advanced.py"
```

### Kontroll

```sql
-- TOP 10 tihedaimat riiki
SELECT rank, name, continent, density
FROM population_density_ranking
ORDER BY rank
LIMIT 10;

-- ETL logid
SELECT start_time, duration_seconds, rows_loaded, status
FROM etl_log
ORDER BY start_time DESC
LIMIT 5;

-- Kontrolli et mõlemast regioonist on andmed
SELECT continent, COUNT(*) AS riike
FROM all_countries
GROUP BY continent;
```

---

## Ülesanne 3: Andmete uurimine DuckDB-ga (lisaülesanne)

Kuna kasutame `pgduckdb` andmebaasi, saame kasutada ka DuckDB funktsionaalsust otse PostgreSQL-is.

Proovi järgmist:

```sql
-- DuckDB laienduse aktiveerimine (vajadusel)
-- SET duckdb.execution = true; 

-- Loe CSV faili otse päringus (ilma tabelita)
SELECT * FROM read_csv('/data/countries.csv', header:=true);

-- Parquet failist lugemine (kui oleks parquet fail)
-- SELECT * FROM read_parquet('/data/example.parquet');
```

Arutamiseks: mis vahe on SQL `COPY` kaudu andmete laadimisel ja DuckDB `read_csv()` funktsiooniga otse päringu sees lugemisel? Millal kumba kasutad?

---

## Koristamine

```bash
docker compose down -v
```

See kustutab konteinerid ja andmemahud.
