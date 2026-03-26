# AGENTS_EDASIJOUDNUD.md

Lisajuhised edasijõudnute (`edasijoudnud/`) praktikumimaterjalide loomiseks.
Kehtib koos failiga `AGENTS.md`.

## Sihtrühm

- Õppijal on vähemalt põhiteadmised käsureast, SQL-ist, Pythonist, Dockerist ja versioonihaldusest.
- Liigu kiiremini. Jäta ära elementaarsed vahetutvustused.
- Keskendu valikute põhjendamisele, tehnilistele kompromissidele ja töövoo tervikpildile.
- Ka edasijõudnud õppija vajab selget eesmärki, oodatavat tulemust ja põhjendust.

## Tööriista _stack_ ja keskkond

Praktikumid toimuvad Docker Compose põhises keskkonnas. Juhendis nimeta alati, millises konteineris või teenuses sammud tehakse.

Kasutatavad tööriistad:

- PostgreSQL + pg_duckdb
- dbt Core
- Apache Airflow
- Apache Superset
- OpenMetadata
- Apache Kafka
- Databricks Free Edition

Juhendi jaotises "Eeldused" kirjelda, millised teenused peavad jooksma ja kuidas neid käivitada (nt `docker compose up -d airflow`). Ära eelda, et kõik teenused on alati üleval.

## Koodi kvaliteedi ootused

Edasijõudnute materjalides ei piisa sellest, et käsk töötab. Juhend peab suunama:

- **Idempotentsus.** Operatsioon peab olema korratav ilma kõrvalmõjudeta. Too konkreetne näide (nt dbt incremental mudel, Airflow retry).
- **Logimine ja veakäsitlus.** Näita tähendusrikast logirida ja oodatavate vigade käsitlust. Mitte `try/except: pass`.
- **Saladuste haldus.** Paroolid ja API võtmed ei tohi reposse. Kasuta `.env` faile ja näita, kuidas Docker Compose neid teenustele edastab.
- **Koodi loetavus.** Selged nimed. Pikema koodinäite (10+ rida) eel lisa lühike struktuuriselgitus.

## Arhitektuurilise põhjenduse formaat

Hanke õpiväljundid nõuavad valikute põhjendamist. Kui praktikumis tehakse arhitektuuriline otsus, kasuta vormistust:

1. **Probleem.** Mida lahendame?
2. **Variandid.** Millised alternatiivid olid laual? (vähemalt kaks)
3. **Valik ja põhjendus.** Miks just see?
4. **Kompromissid.** Mida kaotame? Millal vaadata otsus üle?

Piisab 4-6 lausest. Eesmärk on harjutada oskust tehnilist otsust sõnastada ja kaitsta.

## Tõrkeotsing ja diagnostika

Ära anna alati vastust ette. Paku diagnostikasamme ja lase õppijal ise järeldus teha.

Formaat:

- **Sümptom.** Mida õppija näeb?
- **Diagnostikasammud.** Kuidas põhjust leida?
- **Lahendus.** Mida muuta?

Sageli kasutatavad diagnostikavõtted:

- `docker logs <teenus>` ja `docker ps` konteineri seisundi jaoks
- `\conninfo`, `\dt`, `SELECT version()` andmebaasi ühenduse kontrolliks
- Airflow UI task log ja DAG Run vaade töövoo veaotsinguks
- dbt `--debug` ja `target/run_results.json` vigade jaoks

## Mida vältida

- Ära muuda juhendit ainult käskude loendiks. Eesmärk ja põhjendus peavad olema.
- Ära jäta arhitektuurilisi valikuid põhjendamata.
- Ära eelda, et õppija mäletab eelmise nädala seadistust. Lisa kontrollikäsk.
- Ära jäta olulisi eeldusi nimetamata.