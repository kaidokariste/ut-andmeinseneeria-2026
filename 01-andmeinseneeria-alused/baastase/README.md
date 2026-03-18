# Praktikum 01: PostgreSQL-iga ühenduse loomine ja esimese CSV-faili laadimine

## Eesmärk

Selle praktikumi eesmärk on panna oma arvutis tööle PostgreSQL-andmebaas Dockeri konteineris, luua sellele ühendus ja laadida tabelisse esimene CSV-fail.

## Õpiväljundid

Praktikumi lõpuks oskab õppija:

- käivitada `docker compose` abil PostgreSQL-andmebaasi;
- luua ühenduse andmebaasiga tööriistaga `psql`;
- luua SQL-iga tabeli;
- laadida CSV-faili tabelisse käsuga `COPY`;
- kontrollida SQL-päringuga, et andmed jõudsid tabelisse.

## Hinnanguline ajakulu

Arvesta umbes 2 tunniga koos küsimuste, võimalike tõrgete lahendamise ja iseseisva harjutusega.

Praktikumi põhiosa koosneb neljast etapist:

- keskkonna käivitamine;
- ühenduse loomine;
- tabeli loomine ja CSV-faili laadimine;
- tulemuse kontrollimine.

## Enne alustamist

Vaja on:

- Docker Desktopi või muud töötavat Dockeri keskkonda
- terminali
- tekstiredaktorit või VS Code'i

Dockeri paigaldusjuhendid:

- Docker Desktop: <https://docs.docker.com/get-docker/>
- Windowsi paigaldusjuhend: <https://docs.docker.com/desktop/setup/install/windows-install/>
- macOS-i paigaldusjuhend: <https://docs.docker.com/desktop/setup/install/mac-install/>

## Praktikumi failid

- [`compose.yml`](./compose.yml) kirjeldab andmebaasi konteinerit
- [`.env.example`](./.env.example) sisaldab ühenduse näidisväärtusi
- [`data/countries.csv`](./data/countries.csv) on näidisandmestik
- [`scripts/01_create_countries_table.sql`](./scripts/01_create_countries_table.sql) loob tabeli
- [`scripts/02_load_countries.sql`](./scripts/02_load_countries.sql) laadib CSV-faili tabelisse
- [`scripts/03_check_countries.sql`](./scripts/03_check_countries.sql) kontrollib tulemust
- [`scripts/99_drop_countries.sql`](./scripts/99_drop_countries.sql) kustutab tabeli, kui soovid alustada puhtalt lehelt

## Uued mõisted

### Docker image

Valmis alus, mille põhjal konteiner käivitatakse.

### Konteiner

Töötav eraldatud keskkond. Selles praktikumis töötab PostgreSQL konteineris.

### Docker volume

Dockeri mahuühendus seob konteineri mõne hosti kausta või püsiva andmeruumiga. Selles praktikumis on meil kaks tüüpilist näidet:

- `pgdata:/var/lib/postgresql` hoiab andmebaasi andmed alles
- `./data:/data` teeb hosti `./data` kausta konteineri sees nähtavaks

### Docker network

Dockeri sisevõrk, mille kaudu konteinerid omavahel suhtlevad. Selles praktikumis ei pea me seda veel eraldi seadistama, aga `docker compose` loob selle taustal automaatselt.

### PostgreSQL

Levinud relatsiooniline andmebaas. Selles praktikumis kasutame seda SQL-päringute ja CSV-andmete laadimise jaoks.

### `psql`

PostgreSQL käsurea klient. Selle kaudu saame andmebaasiga ühenduse luua ja SQL-i käivitada.

### CSV

Lihtne tekstifail tabelandmete jaoks. Olulised omadused on päis, eraldaja ja kodeering.

### `COPY`

PostgreSQL käsk, mis loeb faili ja laadib selle sisu tabelisse.

## Tähtis vahe: host ja konteiner

Selles praktikumis on kaks konteksti.

- Host on sinu arvuti.
- Konteiner on Dockeri sees töötav keskkond.

See vahe on oluline, sest failitee *on* kummaski kontekstis erinev.

Näide:

- hostis on fail tee all `<repo juurkataloog>/01-andmeinseneeria-alused/baastase/data/countries.csv`
- andmebaasi konteineri sees on sama fail tee all `/data/countries.csv`

Kirje `./data:/data` tähendab siin väga konkreetselt järgmist:

- vasak pool `./data` viitab sinu arvutis oleva praktikumi kausta `data` alamkaustale
- sinna saad failibrauseri, VS Code'i või muu redaktori kaudu faile lisada, kustutada ja muuta
- parem pool `/data` on konteineri sees olev kaust
- konteiner näeb selles kaustas sama sisu, mis on hosti `./data` kaustas

Seega:

- kui lisad hostis faili `data/uus_fail.csv`, siis konteineri sees on sama fail olemas teel `/data/uus_fail.csv`
- kui SQL-is kasutad käsku `COPY FROM '/data/countries.csv'`, siis loeb PostgreSQL seda faili konteineri vaatest
- sama fail on siiski pärit sinu arvuti `data` kaustast, mitte "kuskilt Dockeri seest"

## 1. Ava praktikumi kaust

Liigu terminalis kausta `01-andmeinseneeria-alused/baastase`.

Kui kasutad VS Code'i, siis lihtsaim tee on avada see kaust ja käivitada terminal otse sealt.

Kui alustad repo juurkaustast, siis kasuta käsku:

```bash
cd 01-andmeinseneeria-alused/baastase
```

## 2. Loo `.env` fail

`docker compose` loeb ühenduse väärtused failist `.env`. Repositooriumis on ainult näidisfail `.env.example`.

macOS-is või Linuxis:

```bash
cp .env.example .env
```

Windows PowerShellis:

```powershell
Copy-Item .env.example .env
```

Vaikimisi väärtused on:

- andmebaas: `praktikum`
- kasutaja: `praktikum`
- parool: `praktikum`
- port: `5432`

Praegu ei ole vaja neid muuta.

## 3. Vaata korraks üle `compose.yml`

Enne käivitamist tasub aru saada, mida see fail teeb.

- `env_file` ütleb, et `docker compose` loeb ühenduse väärtused failist `.env`
- `image` määrab, milline andmebaasi pilt käivitatakse
- `ports` seob konteineri pordi `5432` sinu arvuti pordiga `5432`
- `volumes` teeb kaustad `data` ja `scripts` konteineris nähtavaks ning hoiab andmebaasi andmed alles
- eraldi `networks` plokki siin ei ole, sest `docker compose` loob vaikimisi sisevõrgu automaatselt
- `healthcheck` kontrollib, kas andmebaas on valmis ühendusi vastu võtma

Me kasutame siin `docker compose`-it, kuigi teenuseid on ainult üks. Põhjus on lihtne: nii on keskkond kirjas failis, mitte pika käsu sees.

Märkus:

- pildi nimi on `pgduckdb/pgduckdb`, kuid selles praktikumis kasutame seda nagu PostgreSQL-andmebaasi
- eraldi `./data:/data` köide on siin praktikumi CSV-failide jaoks

## 4. Käivita andmebaas

```bash
docker compose up -d
```

Mida see käsk teeb:

- loeb `compose.yml` faili
- loob vajadusel andmemahu
- käivitab taustal PostgreSQL konteineri

Kontrolli, kas konteiner töötab:

```bash
docker compose ps
```

Kui tahad näha viimaseid logisid:

```bash
docker compose logs db --tail=20
```

Oodatav tulemus:

- teenuse `db` olek on `running` või `healthy`

## 5. Loo ühendus andmebaasiga

Kasutame selle praktikumi põhiteena `psql`-i otse konteineri sees. Nii väldime seda, et käsurea klient peaks olema õppija arvutisse eraldi paigaldatud.

```bash
docker compose exec db psql -U praktikum -d praktikum
```

Kui muutsid `.env` failis kasutajanime või andmebaasi nime, siis asenda need käsus enda väärtustega.

Kui ühendus õnnestub, näed `psql`-i prompti:

```text
praktikum=#
```

Kasulikud esimesed käsud `psql`-is:

```sql
\l
\dt
\q
```

Selgitus:

- `\l` näitab andmebaase
- `\dt` näitab tabeleid
- `\q` väljub `psql`-ist

Kui väljusid, loo ühendus uuesti.

## 6. Baas-SQL lühikordus

Selles praktikumis kasutame peamiselt kolme tüüpi SQL-käske.

### Tabeli loomine

```sql
CREATE TABLE countries (...);
```

### Andmete laadimine

```sql
COPY countries FROM '...';
```

### Andmete kontrollimine

```sql
SELECT * FROM countries;
```

Praegu piisab sellest, kui saad aru, et:

- `CREATE TABLE` loob tühja tabeli
- `COPY` loeb failist andmed sisse
- `SELECT` kuvab andmeid

## 7. Vaata CSV-fail üle

Ava fail [`data/countries.csv`](./data/countries.csv) tekstiredaktoris. Esimesed read on sellised:

```csv
id,name,capital,population,area_km2,continent
1,Eesti,Tallinn,1331000,45339,Europe
2,Läti,Riia,1850000,64559,Europe
3,Leedu,Vilnius,2800000,65300,Europe
```

Mida tähele panna:

- failil on päis
- eraldaja on koma
- failis on täpitähed, seega faili kodeering peab olema teada ja importimisel õigesti määratud
- selles praktikumis eeldame, et fail on UTF-8 kodeeringus
- iga veerg peab sobima loodava tabeli veerutüübiga

## 8. Loo tabel

Käivita järgmine SQL:

```sql
CREATE TABLE IF NOT EXISTS countries (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    capital TEXT NOT NULL,
    population BIGINT,
    area_km2 BIGINT,
    continent TEXT NOT NULL
);
```

Soovi korral võid sama sammu teha ka valmis failist:

```sql
\i /scripts/01_create_countries_table.sql
```

Kontrolli, et tabel loodi:

```sql
\dt
```

Oodatav tulemus:

- tabelite loetelus on `countries`

## 9. Laadi CSV-fail tabelisse

Käivita järgmine SQL:

```sql
TRUNCATE TABLE countries;

COPY countries (id, name, capital, population, area_km2, continent)
FROM '/data/countries.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    ENCODING 'UTF8'
);
```

Soovi korral võid sama sammu teha ka valmis failist:

```sql
\i /scripts/02_load_countries.sql
```

Miks siin on `TRUNCATE TABLE countries;`?

- kui käivitad sama impordi uuesti, ei teki dubleerivaid ridu

Miks failitee on `/data/countries.csv`, mitte midagi sinu arvutist?

Selles praktikumis töötab nii andmebaas kui ka `psql` samas Dockeri keskkonnas. Seepärast loeb SQL käsk `COPY` faili teelt `/data/countries.csv`, mis on konteineri sees olemas.

Meie `compose.yml` seob hosti kausta `data` konteineri kaustaga `/data`, nii et PostgreSQL näeb seda faili konteineri vaates.

> **NB!**
> Kui `psql` ja andmebaas ei tööta samas masinas, siis tavaline SQL käsk `COPY` loeb faili ikka andmebaasi serveri vaatest.
> Sellises olukorras kasutatakse sageli `psql` metakäsku `\copy`, mis võimaldab lugeda faili kliendi masinast ja saata andmed andmebaasi.
> Selles praktikumis me `\copy` käsku ei kasuta, aga seda tasub edaspidiseks teada.

Oodatav tulemus:

- `COPY 10`

See tähendab, et tabelisse laaditi 10 rida.

## 10. Kontrolli tulemust

Käivita kontrollpäringud:

```sql
SELECT COUNT(*) AS riikide_arv
FROM countries;

SELECT name, capital, population
FROM countries
ORDER BY population DESC
LIMIT 5;
```

Soovi korral võid sama sammu teha ka valmis failist:

```sql
\i /scripts/03_check_countries.sql
```

Oodatav tulemus:

- ridu on `10`
- suurima rahvaarvuga riigid on tabelis nähtavad

## Märkus töövõtte kohta

Õppimise mõttes on kasulik esimesel korral SQL käsitsi läbi teha. Nii näed paremini, milline käsk mida teeb.

Kui sama tegevust on vaja hiljem korrata, siis on tavaliselt parem hoida SQL eraldi `.sql` failis ja käivitada see failist. See teeb töövoo:

- korratavaks;
- vähem veatundlikuks;
- lihtsamini jagatavaks;
- mugavamaks parandada ja uuesti käivitada.

Selles praktikumis kasutamegi mõlemat lähenemist:

- juhendis näed SQL-i otse;
- kaustas `scripts/` on sama loogika eraldi failidena olemas.

Kui töötad `psql` sees, saad faili käivitada käsuga `\i`. Kui käivitad skripti otse käsurealt, on tavapärane kasutada `psql -f`.

`psql -f` on eriti mugav siis, kui soovid skripti korduvalt käivitada või vea korral täpsemalt aru saada, millises failirea juures probleem tekkis.

## 11. Välju ja peata teenus

Välju `psql`-ist:

```sql
\q
```

Kui soovid praktikumis tööle joone alla tõmmata, peata konteiner:

```bash
docker compose down
```

Kui soovid kustutada ka andmemahu:

```bash
docker compose down -v
```

## Levinud vead ja lahendused

### Port 5432 on juba kasutusel

Sümptom:

- `docker compose up -d` annab veateate, et porti ei saa siduda

Lahendus:

- muuda failis `compose.yml` rida `"5432:5432"` näiteks kujule `"55432:5432"`
- kui ühendud GUI-kliendiga, kasuta siis porti `55432`

### Konteiner ei lähe käima

Kontrolli:

```bash
docker compose logs db --tail=50
```

Vaata, kas probleem on pildis, pordis või `.env` faili väärtustes.

### `psql` ei ühendu

Kontrolli:

- kas konteiner on olekus `healthy`
- kas kasutajanimi ja andmebaasi nimi vastavad `.env` failile

### `COPY` ei leia faili

Peamine põhjus:

- failitee on vale või faili ei näe konteiner

Kontrolli:

- kas `data/countries.csv` on olemas
- kas `compose.yml` sisaldab köidet `./data:/data`
- kas SQL-is on failitee `/data/countries.csv`

### Täpitähed on katki või `COPY` annab kodeeringuvea

Võimalikud sümptomid:

- täpitähtede asemel näed valesid märke
- `COPY` annab vea, näiteks `invalid byte sequence for encoding "UTF8"`

Peamine põhjus:

- faili tegelik kodeering ei klapi sellega, mida PostgreSQL importimisel eeldab
- Windowsis ei maksa eeldada, et iga CSV-fail on automaatselt UTF-8

Lahendus:

- kui võimalik, salvesta fail uuesti UTF-8 kodeeringus
- kui fail tuleb Excelist, eelista salvestamisel vormingut `CSV UTF-8`
- kui tead faili tegelikku kodeeringut, muuda `COPY` käsus `ENCODING` väärtust

Näiteks kui fail on Windowsi baltikeelses kodeeringus, siis võib sobida:

```sql
COPY countries (id, name, capital, population, area_km2, continent)
FROM '/data/countries.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    ENCODING 'WIN1257'
);
```

Selles praktikumis olev näidisfail on UTF-8. Kui kasutad oma faili, kontrolli kodeering üle enne importi.

## Kui tahad kasutada DBeaverit või pgAdmini

Põhitee selles praktikumis on `psql`, aga soovi korral saad kasutada ka GUI-klienti.

Ühenduse andmed:

- host: `localhost`
- port: `5432`
- andmebaas: `praktikum`
- kasutaja: `praktikum`
- parool: `praktikum`

Oluline märkus:

- kui käivitad SQL-i GUI-kliendist, siis `COPY FROM '/data/countries.csv'` loeb faili endiselt andmebaasi konteineri seest
- see ei loe faili sinu arvuti `Downloads` kaustast

## Lisamärkus: kuidas sama asja teha `docker run` käsuga

`docker compose` ei ole ainus tee. Sama andmebaasi saab käivitada ka ühe pika käsuga:

```bash
docker run --name praktikum-db \
  -e POSTGRES_USER=praktikum \
  -e POSTGRES_PASSWORD=praktikum \
  -e POSTGRES_DB=praktikum \
  -p 5432:5432 \
  -v pgdata:/var/lib/postgresql \
  -v "$(pwd)/data:/data" \
  -v "$(pwd)/scripts:/scripts" \
  -d pgduckdb/pgduckdb:18-v1.1.1
```

Miks me siiski eelistame `docker compose`-it:

- seadistus on failis ja lihtsamini loetav
- sama keskkonda on lihtne uuesti käivitada
- pikkade parameetrite meeldejätmine ei ole vajalik

Märkus:

- see näide on kõige loomulikum macOS-is ja Linuxis
- Windowsis tasub selles praktikumis jääda `docker compose` tee juurde

## Iseseisev harjutus

Vali üks järgmistest variantidest.

### Variant A: puhas avalik andmestik

Kasuta Shopify ametlikku tootenäidise CSV-d:

- <https://help.shopify.com/en/manual/products/import-export/using-csv>

Selle variandi mõte on töötada failiga, mis on hästi vormistatud ja pärineb reaalsest e-poe töövoost.

Soovitus:

- ära püüa esimesel korral importida kõiki veerge
- vali 5-8 sulle arusaadavat veergu ja loo nende põhjal üks tabel

### Variant B: kontrollitud väljakutse

Kasuta faili `data/orders_messy_semicolon.csv`.

Selles failis on üks oluline detail teistsugune kui põhiharjutuses. Selle variandi mõte on harjutada vea leidmist ja parandamist veateate, päise ja faili sisu põhjal ilma, et fail oleks päriselt katki.

Soovituslik tööjärjekord:

1. laadi fail alla kausta `data/`
2. ava fail ja kontrolli päist, eraldajat ning kodeeringut
3. otsusta, millised veerud tabelisse lähevad
4. loo uus tabel
5. kohanda `COPY` käsku vastavalt failile
6. kontrolli `SELECT COUNT(*)` päringuga, mitu rida laaditi

Kui valid variandi B, siis proovi esialgu importi sama loogikaga nagu põhiharjutuses ja vaata, mis läheb valesti. Seejärel loe veateadet rahulikult, ava fail tekstiredaktoris ja kontrolli vähemalt kolme asja:

1. kas päis on olemas;
2. milline on eraldaja;
3. kas veergude järjekord ja tüübid klapivad sinu tabeliga.

## Kokkuvõte

Selles praktikumis tegid läbi kogu esimese andmete laadimise ahela:

- käivitasid andmebaasi konteineris
- lõid ühenduse PostgreSQL-iga
- lõid tabeli
- laadisid CSV-faili andmebaasi
- kontrollisid, et andmed jõudsid kohale

See on väike, aga oluline baas järgmise praktikumi jaoks. Edasi liigume lihtsa skeemi loomise, ER-diagrammi, põhiliste `SELECT`-, `JOIN`- ja `GROUP BY` päringute ning fakti- ja dimensioonitabelite põhiidee juurde.
