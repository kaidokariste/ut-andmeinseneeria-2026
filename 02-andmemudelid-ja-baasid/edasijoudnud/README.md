# Praktikum 2: Andmemudelid - Star Schema (Edasijõudnud)

## Eesmärk

Kavandada ja ehitada star schema normaliseeritud OLTP allikandmetest, järgides Kimballi dimensionaalse modelleerimise metoodikat.

## Õpiväljundid

Praktikumi lõpuks osaleja:

- Tunneb Kimballi metoodikat ja oskab seda rakendada (äriprotsess → granulaarsus → dimensioonid → faktid)
- Oskab eristada OLTP ja OLAP andmemudeleid ning põhjendada denormaliseerimist
- Suudab kavandada star schema, määrata granulaarsust ja valida mõõdiku tüüpe
- Teab erinevust surrogate key ja natural key vahel ning põhjendab valikut
- Mõistab SCD Type 2 kontseptsiooni ja oskab seda implementeerida

## Ülevaade

| Osa | Kestus | Sisu |
|-----|--------|------|
| Demo | 40-50 min | Kimballi metoodika, OLTP→OLAP teisendus, star schema kavandamine ja ehitamine, andmete laadimine (CSV + OLTP), analüütilised päringud |
| Ülesanded | 40-50 min | SCD Type 2, snowflake schema + EXPLAIN ANALYZE, Python ETL, DimTime (lisaülesanne) |

---

## Keskkond

Kasutame `pgduckdb/pgduckdb:18-v1.1.1` andmebaasi, mis toetab nii PostgreSQL kui DuckDB päringuid.

- **PostgreSQL** — laialt kasutatav vabavaraline relatsiooniline andmebaas: https://www.postgresql.org/docs/
- **DuckDB** — analüütiliste päringute mootor, mis suudab lugeda CSV/Parquet faile otse: https://duckdb.org/docs/stable/
- **pg_duckdb** — PostgreSQL laiendus, mis integreerib DuckDB võimekuse: https://github.com/duckdb/pg_duckdb

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

> **NB!** `.env` fail sisaldab paroole ja **ei tohi** satuda Giti repositooriumisse. Fail on lisatud `.gitignore`-sse.

Oota kuni teenused on valmis:

```bash
docker compose ps
```

### Teenused

| Teenus | Konteiner | Kirjeldus |
|--------|-----------|-----------|
| PostgreSQL | `praktikum-db-02` | Andmebaas (pgduckdb) |
| Python | `praktikum-python-02` | Python 3.13 koos `psycopg2` ja `requests` teekidega |
| pgAdmin | `praktikum-pgadmin-02` | Veebipõhine andmebaasihaldur |

### Ühendused

Vaikimisi väärtused (`.env.example` põhjal):

| Teenus | Kasutaja | Parool | Port |
|--------|----------|--------|------|
| PostgreSQL | `praktikum` | `praktikum` | 5432 |
| pgAdmin | `admin@example.com` | `admin` | 5050 |

pgAdmin: [http://localhost:5050](http://localhost:5050)

pgAdminis andmebaasiga ühenduse lisamiseks kasuta hosti `db` (Docker sisevõrk).

> Andmebaasiga ühendumiseks võib kasutada ka **DBeaver**, **DataGrip** või muud SQL klienti. Sel juhul ühenda otse `localhost:5432` kaudu.

---

## Uued mõisted

### Dimensionaalne modelleerimine (Dimensional Modeling)

Operatsioonilised andmebaasid (OLTP) on optimeeritud kirjutamiseks — paljude tabelitega normaliseeritud struktuur, kus andmete kordumist välditakse. Analüütilised andmebaasid (OLAP) on optimeeritud lugemiseks — vähem tabeleid, vähem JOIN-e, kiiremad agregatsioonid.

Dimensionaalne modelleerimine on lähenemine, mis struktureerib andmed OLAP jaoks. Levinuim metoodika on **Ralph Kimballi** neljasammuline protsess.

### Tähtskeem (Star schema)

Tähtskeem koosneb kesksest **faktitabelist** (mõõdetavad sündmused) ja seda ümbritsevatest **dimensioonitabelitest** (kirjeldavad atribuudid). Graafiliselt meenutab see tähte — fakt keskel, dimensioonid ümber.

### Faktitabel (Fact Table)

Faktitabel sisaldab **mõõdetavaid sündmusi** (tehingud, mõõtmised). Iga rida esindab ühte sündmust kindlal granulaarsustasemel.

Mõõdikute tüübid:
- **Liidetav** (additive) — saab summeerida kõigi dimensioonide lõikes. Näide: müügisumma, kogus.
- **Pool-liidetav** (semi-additive) — saab summeerida osade dimensioonide lõikes. Näide: kontojääk (summeerimine aja lõikes ei anna mõistlikku tulemust).
- **Mitte-liidetav** (non-additive) — summeerimine ei anna mõistlikku tulemust ühegi dimensiooni lõikes. Näide: ühikuhind, temperatuur.

### Dimensioonitabel (Dimension Table)

Dimensioonitabel sisaldab **kirjeldavaid atribuute**, mille järgi saab andmeid filtreerida ja grupeerida. Näide: kuupäev (aasta, kuu, nädalapäev), pood (nimi, linn, regioon), toode (nimi, kategooria, bränd).

### Surrogate key vs natural/business key

- **Loomulik võti** (Natural key, business key) — alliksüsteemist pärinev identifikaator (nt kliendi ID, toote kood). Sõltub alliksüsteemist ja võib muutuda.
- **Surrogaat võti** (Surrogate key) — andmelao sisemine automaatgenereeritud võti. Ei sõltu alliksüsteemist. Võimaldab SCD Type 2 ajaloo säilitamist (sama business key, erinevad surrogate key'd eri versioonidele).

### Granulaarsus (Grain)

Granulaarsus määrab, mida iga rida faktitabelis esindab. See on star schema kavandamise kõige olulisem otsus. Näide: "üks rida = üks toode ühes müügitehingus" vs "üks rida = ühe päeva koondmüük ühe toote kohta".

### Aeglaselt muutuvad dimensioonid (SCD — Slowly Changing Dimension)

Dimensioonide väärtused muutuvad ajas (klient kolib, toote hind muutub). SCD tüübid määravad, kuidas neid muutusi käsitleda:

- **Type 0**: väärtusi ei uuendata. Algne väärtus jääb.
- **Type 1**: väärtus kirjutatakse üle. Ajalugu kaob.
- **Type 2**: lisatakse uus rida. Ajalugu säilitatakse (nt `ValidFrom`/`ValidTo` väljadega).
- **Type 3**: lisatakse uus veerg (nt `OldCity`, `NewCity`). Piiratud ajalugu.

Üpris hea ülevaade on Wikipedias: https://en.wikipedia.org/wiki/Slowly_changing_dimension

Praktikas kasutatakse üldjuhul vaid SCD Type 1 ja 2. Oluline on mõista hästi kuidas toimib SCD Type 2. 

---

## Demo: Star schema kavandamine Kimballi metoodikaga

### 1. Allikandmed — kust tulevad andmed?

Andmelattu jõuavad andmed erinevatest allikatest. Selles demos vaatame kahte levinumat varianti:

1. **Denormaliseeritud eksport** — üks suur lai tabel (CSV fail), kus kogu tehingu info on ühes reas. Tüüpiline, kui OLTP süsteem pakub eksporti või API-t ilma detailse ligipääsuta.
2. **Normaliseeritud OLTP andmebaas** — päris operatsiooniline süsteem, kus andmed on jaotatud paljudesse seotud tabelitesse (kliendid, tellimused, tooted, maksed jne).

Mõlemast ehitame **sama star schema**, erinevus vaid ETL keerukuses.

Ühenda andmebaasiga:

```bash
docker exec -it praktikum-db-02 psql -U praktikum
```

#### Näide 1: Denormaliseeritud eksport (CSV)

Vaatame esmalt lihtsamat varianti — üks CSV fail, kus iga rida sisaldab kogu tehingu infot:

```sql
-- Vaata esimest 5 rida
SELECT * FROM read_csv('/data/source_sales.csv', header := true) LIMIT 5;
```

```sql
-- Mitu rida kokku?
SELECT COUNT(*) FROM read_csv('/data/source_sales.csv', header := true);
```

```sql 
-- Vaata konkreetseid tulpasid 
SELECT r['order_id']::INT, r['store_city']::TEXT, r['customer_name']::TEXT FROM read_csv('/data/source_sales.csv', header := true) AS r LIMIT 20;
``` 

```sql 
-- Vaata konkreetse inimese oste
SELECT * FROM read_csv('/data/source_sales.csv', header := true) AS r WHERE r['customer_id']::INT = 1 LIMIT 20;
``` 

Pane tähele: andmed on **denormaliseeritud**. Kliendi nimi, poe nimi, kategooria, jne kõik korduvad igas reas. See on tüüpiline OLTP ekspordi formaat.

#### Näide 2: Normaliseeritud OLTP andmebaas

Pärismaailmas elavad andmed OLTP süsteemis normaliseeritud kujul, paljudes omavahel seotud tabelites. Loome sellise süsteemi:

```bash
docker exec -it praktikum-db-02 bash -c "psql -U praktikum -f /sql/05_oltp_source.sql"
```

Vaata loodud tabeleid:

```sql
\dt oltp.*
```

OLTP süsteemis on samad äriandmed jaotatud **11 tabelisse**:

| Valdkond | Tabelid | Selgitus |
|----------|---------|----------|
| **Kliendihaldus** | `customer`, `customer_address`, `contact_info` | Üks klient → mitu aadressi, mitu kontakti |
| **Tootekataloog** | `product`, `product_category` | Toode → kategooria (normaliseeritud) |
| **Poed** | `store`, `employee` | Pood → töötajad |
| **Tellimused** | `sales_order_header`, `sales_order_detail` | Päis + read (SAP-is: VBAK + VBAP) |
| **Maksed** | `payment_method`, `payment_transaction` | Makseviis + maksetehingud |

Näiteks: et näha, **mida** klient **ostis**, **millises** poes ja **kuidas** maksis, peame ühendama 6+ tabelit:

```sql
SELECT
    c.first_name || ' ' || c.last_name AS klient,
    p.product_name,
    pc.category_name,
    s.store_name,
    pm.method_name AS makseviis,
    od.quantity,
    od.line_total
FROM oltp.sales_order_detail od
JOIN oltp.sales_order_header oh ON od.order_id = oh.order_id
JOIN oltp.customer c ON oh.customer_id = c.customer_id
JOIN oltp.product p ON od.product_id = p.product_id
JOIN oltp.product_category pc ON p.category_id = pc.category_id
JOIN oltp.store s ON oh.store_id = s.store_id
JOIN oltp.payment_transaction pt ON oh.order_id = pt.order_id
JOIN oltp.payment_method pm ON pt.payment_method_id = pm.payment_method_id
LIMIT 5;
```

**Mõttekoht:** Miks ei ole hea teha analüütilisi päringuid otse operatsioonilises (OLTP) baasis? 
* <details><summary>Keerukus</summary>

  Analüütilised päringud OLTP skeemal on üldjuhul keerulisemad, nõudes päringuid üle rohkemate tabelite.
  </details>
* <details><summary>Kiirus</summary>

  OLTP andmebaasid on mõeldud üksikute ridade kiireteks CRUD operatsioonideks. Analüütilised päringud on pigem lugemis-operatsioonid (_read_) üle väga paljude ridade. 
  </details>
* <details><summary>Ajalugu</summary>

  OLTP andmebaasid on mõeldud hoidma praeguse hetke seisu. Analüütikas on sageli vaja näha varasemat seisu, näha trende, jne.
  </details>
* <details><summary>Inimloetavus</summary>

  OLTP andmebaasid on optimeeritud masinloetavuseks (back-end süsteemidele). _Tavaline_ inimene ei mõtle normaliseeritud kujul. Inimloetavuse jaoks on parem disainida andmemudel denormaliseeritud kujul.
  </details>


### 2. Kimballi samm 1 — Vali äriprotsess

> **Probleem.** Meil on toorandmed. Millist äriprotsessi modelleerime?
>
> **Variandid.** (1) Müügiprotsess — iga tehing ühe tootega. (2) Laohaldus — toodete liikumine lattu ja välja. (3) Kliendisuhe — kliendiregistreerimised ja -lahkumised.
>
> **Valik ja põhjendus.** Müügiprotsess, sest allikandmed sisaldavad müügitehinguid koos koguste, hindade, kuupäevade, poodide ja klientidega.
>
> **Kompromissid.** Laohaldus ja kliendisuhe jäävad katteta. Neid saab hiljem modelleerida eraldi täht-skeemidena.

### 3. Kimballi samm 2 — Määra granulaarsus

Granulaarsus on täht-skeemi **kõige olulisem otsus**. See määrab, mida iga rida faktitabelis esindab.

Uurime allikandmeid:

```sql
-- Kas üks tellimus = üks rida?
SELECT r['order_id']::INT, COUNT(*) AS ridu
FROM read_csv('/data/source_sales.csv', header := true) AS r
GROUP BY r['order_id']::INT
HAVING COUNT(*) > 1
LIMIT 5;
```

Vastus: ei — ühes tellimuses võib olla mitu toodet. Iga rida esindab **ühte toodet ühes tellimuses**.

> **Probleem.** Milline peaks olema faktitabeli granulaarsus?
>
> **Variandid.** (1) Üks rida = üks toode ühes tehingus (tellimuse rida). (2) Üks rida = üks tellimus (koondatud). (3) Üks rida = ühe päeva koondmüük poe ja toote kohta.
>
> **Valik ja põhjendus.** Variant 1 — tellimuse rea tasand. See säilitab kõige suurema detailsuse. Alati saab hiljem andmeid agregeerida (`GROUP BY`), aga detailsust tagasi tuua ei saa.
>
> **Kompromissid.** Rohkem ridu faktitabelis (suurem maht). Kui analüüs on alati päevatasandil, oleks variant 3 kompaktsem.

**Granulaarsus:** üks rida = üks toode ühes müügitehingus.

Parim praktika: granulaarsus võiks olla alati nii detailne kui võimalik. 

### 4. Kimballi samm 3 — Vali dimensioonid

Otsime allikandmetest **kirjeldavaid atribuute**, mille järgi tahame andmeid filtreerida ja grupeerida.

```sql
-- Kuupäevad
SELECT DISTINCT r['order_date']::DATE
FROM read_csv('/data/source_sales.csv', header := true) AS r
ORDER BY r['order_date']::DATE
LIMIT 10;
```

```sql
-- Poed
SELECT DISTINCT r['store_name']::TEXT, r['store_city']::TEXT, r['store_region']::TEXT
FROM read_csv('/data/source_sales.csv', header := true) AS r
ORDER BY r['store_name']::TEXT LIMIT 10;
```

```sql
-- Tooted
SELECT DISTINCT r['product_name']::TEXT, r['product_category']::TEXT, r['product_brand']::TEXT
FROM read_csv('/data/source_sales.csv', header := true) AS r
ORDER BY r['product_category']::TEXT, r['product_name']::TEXT LIMIT 10;
```

```sql
-- Kliendid
SELECT DISTINCT r['customer_id']::INT, r['customer_name']::TEXT, r['customer_city']::TEXT, r['customer_segment']::TEXT
FROM read_csv('/data/source_sales.csv', header := true) AS r
ORDER BY r['customer_id']::INT LIMIT 10;
```

```sql
-- Makseviisid
SELECT DISTINCT r['payment_type']::TEXT
FROM read_csv('/data/source_sales.csv', header := true) AS r
ORDER BY r['payment_type']::TEXT;
```

Saame **viis dimensiooni**:

| Dimensioon | Atribuudid | Märkused |
|------------|-----------|----------|
| **DimDate** | FullDate, Year, Quarter, Month, Day, DayOfWeek, DayOfYear, WeekOfYear, MonthName, IsWeekend | NB! Genereeritakse kuupäevadest (mitte otse allikast) |
| **DimStore** | StoreName, City, Region | Kus müük toimus |
| **DimProduct** | ProductName, Category, Brand | Mida müüdi |
| **DimCustomer** | CustomerID, FirstName, LastName, Segment, City | Kes ostis |
| **DimPayment** | PaymentType | Kuidas maksti |

Iga dimensioon saab **surrogaat võtme** (surrogate key, `INTEGER GENERATED ALWAYS AS IDENTITY`) — andmelao sisemine automaatgenereeritud võti, mis ei sõltu alliksüsteemist. See on vajalik nt:
* Ajaloo jälgimiseks (SCD Type 2)
* Alliksüsteemi muutuste haldamiseks
* Mitme alliksüsteemi puhul (nt customer_id tuleb mitmest süsteemist)
* Puuduvate andmete haldamine (nt `-1 = UNKNOWN`)

### 5. Kimballi samm 4 — Vali faktid (mõõdikud)

Uurime, millised **numbrilised väärtused** on allikandmetes:

```sql
SELECT r['quantity']::INT, r['unit_price']::NUMERIC(10,2), r['total_amount']::NUMERIC(10,2)
FROM read_csv('/data/source_sales.csv', header := true) AS r
LIMIT 10;
```

| Mõõdik | Tüüp | Selgitus |
|--------|------|----------|
| **Quantity** | Liidetav | Ostetud kogus. Saab summeerida kõigi dimensioonide lõikes. |
| **UnitPrice** | Mitte-liidetav | Ühiku hind. Summeerimine ei anna mõistlikku tulemust (ainult AVG). |
| **TotalAmount** | Liidetav | Rea kogusumma (Quantity × UnitPrice). Saab summeerida. |

### 6. Loo tähtskeemi tabelid

Nüüd loome tabelid vastavalt kavandile. Käivita tabelite loomise skript:

```bash
docker exec -it praktikum-db-02 bash -c "psql -U praktikum -f /sql/01_create_tables.sql"
```

Vaata loodud tabeleid:

```sql
\dt
```

Star schema struktuur:

```
                             DimDate
                                |
               DimStore --- FactSales --- DimProduct
                            /       \
                   DimCustomer    DimPayment
```

### 7. Näide 1: Laadimine CSV failist (lame allikas)

Laadime andmed denormaliseeritud CSV failist star schemasse. Laadimise käigus eraldatakse unikaalsed dimensiooniväärtused ja luuakse viited surrogate key'de kaudu.

```bash
docker exec -it praktikum-db-02 bash -c "psql -U praktikum -f /sql/02_load_data.sql"
```

Kontrolli ridade arvu:

```sql
SELECT 'DimDate' AS tabel, COUNT(*) AS ridu FROM DimDate
UNION ALL SELECT 'DimStore', COUNT(*) FROM DimStore
UNION ALL SELECT 'DimProduct', COUNT(*) FROM DimProduct
UNION ALL SELECT 'DimCustomer', COUNT(*) FROM DimCustomer
UNION ALL SELECT 'DimPayment', COUNT(*) FROM DimPayment
UNION ALL SELECT 'FactSales', COUNT(*) FROM FactSales
ORDER BY tabel;
```

Oodatav tulemus: 730 kuupäeva, 5 poodi, 20 toodet, 20 klienti, 3 makseviisi, 1039 faktirida.

CSV-põhise laadimise eelised:
- Allikas on üks lai fail → lihtsad päringud
- `02_load_data.sql` kasutab ainult **5 JOIN-i** (CSV → dimensioonid)

### 8. Näide 2: Laadimine OLTP andmebaasist (normaliseeritud allikas)

Nüüd näitame alternatiivi: laadime **sama tähtskeemi** normaliseeritud OLTP tabelitest. Esmalt loome tühjad tähtskeemi tabelid uuesti:

```bash
docker exec -it praktikum-db-02 bash -c "psql -U praktikum -f /sql/01_create_tables.sql"
```

Seejärel laadime OLTP allikast:

```bash
docker exec -it praktikum-db-02 bash -c "psql -U praktikum -f /sql/06_load_from_oltp.sql"
```

Kontrolli, et ridade arvud klapivad CSV variandiga:

```sql
SELECT 'DimDate' AS tabel, COUNT(*) AS ridu FROM DimDate
UNION ALL SELECT 'DimStore', COUNT(*) FROM DimStore
UNION ALL SELECT 'DimProduct', COUNT(*) FROM DimProduct
UNION ALL SELECT 'DimCustomer', COUNT(*) FROM DimCustomer
UNION ALL SELECT 'DimPayment', COUNT(*) FROM DimPayment
UNION ALL SELECT 'FactSales', COUNT(*) FROM FactSales
ORDER BY tabel;
```

OLTP-põhise laadimise erinevused:
- Allikas on **11 normaliseeritud tabelit** → keerulisemad päringud
- `06_load_from_oltp.sql` kasutab **~14 JOIN-i** (OLTP tabelid + dimensioonide mapping)
- DimProduct nõuab JOIN-i `oltp.product` + `oltp.product_category` vahel (denormaliseerime kategooria otse tootedimensiooni)
- DimCustomer nõuab JOIN-i `oltp.customer` + `oltp.customer_address` vahel (valime arveldusaadressi linna)

> **Järeldus:** Täthskeemi tulemus on identne olenemata allikast. Erinevus on ETL keerukuses. Normaliseeritud allikas nõuab rohkem JOIN-e, aga annab täpsema kontrolli andmete üle.

### 9. Analüütilised päringud

Proovime mõned päringud, mis näitavad star schema eeliseid.

**Müük kategooriate kaupa:**

```sql
SELECT
    p.Category,
    COUNT(f.SaleID)             AS tehinguid,
    SUM(f.TotalAmount)          AS muuk_kokku,
    ROUND(AVG(f.TotalAmount), 2) AS keskmine
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.Category
ORDER BY muuk_kokku DESC;
```

**Top 5 toodet:**

```sql
SELECT
    p.ProductName,
    p.Category,
    SUM(f.Quantity)    AS kogus,
    SUM(f.TotalAmount) AS tulu
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName, p.Category
ORDER BY tulu DESC
LIMIT 5;
```

**Müük regioonide kaupa jooksva summana (aknafunktsioon):**

```sql
SELECT
    d.FullDate,
    s.Region,
    SUM(f.TotalAmount) AS paeva_muuk,
    SUM(SUM(f.TotalAmount)) OVER (
        PARTITION BY s.Region ORDER BY d.FullDate
    ) AS jooksev_summa
FROM FactSales f
JOIN DimDate  d ON f.DateKey  = d.DateKey
JOIN DimStore s ON f.StoreKey = s.StoreKey
GROUP BY d.FullDate, s.Region
ORDER BY s.Region, d.FullDate;
```

**Toodete edetabel kategooria sees:**

```sql
SELECT
    p.Category,
    p.ProductName,
    SUM(f.TotalAmount) AS tulu,
    RANK() OVER (
        PARTITION BY p.Category ORDER BY SUM(f.TotalAmount) DESC
    ) AS koht
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.Category, p.ProductName
ORDER BY p.Category, koht;
```

Märka: tähtskeemi puhul on JOIN-id lihtsa(ma)d ja ühesugused. Faktitabel ühendatakse dimensioondeiga surrogaat võtmete kaudu.

Täiendavad päringud on failis `sql/03_analytical_queries.sql`:

```bash
docker exec -it praktikum-db-02 bash -c "psql -U praktikum -f /sql/03_analytical_queries.sql"
```

### 10. Kokkuvõte: Kimballi neljasammuline protsess

| Samm | Küsimus | Meie valik |
|------|---------|-----------|
| 1. Äriprotsess | Millist protsessi modelleerime? | Jaemüük |
| 2. Granulaarsus | Mida iga rida esindab? | Üks toode ühes tehingus |
| 3. Dimensioonid | Milliste atribuutide järgi analüüsime? | Kuupäev, pood, toode, klient, makseviis |
| 4. Faktid/Mõõdikud | Mida mõõdame? | Kogus, ühikuhind, kogusumma |

**Allikate võrdlus:**

| | CSV eksport (Näide 1) | OLTP andmebaas (Näide 2) |
|---|---|---|
| **Allikstruktuur** | 1 lai tabel (16 veergu) | 11 normaliseeritud tabelit |
| **JOIN-e laadimiseks** | 5 | ~14 |
| **Keerukus** | Lihtne | Keeruline |
| **Pärismaailma analoog** | Exceli eksport, CSV raport | SAP, Oracle EBS, PostgreSQL OLTP |

> Demo lõpp! Aeg ülesannete lahendamiseks.

---

## Ülesanne 1: SCD Type 2 implementeerimine

### Mis on ülesanne?

Kliendi Alice Smith kolimise Tallinnast Tartusse kajastamine andmelaos, kasutades SCD Type 2 mustrit.

Demo DimCustomer tabelis **puuduvad** ajalooväljad (ValidFrom, ValidTo). See ülesanne lisab need.

### Sammud

1. Lisa DimCustomer tabelisse veerud `ValidFrom` ja `ValidTo` (ALTER TABLE)
2. Sea olemasolevatele kirjetele vaikimisi väärtused (ValidFrom = 2025-01-01, ValidTo = 9999-12-31)
3. Sulge Alice'i praegune kirje — sea `ValidTo = CURRENT_DATE - INTERVAL '1 day'`
4. Lisa uus kirje Alice'ile uue linnaga (Tartu), uue `CustomerKey`-ga
5. Lisa uus müügitehing Alice'i uuele kirjele
6. Kirjuta päring, mis näitab müüki linnade kaupa läbi ajaloo

### Nõuded

- `ValidTo = '9999-12-31'` tähistab aktiivset kirjet
- Uus kirje peab saama **uue CustomerKey** (surrogaat võti), aga **sama CustomerID** (loomulik võti)
- Ajaloolised müügid jäävad vana CustomerKey juurde, uued müügid lähevad uuele

### Juhised

Avage fail `sql/04_scd2.sql` ja kirjutage vajalikud SQL laused.

```bash
docker exec -it praktikum-db-02 psql -U praktikum -f /sql/04_scd2.sql
```

### Kontrollküsimused

Mõtle:
- Miks on Alice'il **uus** CustomerKey, mitte endiselt sama?
- Mis juhtub, kui pärid `FactSales JOIN DimCustomer` ilma ValidTo filtrita?
- Miks kasutame surrogaat võtit (CustomerKey) ja mitte loomulikku võtit (CustomerID) faktitabelis?
- Kuidas käituks **Type 1** selle stsenaariumi korral? Mis läheks kaduma?

> Lahendust vaata: `tmp/solution/01_scd2_solution.sql`

---

## Ülesanne 2: Lumehelbe skeem (_Snowflake schema_) + EXPLAIN ANALYZE

### Mis on ülesanne?

Võrrelda tähtskeemi ja lumehelbe skeemi lähenemisi, analüüsida päringute jõudlust.

### Lumehelbe skeemi mõiste

Tähtskeemi puhul on kõik dimensiooni atribuudid ühes tabelis (nt DimProduct sisaldab Category ja Brand otse). Lumehelbe skeem  **normaliseerib** dimensioone: Category ja Brand on eraldi tabelites.

### Sammud

1. Loo eraldi tabelid **DimCategory** (CategoryKey, CategoryName) ja **DimBrand** (BrandKey, BrandName)
2. Loo **DimProductSnowflake** tabel, mis viitab DimCategory ja DimBrand tabelitele FK kaudu
3. Laadi andmed olemasolevast DimProduct tabelist uude struktuuri
4. Kirjuta **sama päring** mõlemas variandis (müük kategooriate kaupa)
5. Käivita mõlema päringu kohta `EXPLAIN ANALYZE` ja võrdle tulemusi

### EXPLAIN ANALYZE juhised

```sql
EXPLAIN ANALYZE
SELECT p.Category, SUM(f.TotalAmount)
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.Category;
```

Vaata väljundist:
- `Planning Time` ja `Execution Time` — mitu ms kulus?
- JOIN strateegia — Hash Join, Nested Loop, Merge Join?
- Ridade arvu hinnangud — `rows=...` vs tegelik

> **NB!** Meie andmemahuga (~1000 rida) ei pruugi jõudluse vahe olla märkimisväärne. Arutame, miks miljonite ridade korral vahe oluline on.

### Arhitektuuriotsuse harjutus

Kirjuta lühike (4-6 lauset) arhitektuuriotsus:

1. **Probleem.** Kuidas modelleerida toote dimensiooni?
2. **Variandid.** Star schema vs snowflake schema.
3. **Valik ja põhjendus.** Kumba eelistad ja miks?
4. **Kompromissid.** Mida kaotame valitud lähenemisega?

> Lahendust vaata: `tmp/solution/02_snowflake_solution.sql`

---

## Ülesanne 3: Python ETL — OLTP allikast star schemasse

### Mis on ülesanne?

Kirjutada Python skript, mis loeb denormaliseeritud CSV allikandmed, transformeerib need dimensioonideks ja faktideks ning laadib PostgreSQL star schemasse.

### Nõuded

- Skript peab olema **idempotentne** (korduv käivitamine ei tekita duplikaate)
- ETL jooksude logimine `etl_log` tabelisse
- Andmebaasi ühenduse parameetrid **keskkonnamuutujatest** (mitte hardcode)
- Veakäsitlus: vead logitakse `etl_log` tabelisse

### Juhised

Ava fail `scripts/etl_star_schema_template.py` — seal on funktsioonide struktuur ette antud.

Täida funktsioonid:
1. **`extract(csv_path)`** — loe CSV fail, tagasta list of dict
2. **`transform_dimensions(rows)`** — eralda unikaalsed dimensioonide väärtused
3. **`transform_facts(rows)`** — valmista ette faktitabeli read (teisenda tüübid)
4. **`load(conn, dimensions, facts)`** — laadi PostgreSQL-i (TRUNCATE + INSERT)

### Vihjed

- `csv.DictReader` loeb CSV faili, kus iga rida on dict
- `customer_name` tuleb jagada `first_name` ja `last_name` osadeks (`split`)
- Kuupäeva nädalapäev: `datetime.strptime(d, '%Y-%m-%d').strftime('%A')`
- Pärast dimensioonide laadimist loe surrogate key'd tagasi:
  ```python
  cur.execute("SELECT CustomerKey, CustomerID FROM DimCustomer")
  customer_keys = {row[1]: row[0] for row in cur.fetchall()}
  ```
- Kasuta `TRUNCATE ... RESTART IDENTITY CASCADE` idempotentsuse jaoks

### Käivitamine

```bash
docker exec -it praktikum-python-02 bash -c "python /scripts/etl_star_schema_template.py"
```

### Kontroll

```sql
-- Kas andmed laaditi?
SELECT COUNT(*) FROM FactSales;

-- Kas dimensioonid on korrektsed?
SELECT COUNT(*) FROM DimCustomer;
SELECT COUNT(*) FROM DimProduct;

-- ETL logid
SELECT start_time, duration_seconds, rows_loaded, status
FROM etl_log
ORDER BY start_time DESC
LIMIT 5;
```

---

## Ülesanne 4: Uus dimensioon — DimTime

### Mis on ülesanne?

Lisa tähtskeemile uus ajaline dimensioon, mis võimaldab tunni-põhist analüüsi.

### Sammud

1. Loo **DimTime** tabel:
   - `TimeKey INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY`
   - `Hour INT` (0-23)
   - `MinuteBlock VARCHAR(20)` (nt '00-59')
   - `TimeOfDay VARCHAR(20)` (Hommik, Lõuna, Pärastlõuna, Õhtu, Öö)
   - `IsBusinessHour BOOLEAN`

2. Täida tabel 24 tunniga (üks rida tunni kohta, kasuta `generate_series`)

3. Lisa FactSales tabelisse `TimeKey` veerg:
   ```sql
   ALTER TABLE FactSales ADD COLUMN TimeKey INT REFERENCES DimTime(TimeKey);
   ```

4. Uuenda olemasolevaid ridu simuleeritud aegadega (nt `SaleID % 24`)

5. Kirjuta päring: **müük kellaaja ja poe kaupa**

```sql
SELECT t.TimeOfDay, s.StoreName, SUM(f.TotalAmount) AS muuk
FROM FactSales f
JOIN DimTime t ON f.TimeKey = t.TimeKey
JOIN DimStore s ON f.StoreKey = s.StoreKey
GROUP BY t.TimeOfDay, s.StoreName
ORDER BY t.TimeOfDay;
```

---

## Kokkuvõte

Selles praktikumis:

1. **Kimballi metoodika** — kavandasime tähtskeemi neljas sammus: äriprotsess, granulaarsus, dimensioonid, faktid
2. **Kaks allikat** — nägime, kuidas sama tähtskeem ehitatakse nii CSV ekspordile kui normaliseeritud OLTP andmebaasile
3. **OLTP → OLAP** — transformeerisime allikandmed struktureeritud tähtskeemiks (5 dimensiooni + faktitabel)
4. **Analüütilised päringud** — kasutasime GROUP BY, aknafunktsioone (SUM OVER, RANK) ja JOIN mustreid
5. **SCD Type 2** — implementeerisime dimensiooni ajaloo säilitamise
6. **Star vs Snowflake** — võrdlesime normaliseeritud ja denormaliseeritud dimensioone EXPLAIN ANALYZE abil

---

## Koristamine

```bash
docker compose down -v
```

See kustutab konteinerid ja andmed.
