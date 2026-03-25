# Praktikum 2: Lihtne faktitabel ja kaks dimensiooni

## Sisukord

- [Praktikumi eesmärk](#praktikumi-eesmärk)
- [Õpiväljundid](#õpiväljundid)
- [Hinnanguline ajakulu](#hinnanguline-ajakulu)
- [Eeldused](#eeldused)
- [Enne alustamist](#enne-alustamist)
- [Ava õige kaust](#1-ava-õige-kaust)
- [Praktikumi failid](#praktikumi-failid)
- [Kus need failid praktikumi ajal asuvad?](#kus-need-failid-praktikumi-ajal-asuvad)
- [Miks see teema on oluline?](#miks-see-teema-on-oluline)
- [Uued mõisted](#uued-mõisted)
- [Tähtis vahe: terminal ja `psql`](#tähtis-vahe-terminal-ja-psql)
- [1. Loo `.env` fail](#2-loo-env-fail)
- [2. Käivita andmebaas](#3-käivita-andmebaas)
- [3. Loo ühendus andmebaasiga](#4-loo-ühendus-andmebaasiga)
- [4. Loo toorandmete tabel](#5-loo-toorandmete-tabel)
- [5. Laadi CSV-fail toortabelisse](#6-laadi-csv-fail-toortabelisse)
- [6. Uuri toorandmeid enne mudeli loomist](#7-uuri-toorandmeid-enne-mudeli-loomist)
- [7. Loo tähtskeemi tabelid](#8-loo-tähtskeemi-tabelid)
- [8. Täida dimensioonid ja faktitabel](#9-täida-dimensioonid-ja-faktitabel)
- [9. Vaata, kuidas tähtskeem töötab](#10-vaata-kuidas-tähtskeem-töötab)
- [10. Kirjuta esimesed analüütilised päringud](#11-kirjuta-esimesed-analüütilised-päringud)
- [Kontrollpunktid](#kontrollpunktid)
- [Levinud vead ja lahendused](#levinud-vead-ja-lahendused)
- [Lühike kokkuvõte](#lühike-kokkuvõte)
- [Lisaülesanne](#lisaülesanne)
- [Koristamine](#koristamine)

## Praktikumi eesmärk

Selle praktikumi eesmärk on teha esimene lihtne samm relatsioonilisest toorandmest dimensionaalse mudeli suunas. Laeme sisse väikese veebipoe müügiandmestiku, vaatame seda algsel kujul ning ehitame selle põhjal ühe lihtsa faktitabeli ja kaks dimensioonitabelit.

## Õpiväljundid

Praktikumi lõpuks oskab õppija:

- käivitada praktikumi andmebaasikeskkonna;
- avada `psql` käsurea kliendi konteineri sees;
- laadida denormaliseeritud CSV-faili andmebaasi tabelisse;
- luua lihtsa tähtskeemi, milles on üks faktitabel ja kaks dimensioonitabelit;
- kirjutada lihtsaid `SELECT`, `JOIN` ja `GROUP BY` päringuid;
- selgitada, miks sama äriinfo ei jää dimensionaalses mudelis ühte toortabelisse.

## Hinnanguline ajakulu

Arvesta umbes 2 tunniga. Kui keskkond on juba eelmisest praktikumikorrast töökorras, võib põhiosa minna kiiremini.

## Eeldused

Vaja on:

- VS Code'i või GitHub Codespacesit;
- terminali; Windowsis soovitame kasutada PowerShelli, macOS-is, Linuxis ja Codespacesis oma tavalist terminali;
- töötavat Dockeri keskkonda, kui teed praktikumi oma arvutis; GitHub Codespacesis on see praktikumi jaoks juba olemas;
- selle repositooriumi faile kohalikul masinal või Codespace'is.

## Enne alustamist

### Lühikordus esimesest praktikumikorrast

Selle praktikumi eelduseks on, et vähemalt järgmised tegevused on sulle tuttavad:

- oskad avada õige kausta VS Code'is;
- oskad luua faili `.env.example` põhjal uue faili `.env`;
- oskad käivitada käsu `docker compose up -d`;
- oskad avada `psql` käsurea kliendi käsuga `docker compose exec ... psql ...`;
- saad aru, et osa käske käib sinu arvuti või Codespace'i terminalis ja osa käib `psql` sees.

Kui mõni neist sammudest on veel ebakindel, vaata enne või praktikumi käigus uuesti:

- [Praktikum 1: PostgreSQL-iga ühenduse loomine ja esimese CSV-faili laadimine](../../01-andmeinseneeria-alused/baastase/README.md)

### Millist tööriista kasutada?

Selles praktikumis soovitame ühte väga konkreetset tööteed:

- ava see kaust VS Code'is;
- ava terminal VS Code'i menüüst `Terminal -> New Terminal`;
- sisesta kõik käsud sellesse samasse terminaliaknasse;
- ava SQL-failid ja CSV-fail VS Code'is.

Selle juhendi vaikimisi eeldus on:

- Windowsis kasutad PowerShelli;
- macOS-is, Linuxis ja Codespacesis kasutad tavalist terminali, mille käske märgime siin `bash` plokkides;
- kui terminalikäsu juures ei ole eraldi märget, siis töötab sama käsk nii Bashis kui ka PowerShellis.

Võid kasutada ka eraldi terminalirakendust:

- Windowsis PowerShelli;
- macOS-is rakendust Terminal;
- Linuxis oma tavalist terminali.

Aga siis kontrolli eriti hoolikalt, et liigud õigesse kausta enne käskude käivitamist.

### See praktikum sobib ka GitHub Codespacesis läbimiseks.

Codespaces avab repositooriumi brauseris VS Code'i vaates, kus on olemas:

- failivaade;
- sisseehitatud terminal;
- sama kaustastruktuur, mida selles juhendis kasutame.

Eeldused:

- sul on GitHubi kasutaja;

Kasulikud lingid:

- repositoorium: <https://github.com/KristoR/ut-andmeinseneeria-2026>
- sinu Codespacesi sessioonid: <https://github.com/codespaces>
- GitHubi juhend Codespace'i loomiseks: <https://docs.github.com/en/codespaces/developing-in-a-codespace/creating-a-codespace-for-a-repository>
- GitHubi juhend Codespace'i kustutamiseks: <https://docs.github.com/en/codespaces/developing-in-a-codespace/deleting-a-codespace>
- GitHubi info Codespacesi kulude ja salvestusmahu kohta: <https://docs.github.com/en/billing/managing-billing-for-your-products/managing-billing-for-github-codespaces/about-billing-for-github-codespaces>

Kui tahad selle repositooriumi avada Codespacesis:

1. Ava GitHubis repositooriumi leht.
2. Vali vajadusel õige haru.
3. Vajuta `Code`.
4. Ava vahekaart `Codespaces`.
5. Vali `Create codespace on ...`.
6. Kui Codespace on avanenud, ava terminal menüüst `Terminal -> New Terminal`.

Codespacesi hügieen:

- hoia korraga avatuna üks sessioon selle repositooriumi jaoks;
- kui tahad tööd jätkata, ava pigem olemasolev sessioon kui loo uus;
- pärast praktikumi peata sessioon;
- kui sessiooni enam vaja ei ole, kustuta see lehelt <https://github.com/codespaces>, et see ei kulutaks salvestusmahtu ja sinu kasutuskvooti.

## Enne alustamist: kuidas saada kätte selle nädala failid?

Kui töötasid eelmisel nädalal oma arvutis kohaliku koopiaga, siis kontrolli enne praktikumi, et sul oleksid olemas selle nädala failid.

### Kui kasutasid eelmisel nädalal `git clone` käsku

Sel juhul on kõige lihtsam tee tavaliselt olemasolevat koopiat uuendada.

1. Ava terminal repositooriumi juurkaustas.
2. Kontrolli olekut:

```bash
git status
```

3. Kui näed, et sul ei ole pooleli kohalikke muudatusi, uuenda failid:

```bash
git pull
```

Kui `git pull` annab veateate kohalike muudatuste kohta ja sa ei tea, kuidas edasi minna, siis kõige turvalisem algaja variant on võtta praktikumi jaoks uus puhas koopia eraldi kausta.

### Kui laadisid eelmisel nädalal GitHubist ZIP-faili

Sellisel juhul ei saa olemasolevat kausta ühe käsuga uuendada.

Kõige lihtsam tee on:

1. ava repositoorium GitHubis: <https://github.com/KristoR/ut-andmeinseneeria-2026>
2. vajuta `Code`
3. vali `Download ZIP`
4. paki fail lahti uude kausta
5. ava praktikumi jaoks just see uus kaust

Soovitus:

- ära kirjuta eelmist kausta automaatselt üle;
- kui sul on eelmise nädala kaustas enda märkmeid või muudatusi, hoia see alles ja tee praktikumi jaoks uus värske koopia.

## 1. Ava õige kaust

Ava kaust `02-andmemudelid-ja-baasid/baastase`.

Kui alustad repo juurkaustast, kasuta terminalis:

```bash
cd 02-andmemudelid-ja-baasid/baastase
```

Soovi korral kontrolli oma asukohta.

macOS-is, Linuxis ja Codespacesis:

```bash
pwd
```

Windows PowerShellis:

```powershell
Get-Location
```

## Praktikumi failid

Kõik allpool toodud relatiivsed failiteed eeldavad, et oled `02-andmemudelid-ja-baasid/baastase` kaustas.

- [`compose.yml`](./compose.yml) kirjeldab praktikumi andmebaasikonteinerit
- [`.env.example`](./.env.example) sisaldab ühenduse vaikimisi väärtusi
- [`data/veebipoe_muuk.csv`](./data/veebipoe_muuk.csv) on praktikumi toorandmestik
- [`scripts/01_create_source_table.sql`](./scripts/01_create_source_table.sql) loob toorandmete tabeli
- [`scripts/02_load_source_data.sql`](./scripts/02_load_source_data.sql) laadib CSV-faili tabelisse
- [`scripts/03_create_star_schema.sql`](./scripts/03_create_star_schema.sql) loob dimensioonid ja faktitabeli
- [`scripts/04_load_star_schema.sql`](./scripts/04_load_star_schema.sql) täidab dimensioonid ja faktitabeli
- [`scripts/05_check_results.sql`](./scripts/05_check_results.sql) sisaldab kontrollpäringuid
- [`scripts/99_reset.sql`](./scripts/99_reset.sql) kustutab praktikumi tabelid, kui soovid alustada uuesti
- [`data/veebipoe_muuk_lisaulesanne.csv`](./data/veebipoe_muuk_lisaulesanne.csv) on lisaülesande rikkam toorandmestik
- [`scripts/lisa_01_create_source_table_laiem.sql`](./scripts/lisa_01_create_source_table_laiem.sql) loob lisaülesande lähtetabeli
- [`scripts/lisa_02_load_source_data_laiem.sql`](./scripts/lisa_02_load_source_data_laiem.sql) laadib lisaülesande CSV faili tabelisse
- [`scripts/lisa_03_create_dim_kuupaev.sql`](./scripts/lisa_03_create_dim_kuupaev.sql) genereerib etteantud kuupäevavahemikust `dim_kuupaev` tabeli

## Kus need failid praktikumi ajal asuvad?

Praktikumi ajal on sul korraga kaks vaadet samadele failidele.

- VS Code'is näed faile nende tavalises asukohas, näiteks `./scripts` ja `./data`
- andmebaasi konteineri sees on samad kaustad nähtavad teedel `/scripts` ja `/data`

Seetõttu:

- avad faili VS Code'is näiteks teelt `scripts/03_create_star_schema.sql`
- käivitad sama faili `psql` sees käsuga `\i /scripts/03_create_star_schema.sql`

See vahe on oluline, sest just siin lähevad alguses kõige sagedamini segi hosti failiteed ja konteineri failiteed.

## Miks see teema on oluline?

Loengus nägid, et operatiivsed andmed ja analüütiline mudel ei ole sama asi.

Päriselus tuleb väga tihti ette olukord, kus:

- allikas annab sulle ühe suure tabeli või ekspordi;
- seal kordub sama kliendi ja toote info paljudes ridades;
- analüüsiks oleks mugavam kasutada eraldi kirjeldavaid tabeleid ja eraldi mõõdikuid kandvat tabelit.

See praktikum on esimene väike samm selle mõttemudeli suunas. Me ei tee veel täismahus andmeladu, aga teeme läbi kõige olulisema loogika:

- mis on toorandmed;
- mis on dimensioon;
- mis on fakt;
- kuidas `JOIN` ja `GROUP BY` aitavad seda mudelit kasutada.

## Uued mõisted

### Denormaliseeritud toortabel

See on tabel, kus ühe rea sees on korraga nii kliendi, toote kui ka müügisündmuse info.

Selles praktikumis on selleks tabel `source_muuk`.

### Dimensioonitabel

Dimensioonitabel hoiab kirjeldavaid tunnuseid, mille järgi me tahame hiljem andmeid filtreerida või rühmitada.

Selles praktikumis loome kaks dimensiooni:

- `dim_klient`
- `dim_toode`

### Faktitabel

Faktitabel hoiab mõõdetavat sündmust.

Selles praktikumis on selleks `fact_muuk`, kus iga rida kirjeldab ühe toote müüki ühel tellimusereal.

### Surrogaatvõti

Surrogaatvõti on andmelao sisemine tehniline võti, mis ei tule otse alliksüsteemist.

Selles praktikumis on näiteks:

- `klient_key`
- `toode_key`

### Granulaarsus

Granulaarsus vastab küsimusele: mida üks rida tähendab?

Selles praktikumis kasutame granulaarsust:

- üks rida = ühe toote müük ühel tellimusereal

## Tähtis vahe: terminal ja `psql`

Praktikumi jooksul vahetad kahe käsukeskkonna vahel.

### Tavaline terminal

Siin käivitad näiteks:

- `cd`
- `docker compose up -d`
- `docker compose exec db psql -U praktikum -d praktikum`

### `psql`

Kui oled käsuga andmebaasi sisse läinud, muutub prompt näiteks selliseks:

```text
praktikum=#
```

Siis käivad käsud juba andmebaasis, näiteks:

```sql
\dt
SELECT * FROM source_muuk LIMIT 5;
\q
```

Kui näed `praktikum=#`, oled `psql` sees. Kui näed tavalist terminaliprompti, oled tagasi terminalis.

## 2. Loo `.env` fail

macOS-is, Linuxis ja tavaliselt ka Codespacesis:

```bash
cp .env.example .env
```

Windows PowerShellis:

```powershell
Copy-Item .env.example .env
```

Vaikimisi väärtused on:

- kasutaja: `praktikum`
- parool: `praktikum`
- andmebaas: `praktikum`

Praegu ei ole vaja neid muuta.

## 3. Käivita andmebaas

See samm käib sinu arvuti (VS Code) või Codespace'i terminalis.

```bash
docker compose up -d
```

Kontrolli, et teenus töötab:

```bash
docker compose ps
```

Oodatav tulemus:

- teenuse `db` olek on `running` või `healthy`

Kui näed alguses olekut `starting`, oota mõni sekund ja käivita `docker compose ps` uuesti.

Märkus:

- selles praktikumis kasutame hosti pordi `5433`, mitte `5432`;
- nii ei lähe see keskkond konflikti esimese praktikumi konteineriga, kui see on veel töös.

## 4. Loo ühendus andmebaasiga

See samm algab sinu arvuti (VS Code) või Codespace'i terminalis ja lõpeb `psql` käsurea sees.

Kasuta teadlikult seda teed, mis ei eelda sinu arvutisse eraldi `psql` paigaldust:

```bash
docker compose exec db psql -U praktikum -d praktikum
```

Kui ühendus õnnestub, näed:

```text
praktikum=#
```

Proovi kahte esimest käsku:

```sql
SELECT current_database();
\dt
```

Kui tahad `psql`-ist väljuda, kasuta:

```sql
\q
```

Loo ühendus seejärel uuesti.

## 5. Loo toorandmete tabel

See samm käib `psql` sees.

Käivita:

```sql
\i /scripts/01_create_source_table.sql
```

Kontrolli tulemust:

```sql
\dt
```

Oodatav tulemus:

- tabelite hulgas on `source_muuk`

## 6. Laadi CSV-fail toortabelisse

See samm käib endiselt `psql` sees.

```sql
\i /scripts/02_load_source_data.sql
```

Kui laadimine õnnestub, näed tavaliselt väljundit `COPY 12`.

Kontrolli, et andmed jõudsid kohale:

```sql
SELECT * FROM source_muuk LIMIT 10;
```

Vaata ka ridu kokku:

```sql
SELECT COUNT(*) FROM source_muuk;
```

Oodatav tulemus:

- tabelis on 12 rida;
- sama `tellimuse_nr` võib korduda rohkem kui üks kord, sest ühes tellimuses võib olla mitu toodet.

## 7. Uuri toorandmeid enne mudeli loomist

See samm käib `psql` sees.

See samm aitab näha, millist sündmust meie andmed tegelikult kirjeldavad.

Modelleerimise eesmärk ei ole ainult dubleerimist vähendada. Eesmärk on teha mudel, millega on hiljem lihtsam esitada õigeid küsimusi ja raskem saada BI-tööriistas eksitavaid summasid.

Proovi järgmisi päringuid:

```sql
SELECT tellimuse_nr, COUNT(*) AS ridu
FROM source_muuk
GROUP BY tellimuse_nr
ORDER BY tellimuse_nr;
```

```sql
SELECT DISTINCT kliendi_id, kliendi_nimi, kliendityyp
FROM source_muuk
ORDER BY kliendi_id;
```

```sql
SELECT DISTINCT toote_kood, toote_nimi, kategooria
FROM source_muuk
ORDER BY toote_kood;
```

Pane tähele:

- kliendi info kordub mitmes reas;
- toote info kordub mitmes reas;
- mõõdetav sündmus on konkreetne müük, millel on kogus ja ühikuhind.

See ongi koht, kus dimensionaalne mudel on vaeva väärt. Me sõnastame selgelt, mida üks rida tähendab, ja paigutame kirjeldavad tunnused ning mõõdikud õigesse kohta.

### Dimensionaalse modelleerimise neli sammu selles praktikumis

Enne tabelite loomist tee endale selgeks neli otsust:

1. Vali äriprotsess.
   Selles praktikumis modelleerime veebipoe müüki.
2. Deklareeri granulaarsus.
   Üks rida kirjeldab ühe toote müüki ühel tellimusereal.
3. Vali dimensioonid.
   Selles praktikumis on nendeks klient ja toode.
4. Vali faktid.
   Selles praktikumis on nendeks kogus ja müügisumma.

Oluline mõttepunkt:

- päriselus võib sisendandmestik sisaldada korraga mitme granulaarsusega kirjeldavaid ja mõõdetavaid välju;
- näiteks võib tellimuserea andmete kõrval olla ka tellimuse kogusumma või tarnekulu;
- oma dimensiooni- ja faktitabelisse võtame ainult need tulbad, mis sobivad meie granulaarsuse lausega.

## 8. Loo tähtskeemi tabelid

See samm käib `psql` sees.

Järgmine SQL-fail loob tabelid nii, et need vastaksid eelmises sammus valitud äriprotsessile, granulaarsusele, dimensioonidele ja faktidele.

Enne faili käivitamist pane tähele, milliseid välju me sellesse mudelisse üldse valime.

### `dim_klient`: millised tulbad valime ja miks?

`dim_klient` tabelisse võtame:

- `kliendi_id`, et tunneksime sama kliendi allikast ära;
- `kliendi_nimi`, sest see on kirjeldav tunnus;
- `kliendityyp`, sest selle järgi võib hiljem andmeid rühmitada või filtreerida.

Lisaks loob andmeladu sellele tabelile tehnilise võtme `klient_key`, mida kasutame hiljem faktitabelis.

### `dim_toode`: millised tulbad valime ja miks?

`dim_toode` tabelisse võtame:

- `toote_kood`, et tunneksime sama toote allikast ära;
- `toote_nimi`, sest see on kirjeldav tunnus;
- `kategooria`, sest selle järgi saab hiljem andmeid rühmitada või filtreerida.

Lisaks loob andmeladu sellele tabelile tehnilise võtme `toode_key`, mida kasutame hiljem faktitabelis.

### `fact_muuk`: kuidas valime faktid?

`fact_muuk` tabelisse jätame ainult selle müügisündmuse konteksti ja mõõdikud, mis sobivad meie granulaarsuse lausega:

- `kuupaev`;
- `tellimuse_nr`;
- `klient_key`;
- `toode_key`;
- `kogus`;
- `muugisumma`.

`kuupaev`, `tellimuse_nr`, `klient_key` ja `toode_key` aitavad kirjeldada, millise müügisündmusega on tegu. `kogus` ja `muugisumma` on selle sündmuse mõõdikud, mida on hiljem mõistlik analüüsis kokku liita.

Toorandmestikus on selle müügisündmuse kohta olemas `uhikuhind`. Faktitabelisse me ei tõsta seda välja eraldi ümber, vaid arvutame selle põhjal sündmuse koguväärtuse `muugisumma`. Nii salvestame faktitabelisse mõõdiku, mis vastab otse küsimusele "kui palju müüsime?" selle ühe tellimuserea tasemel.

```sql
    muugisumma = kogus * uhikuhind
```

See valik hoiab faktitabeli kooskõlas meie granulaarsuse lausega: üks rida kirjeldab ühe toote müüki ühel tellimusereal. Iga `fact_muuk` rida kannab nüüd selle ühe sündmuse kogust ja väärtust.

Käivita:

```sql
\i /scripts/03_create_star_schema.sql
```

Vaata loodud tabeleid:

```sql
\dt
```

Sa peaksid nüüd nägema vähemalt nelja praktikumi tabelit:

- `source_muuk`
- `dim_klient`
- `dim_toode`
- `fact_muuk`

## 9. Täida dimensioonid ja faktitabel

See samm käib `psql` sees.

Nüüd, kui tabelite kuju on paigas, täidame dimensioonid ja faktitabeli toorandmestikust.

Käivita:

```sql
\i /scripts/04_load_star_schema.sql
```

Kontrolli ridade arvu:

```sql
SELECT 'source_muuk' AS tabel, COUNT(*) AS ridu FROM source_muuk
UNION ALL
SELECT 'dim_klient', COUNT(*) FROM dim_klient
UNION ALL
SELECT 'dim_toode', COUNT(*) FROM dim_toode
UNION ALL
SELECT 'fact_muuk', COUNT(*) FROM fact_muuk
ORDER BY tabel;
```

Siin kasutame `UNION ALL`-i selleks, et panna mitme `SELECT`-lause tulemused ühte väljundisse üksteise alla. Oluline on, et igal `SELECT`-il oleks sama struktuur: sama arv veerge ja sobivad andmetüübid.

Oodatav tulemus:

- `source_muuk`: 12 rida
- `dim_klient`: 5 rida
- `dim_toode`: 6 rida
- `fact_muuk`: 12 rida

## 10. Vaata, kuidas tähtskeem töötab

See samm käib `psql` sees.

Kontrollpäring, mis ühendab faktitabeli ja mõlemad dimensioonid:

```sql
SELECT
    f.tellimuse_nr,
    f.kuupaev,
    k.kliendi_nimi,
    k.kliendityyp,
    t.toote_nimi,
    t.kategooria,
    f.kogus,
    f.muugisumma
FROM fact_muuk f
JOIN dim_klient k ON f.klient_key = k.klient_key
JOIN dim_toode t ON f.toode_key = t.toode_key
ORDER BY f.kuupaev, f.tellimuse_nr, t.toote_nimi;
```

Selle päringu tulemusest peaksid nägema:

- faktitabel kannab mõõdetavat sündmust;
- dimensioonid annavad sellele sündmusele kirjeldava konteksti.

## 11. Kirjuta esimesed analüütilised päringud

See samm käib `psql` sees.

### Päring 1: müük kategooriate kaupa

```sql
SELECT
    t.kategooria,
    SUM(f.muugisumma) AS muuk_kokku
FROM fact_muuk f
JOIN dim_toode t ON f.toode_key = t.toode_key
GROUP BY t.kategooria
ORDER BY muuk_kokku DESC;
```

### Päring 2: müük klienditüüpide kaupa

```sql
SELECT
    k.kliendityyp,
    SUM(f.muugisumma) AS muuk_kokku
FROM fact_muuk f
JOIN dim_klient k ON f.klient_key = k.klient_key
GROUP BY k.kliendityyp
ORDER BY muuk_kokku DESC;
```

### Päring 3: enim müüdud tooted koguse järgi

```sql
SELECT
    t.toote_nimi,
    SUM(f.kogus) AS kogus_kokku
FROM fact_muuk f
JOIN dim_toode t ON f.toode_key = t.toode_key
GROUP BY t.toote_nimi
ORDER BY kogus_kokku DESC, t.toote_nimi;
```

Kui tahad, võid käivitada ka valmis kontrollfaili:

```sql
\i /scripts/05_check_results.sql
```

## Kontrollpunktid

Praktikumi lõpuks peaksid sa suutma kontrollida vähemalt järgmist:

- oskad öelda, mis on `source_muuk` ja miks see ei ole veel hea analüütiline mudel;
- oskad näidata, millised väljad läksid `dim_klient` tabelisse;
- oskad näidata, millised väljad läksid `dim_toode` tabelisse;
- oskad öelda, mida üks rida `fact_muuk` tabelis tähendab;
- oskad kirjutada vähemalt ühe `JOIN`-iga ja ühe `GROUP BY`-ga päringu.

## Levinud vead ja lahendused

### Käsk ei tööta, sest oled vales kaustas

Sümptom:

- terminalis käivitatud käsk ei leia faili või `docker compose` ei leia `compose.yml` faili.

Tõenäoline põhjus:

- oled vales kaustas.

Lahendus:

- kontrolli oma asukohta;
- liigu vajadusel praktikumi kausta.

macOS-is, Linuxis ja Codespacesis:

```bash
pwd
```

Windows PowerShellis:

```powershell
Get-Location
```

Vajadusel liigu õigesse kohta:

```bash
cd 02-andmemudelid-ja-baasid/baastase
```

### Docker ei käivitu

Sümptom:

- `docker --version` või `docker compose up -d` ei tööta.

Tõenäoline põhjus:

- Docker ei ole selles keskkonnas saadaval või ei tööta.

Lahendus:

- kontrolli kõigepealt, kas Docker on selles keskkonnas kasutatav.

```bash
docker --version
docker compose version
```

Kui esimene käsk ei tööta, ei ole Docker veel selles keskkonnas kasutatav.

Kui töötad oma arvutis ja ei saa Dockerit kiiresti tööle, siis kasuta selle praktikumi tegemiseks GitHub Codespacesit.

### Port on juba kinni

Sümptom:

- `docker compose up -d` annab veateate, et port on juba kasutuses.

Tõenäoline põhjus:

- mõni varasem konteiner kasutab juba sama porti.

Lahendus:

- vaata, millised teenused on praegu töös;
- peata vajadusel vana praktikumikeskkond.

```bash
docker compose ps
```

Kui esimese praktikumi konteiner on veel töös, ava esimese praktikumi kaust ja peata see käsuga:

```bash
docker compose down -v
```

### Sisestasid `\i` käsu tavalisse terminali

Sümptom:

- sisestad `\i /scripts/...` käsu, aga see ei tööta.

Tõenäoline põhjus:

- `\i` töötab ainult `psql` sees, mitte tavalises terminalis.

Lahendus:

- loo kõigepealt ühendus andmebaasiga ja käivita `\i` seejärel `psql` sees.

Kui näed tavalist shelli prompti, siis loo kõigepealt ühendus:

```bash
docker compose exec db psql -U praktikum -d praktikum
```

### `psql` näitab väljundit mitme leheküljena

Sümptom:

- ekraan jääb seisma ja all on näiteks `(END)`.

Tõenäoline põhjus:

- `psql` avas pika väljundi lehitsejas.

Lahendus:

- vajuta `q`, et väljundist tagasi tulla.

```text
q
```

## Lühike kokkuvõte

Selles praktikumis tegid läbi ühe väga väikese, aga olulise modelleerimistsükli:

1. laadisid sisse toorandmed;
2. uurisid, milline info seal kordub;
3. eraldasid kliendi ja toote eraldi dimensioonidesse;
4. jätsid mõõdetava müügisündmuse faktitabelisse;
5. kasutasid `JOIN` ja `GROUP BY` päringuid, et mudelilt küsimusi küsida.

See on hea alus järgmisteks teemadeks, kus mudel muutub suuremaks ja valikud vähem ilmseks.

## Lisaülesanne

Kui jõuad varem valmis, tee lisaülesanne kahe sammuna.

### Lisaülesanne 1: lisa `dim_kuupaev`

Kui tahad harjutada ühte uut dimensiooni ilma kogu mudelit ümber tegemata, loo kõigepealt `dim_kuupaev`.

Abiks on fail [`scripts/lisa_03_create_dim_kuupaev.sql`](./scripts/lisa_03_create_dim_kuupaev.sql).

See skript:

- genereerib kuupäevatabeli vahemikus `2025-09-01` kuni `2025-09-30`;
- loob võtme `kuupaev_key`;
- lisab mitu kirjeldavat tunnust, näiteks kuu, nädalanumbri, nädalapäeva nime ja tööpäeva tunnuse.

Kui tahad kasutada teist kuupäevavahemikku, muuda failis skripti alguses väärtusi `algus_kuupaev` ja `lopp_kuupaev`.

Proovi seejärel muuta oma mudelit nii, et faktitabel ei hoiaks enam otse välja `kuupaev`, vaid viitaks `kuupaev_key` kaudu tabelile `dim_kuupaev`.

### Lisaülesanne 2: ehita uus tähtskeem rikkama toorandmestiku põhjal

Kui tahad veidi suuremat modelleerimisülesannet, kasuta faili [`data/veebipoe_muuk_lisaulesanne.csv`](./data/veebipoe_muuk_lisaulesanne.csv).

See andmestik on suurem kui põhiülesande CSV ja sisaldab lisaks:

- kliendi linna;
- toote tootemarki;
- kampaania nime, tüüpi ja liiklusallikat;
- makseviisi;
- juba arvutatud müügisummat.

Soovituslik tööjärjekord:

1. Loo lähtetabel failiga [`scripts/lisa_01_create_source_table_laiem.sql`](./scripts/lisa_01_create_source_table_laiem.sql).
2. Laadi andmed failiga [`scripts/lisa_02_load_source_data_laiem.sql`](./scripts/lisa_02_load_source_data_laiem.sql).
3. Sõnasta uus granulaarsuse lause.
4. Kavanda selle põhjal uus tähtskeem.

Üks mõistlik lähtekoht on:

- üks rida = ühe toote müük ühel tellimusereal ühe kampaania kaudu ühel kuupäeval

Vihjed dimensioonide valikuks:

- `dim_kuupaev`
- `dim_klient`
- `dim_toode`
- `dim_kampaania`
- `dim_makseviis`

Vihjed faktide valikuks:

- `kogus`
- `muugisumma`

Kui lood uuele mudelile teised tabelinimed, kohanda ka allolevates näidispäringutes tabelinimesid.

Kui sinu mudel on hästi õnnestunud, peaksid sa saama kirjutada lühikesed ja loetavad päringud vähemalt kahele sellisele äriküsimusele:

```sql
SELECT
    d.nadalapaev_nimi,
    kp.kampaania_tyyp,
    SUM(f.muugisumma) AS muuk_kokku
FROM fact_muuk f
JOIN dim_kuupaev d ON f.kuupaev_key = d.kuupaev_key
JOIN dim_kampaania kp ON f.kampaania_key = kp.kampaania_key
GROUP BY d.nadalapaev_nimi, kp.kampaania_tyyp
ORDER BY kp.kampaania_tyyp, d.nadalapaev_nimi;
```

See päring kontrollib, kas kuupäeva- ja kampaaniadimensioonid töötavad.

```sql
SELECT
    k.kliendityyp,
    t.kategooria,
    SUM(f.muugisumma) AS muuk_kokku
FROM fact_muuk f
JOIN dim_klient k ON f.klient_key = k.klient_key
JOIN dim_toode t ON f.toode_key = t.toode_key
GROUP BY k.kliendityyp, t.kategooria
ORDER BY k.kliendityyp, muuk_kokku DESC;
```

See päring kontrollib, kas kliendi- ja tootedimensioonid töötavad ning kas müügisumma on õiges kohas faktitabelis.

## Koristamine

Kui soovid praktikumi keskkonna lõpus peatada:

```bash
docker compose down -v
```

See peatab konteineri ja kustutab praktikumi andmed.
