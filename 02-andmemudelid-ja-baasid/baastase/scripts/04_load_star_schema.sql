TRUNCATE TABLE fact_muuk, dim_toode, dim_klient RESTART IDENTITY;

INSERT INTO dim_klient (kliendi_id, kliendi_nimi, kliendityyp)
SELECT DISTINCT
    kliendi_id,
    kliendi_nimi,
    kliendityyp
FROM source_muuk
ORDER BY kliendi_id;

INSERT INTO dim_toode (toote_kood, toote_nimi, kategooria)
SELECT DISTINCT
    toote_kood,
    toote_nimi,
    kategooria
FROM source_muuk
ORDER BY toote_kood;

INSERT INTO fact_muuk (
    kuupaev,
    tellimuse_nr,
    klient_key,
    toode_key,
    kogus,
    muugisumma
)
SELECT
    s.kuupaev,
    s.tellimuse_nr,
    k.klient_key,
    t.toode_key,
    s.kogus,
    ROUND((s.kogus * s.uhikuhind)::NUMERIC, 2) AS muugisumma
FROM source_muuk s
JOIN dim_klient k
    ON s.kliendi_id = k.kliendi_id
JOIN dim_toode t
    ON s.toote_kood = t.toote_kood
ORDER BY s.kuupaev, s.tellimuse_nr, s.toote_kood;
