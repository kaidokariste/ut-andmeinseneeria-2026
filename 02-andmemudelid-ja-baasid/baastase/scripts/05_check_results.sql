SELECT 'dim_klient' AS tabel, COUNT(*) AS ridu FROM dim_klient
UNION ALL
SELECT 'dim_toode', COUNT(*) FROM dim_toode
UNION ALL
SELECT 'fact_muuk', COUNT(*) FROM fact_muuk
UNION ALL
SELECT 'source_muuk', COUNT(*) FROM source_muuk
ORDER BY tabel;

SELECT
    f.tellimuse_nr,
    f.kuupaev,
    k.kliendi_nimi,
    t.toote_nimi,
    t.kategooria,
    f.kogus,
    f.muugisumma
FROM fact_muuk f
JOIN dim_klient k ON f.klient_key = k.klient_key
JOIN dim_toode t ON f.toode_key = t.toode_key
ORDER BY f.kuupaev, f.tellimuse_nr, t.toote_nimi;

SELECT
    t.kategooria,
    SUM(f.muugisumma) AS muuk_kokku
FROM fact_muuk f
JOIN dim_toode t ON f.toode_key = t.toode_key
GROUP BY t.kategooria
ORDER BY muuk_kokku DESC;

SELECT
    k.kliendityyp,
    SUM(f.muugisumma) AS muuk_kokku
FROM fact_muuk f
JOIN dim_klient k ON f.klient_key = k.klient_key
GROUP BY k.kliendityyp
ORDER BY muuk_kokku DESC;
