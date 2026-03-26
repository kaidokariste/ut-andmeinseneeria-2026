DROP TABLE IF EXISTS dim_kuupaev;

CREATE TABLE dim_kuupaev AS
WITH piirid AS (
    SELECT
        DATE '2025-09-01' AS algus_kuupaev,
        DATE '2025-09-30' AS lopp_kuupaev
),
kuupaevad AS (
    SELECT generate_series(algus_kuupaev, lopp_kuupaev, INTERVAL '1 day')::DATE AS kuupaev
    FROM piirid
)
SELECT
    TO_CHAR(kuupaev, 'YYYYMMDD')::INTEGER AS kuupaev_key,
    kuupaev,
    EXTRACT(DAY FROM kuupaev)::INTEGER AS paeva_nr_kuus,
    EXTRACT(DOY FROM kuupaev)::INTEGER AS paeva_nr_aastas,
    EXTRACT(ISODOW FROM kuupaev)::INTEGER AS nadalapaev_nr,
    CASE EXTRACT(ISODOW FROM kuupaev)::INTEGER
        WHEN 1 THEN 'esmaspaev'
        WHEN 2 THEN 'teisipaev'
        WHEN 3 THEN 'kolmapaev'
        WHEN 4 THEN 'neljapaev'
        WHEN 5 THEN 'reede'
        WHEN 6 THEN 'laupaev'
        WHEN 7 THEN 'puhapaev'
    END AS nadalapaev_nimi,
    EXTRACT(WEEK FROM kuupaev)::INTEGER AS nadal_nr,
    EXTRACT(MONTH FROM kuupaev)::INTEGER AS kuu_nr,
    CASE EXTRACT(MONTH FROM kuupaev)::INTEGER
        WHEN 1 THEN 'jaanuar'
        WHEN 2 THEN 'veebruar'
        WHEN 3 THEN 'marts'
        WHEN 4 THEN 'aprill'
        WHEN 5 THEN 'mai'
        WHEN 6 THEN 'juuni'
        WHEN 7 THEN 'juuli'
        WHEN 8 THEN 'august'
        WHEN 9 THEN 'september'
        WHEN 10 THEN 'oktoober'
        WHEN 11 THEN 'november'
        WHEN 12 THEN 'detsember'
    END AS kuu_nimi,
    EXTRACT(QUARTER FROM kuupaev)::INTEGER AS kvartal,
    EXTRACT(YEAR FROM kuupaev)::INTEGER AS aasta,
    CASE
        WHEN EXTRACT(ISODOW FROM kuupaev)::INTEGER IN (6, 7) THEN 1
        ELSE 0
    END AS nadalavahetus_ind,
    CASE
        WHEN EXTRACT(ISODOW FROM kuupaev)::INTEGER BETWEEN 1 AND 5 THEN 1
        ELSE 0
    END AS toopaev_ind
FROM kuupaevad
ORDER BY kuupaev;

ALTER TABLE dim_kuupaev
ADD PRIMARY KEY (kuupaev_key);
