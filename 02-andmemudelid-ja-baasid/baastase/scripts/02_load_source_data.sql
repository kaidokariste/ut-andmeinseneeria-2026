TRUNCATE TABLE source_muuk;

\copy source_muuk (
    tellimuse_nr,
    kuupaev,
    kliendi_id,
    kliendi_nimi,
    kliendityyp,
    toote_kood,
    toote_nimi,
    kategooria,
    kogus,
    uhikuhind
)
FROM '/data/veebipoe_muuk.csv'
WITH (FORMAT csv, HEADER true);
