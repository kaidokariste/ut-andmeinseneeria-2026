TRUNCATE TABLE source_muuk_laiem;

\copy source_muuk_laiem (
    tellimuse_nr,
    kuupaev,
    kliendi_id,
    kliendi_nimi,
    kliendi_linn,
    kliendityyp,
    toote_nimi,
    kategooria,
    tootemark,
    kampaania_nimi,
    kampaania_tyyp,
    liiklusallikas,
    makseviis,
    kogus,
    uhikuhind,
    muugisumma
)
FROM '/data/veebipoe_muuk_lisaulesanne.csv'
WITH (FORMAT csv, HEADER true);
