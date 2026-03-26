DROP TABLE IF EXISTS source_muuk_laiem;

CREATE TABLE source_muuk_laiem (
    tellimuse_nr INTEGER NOT NULL,
    kuupaev DATE NOT NULL,
    kliendi_id INTEGER NOT NULL,
    kliendi_nimi TEXT NOT NULL,
    kliendi_linn TEXT NOT NULL,
    kliendityyp TEXT NOT NULL,
    toote_nimi TEXT NOT NULL,
    kategooria TEXT NOT NULL,
    tootemark TEXT NOT NULL,
    kampaania_nimi TEXT NOT NULL,
    kampaania_tyyp TEXT NOT NULL,
    liiklusallikas TEXT NOT NULL,
    makseviis TEXT NOT NULL,
    kogus INTEGER NOT NULL,
    uhikuhind NUMERIC(10,2) NOT NULL,
    muugisumma NUMERIC(10,2) NOT NULL
);
