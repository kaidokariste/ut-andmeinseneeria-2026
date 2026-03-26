DROP TABLE IF EXISTS source_muuk;

CREATE TABLE source_muuk (
    tellimuse_nr TEXT NOT NULL,
    kuupaev DATE NOT NULL,
    kliendi_id TEXT NOT NULL,
    kliendi_nimi TEXT NOT NULL,
    kliendityyp TEXT NOT NULL,
    toote_kood TEXT NOT NULL,
    toote_nimi TEXT NOT NULL,
    kategooria TEXT NOT NULL,
    kogus INTEGER NOT NULL,
    uhikuhind NUMERIC(10,2) NOT NULL
);
