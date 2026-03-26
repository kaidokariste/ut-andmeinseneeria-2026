DROP TABLE IF EXISTS fact_muuk;
DROP TABLE IF EXISTS dim_toode;
DROP TABLE IF EXISTS dim_klient;

CREATE TABLE dim_klient (
    klient_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kliendi_id TEXT NOT NULL UNIQUE,
    kliendi_nimi TEXT NOT NULL,
    kliendityyp TEXT NOT NULL
);

CREATE TABLE dim_toode (
    toode_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    toote_kood TEXT NOT NULL UNIQUE,
    toote_nimi TEXT NOT NULL,
    kategooria TEXT NOT NULL
);

CREATE TABLE fact_muuk (
    muuk_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kuupaev DATE NOT NULL,
    tellimuse_nr TEXT NOT NULL,
    klient_key INTEGER NOT NULL REFERENCES dim_klient (klient_key),
    toode_key INTEGER NOT NULL REFERENCES dim_toode (toode_key),
    kogus INTEGER NOT NULL,
    muugisumma NUMERIC(10,2) NOT NULL
);
