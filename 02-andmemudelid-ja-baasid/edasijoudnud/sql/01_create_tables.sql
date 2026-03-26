-- ============================================================
-- 01_create_tables.sql
-- Star schema tabelite loomine (PostgreSQL / pgduckdb)
--
-- Kimballi metoodika jargi kavandatud:
--   1. Ariprotsess: jaemuuk (retail sales)
--   2. Granulaarsus: uks rida = uks toode uhes muugitehingus
--   3. Dimensioonid: kuupaev, pood, toode, klient, makseviis
--   4. Moodikud: kogus, uhikuhind, kogusumma (liidetavad)
-- ============================================================

-- Idempotentsus: kustutame olemasolevad tabelid (jarjekord: fakt enne dimensioone)
DROP TABLE IF EXISTS FactSales CASCADE;
DROP TABLE IF EXISTS DimDate CASCADE;
DROP TABLE IF EXISTS DimStore CASCADE;
DROP TABLE IF EXISTS DimProduct CASCADE;
DROP TABLE IF EXISTS DimCustomer CASCADE;
DROP TABLE IF EXISTS DimPayment CASCADE;

-- ------------------------------------------------------------
-- Dimensioonitabelid
-- Iga dimensioon kasutab surrogate key'd (GENERATED ALWAYS AS IDENTITY).
-- Surrogate key on andmelao sisemine voti, mis ei soltu alliksusteemist.
-- ------------------------------------------------------------

-- Kuupaeva dimensioon: genereeritakse, mitte ei voeta allikast
-- Voimaldab ajalisi agregatsioone (aasta, kvartal, kuu, nadalapaev jne)
CREATE TABLE DimDate (
    DateKey     INTEGER PRIMARY KEY,  -- YYYYMMDD formaat (nt 20250115)
    FullDate    DATE NOT NULL UNIQUE,
    Year        INT NOT NULL,
    Quarter     INT NOT NULL,
    Month       INT NOT NULL,
    Day         INT NOT NULL,
    DayOfWeek   VARCHAR(15),
    DayOfYear   INT NOT NULL,
    WeekOfYear  INT NOT NULL,
    MonthName   VARCHAR(20),
    IsWeekend   BOOLEAN NOT NULL
);

-- Poe dimensioon: kus muuk toimus
CREATE TABLE DimStore (
    StoreKey  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    StoreName VARCHAR(100) NOT NULL,
    City      VARCHAR(50),
    Region    VARCHAR(50)
);

-- Toote dimensioon: mida muudi
CREATE TABLE DimProduct (
    ProductKey  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ProductName VARCHAR(100) NOT NULL,
    Category    VARCHAR(50),
    Brand       VARCHAR(50)
);

-- Kliendi dimensioon: kes ostis
-- NB: SCD Type 2 valjad (ValidFrom, ValidTo) lisatakse ulesandes 1
CREATE TABLE DimCustomer (
    CustomerKey INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    CustomerID  INT NOT NULL,
    FirstName   VARCHAR(50),
    LastName    VARCHAR(50),
    Segment     VARCHAR(50),
    City        VARCHAR(50)
);

-- Makseviisi dimensioon: kuidas maksti
CREATE TABLE DimPayment (
    PaymentKey  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PaymentType VARCHAR(30) NOT NULL
);

-- ------------------------------------------------------------
-- Faktitabel
-- Granulaarsus: uks rida = uks toode uhes muugitehingus
-- Moodikud (koik liidetavad):
--   Quantity    - ostetud kogus
--   UnitPrice   - uhiku hind (NB: mitteliidetav eraldi, aga siin informatiivne)
--   TotalAmount - rida kogusumma (Quantity * UnitPrice)
-- ------------------------------------------------------------

CREATE TABLE FactSales (
    SaleID      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    DateKey     INT REFERENCES DimDate(DateKey),
    StoreKey    INT REFERENCES DimStore(StoreKey),
    ProductKey  INT REFERENCES DimProduct(ProductKey),
    CustomerKey INT REFERENCES DimCustomer(CustomerKey),
    PaymentKey  INT REFERENCES DimPayment(PaymentKey),
    Quantity    INT NOT NULL,
    UnitPrice   NUMERIC(10,2),
    TotalAmount NUMERIC(10,2) NOT NULL
);
