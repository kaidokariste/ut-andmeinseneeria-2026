-- ============================================================
-- 02_load_data.sql
-- Andmete laadimine denormaliseeritud OLTP allikast star schemasse
--
-- Allikas: source_sales.csv (lokaalne koopia)
-- Alternatiiv: asenda '/data/source_sales.csv' URL-iga:
--   'https://raw.githubusercontent.com/REPO/main/.../source_sales.csv'
-- ============================================================

-- Idempotentsus: tuhjendame tabelid enne laadimist
TRUNCATE TABLE FactSales RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimDate RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimStore RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimProduct RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimCustomer RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimPayment RESTART IDENTITY CASCADE;

-- ------------------------------------------------------------
-- CSV andmete laadimine vahemälutabelisse
-- DuckDB loeb CSV faili ja loob tabeli, PostgreSQL kasutab tulemust
-- ------------------------------------------------------------

DROP TABLE IF EXISTS _csv_source;
CREATE TABLE _csv_source AS
SELECT * FROM read_csv('/data/source_sales.csv', header := true);

-- ------------------------------------------------------------
-- 1. Laadi dimensioonid allikast (DISTINCT vaartused)
-- ------------------------------------------------------------

-- DimDate: genereerime kogu 2025-2026 kuupaevad (generate_series)
INSERT INTO DimDate (DateKey, FullDate, Year, Quarter, Month, Day, DayOfWeek, DayOfYear, WeekOfYear, MonthName, IsWeekend)
SELECT
    to_char(d, 'YYYYMMDD')::INT,
    d::DATE,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    EXTRACT(DAY FROM d)::INT,
    to_char(d, 'Day'),
    EXTRACT(DOY FROM d)::INT,
    EXTRACT(WEEK FROM d)::INT,
    to_char(d, 'Month'),
    EXTRACT(ISODOW FROM d)::INT IN (6, 7)
FROM generate_series('2025-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d;

-- DimStore
INSERT INTO DimStore (StoreName, City, Region)
SELECT DISTINCT
    store_name,
    store_city,
    store_region
FROM _csv_source
ORDER BY store_name;

-- DimProduct
INSERT INTO DimProduct (ProductName, Category, Brand)
SELECT DISTINCT
    product_name,
    product_category,
    product_brand
FROM _csv_source
ORDER BY product_category, product_name;

-- DimCustomer (ilma SCD2 valjadeta)
INSERT INTO DimCustomer (CustomerID, FirstName, LastName, Segment, City)
SELECT DISTINCT
    customer_id,
    split_part(customer_name, ' ', 1),
    split_part(customer_name, ' ', 2),
    customer_segment,
    customer_city
FROM _csv_source
ORDER BY customer_id;

-- DimPayment
INSERT INTO DimPayment (PaymentType)
SELECT DISTINCT
    payment_type
FROM _csv_source
ORDER BY payment_type;

-- ------------------------------------------------------------
-- 2. Laadi faktitabel (viited dimensioonidesse JOIN kaudu)
-- ------------------------------------------------------------

INSERT INTO FactSales (DateKey, StoreKey, ProductKey, CustomerKey, PaymentKey, Quantity, UnitPrice, TotalAmount)
SELECT
    d.DateKey,
    s.StoreKey,
    p.ProductKey,
    c.CustomerKey,
    pm.PaymentKey,
    src.quantity::INT,
    src.unit_price::NUMERIC(10,2),
    src.total_amount::NUMERIC(10,2)
FROM _csv_source src
JOIN DimDate     d  ON src.order_date::DATE = d.FullDate
JOIN DimStore    s  ON src.store_name       = s.StoreName
JOIN DimProduct  p  ON src.product_name     = p.ProductName
JOIN DimCustomer c  ON src.customer_id      = c.CustomerID
JOIN DimPayment  pm ON src.payment_type     = pm.PaymentType;

-- ------------------------------------------------------------
-- 3. Koristus ja kontroll
-- ------------------------------------------------------------

DROP TABLE IF EXISTS _csv_source;

SELECT 'DimDate' AS tabel, COUNT(*) AS ridu FROM DimDate
UNION ALL SELECT 'DimStore', COUNT(*) FROM DimStore
UNION ALL SELECT 'DimProduct', COUNT(*) FROM DimProduct
UNION ALL SELECT 'DimCustomer', COUNT(*) FROM DimCustomer
UNION ALL SELECT 'DimPayment', COUNT(*) FROM DimPayment
UNION ALL SELECT 'FactSales', COUNT(*) FROM FactSales
ORDER BY tabel;
