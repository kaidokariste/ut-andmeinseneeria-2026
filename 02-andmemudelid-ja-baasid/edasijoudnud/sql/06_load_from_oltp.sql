-- ============================================================
-- 06_load_from_oltp.sql
-- Andmete laadimine normaliseeritud OLTP allikast star schemasse
--
-- See on alternatiiv failile 02_load_data.sql (CSV-st laadimine).
-- Tulemus on identne, aga allikas on oltp.* skeemi tabelid.
--
-- Opetuspunkt: OLTP-st laadimine nõuab rohkem JOIN-e,
-- sest andmed on jaotatud paljudesse normaliseeritud tabelitesse.
-- ============================================================

-- Eeldus: oltp skeem on juba loodud ja taidetud (05_oltp_source.sql)
-- Eeldus: star schema tabelid on juba loodud (01_create_tables.sql)

-- Idempotentsus: tuhjendame tabelid enne laadimist
TRUNCATE TABLE FactSales RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimDate RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimStore RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimProduct RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimCustomer RESTART IDENTITY CASCADE;
TRUNCATE TABLE DimPayment RESTART IDENTITY CASCADE;

-- ------------------------------------------------------------
-- 1. Laadi dimensioonid OLTP tabelitest
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


-- DimStore: otse oltp.store tabelist
INSERT INTO DimStore (StoreName, City, Region)
SELECT store_name, city, region
FROM oltp.store
ORDER BY store_name;

-- DimProduct: nõuab JOIN-i kategooria tabeliga
-- (OLTP-s on kategooria eraldi tabelis, star schema-s denormaliseeritud)
INSERT INTO DimProduct (ProductName, Category, Brand)
SELECT
    p.product_name,
    pc.category_name,
    p.brand
FROM oltp.product p
JOIN oltp.product_category pc ON p.category_id = pc.category_id
ORDER BY pc.category_name, p.product_name;

-- DimCustomer: nõuab JOIN-i aadressitabeliga
-- (OLTP-s on aadress eraldi tabelis, star schema-s denormaliseeritud)
INSERT INTO DimCustomer (CustomerID, FirstName, LastName, Segment, City)
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.segment,
    ca.city
FROM oltp.customer c
JOIN oltp.customer_address ca ON c.customer_id = ca.customer_id
WHERE ca.address_type = 'billing' AND ca.is_primary = true
ORDER BY c.customer_id;

-- DimPayment: otse oltp.payment_method tabelist
INSERT INTO DimPayment (PaymentType)
SELECT method_name
FROM oltp.payment_method
ORDER BY method_name;

-- ------------------------------------------------------------
-- 2. Laadi faktitabel OLTP tabelitest
--
-- NB: See nõuab JOIN-e labi kogu OLTP skeemi:
--   sales_order_detail -> sales_order_header -> customer -> customer_address
--   sales_order_detail -> product -> product_category
--   sales_order_header -> store
--   payment_transaction -> payment_method
--
-- Pluss dimensioonide mapping (DimDate, DimStore, DimProduct, DimCustomer, DimPayment)
-- Kokku ~14 JOIN-i! Vorreldav CSV variandiga, kus on ainult 5 JOIN-i.
-- ------------------------------------------------------------

INSERT INTO FactSales (DateKey, StoreKey, ProductKey, CustomerKey, PaymentKey,
                       Quantity, UnitPrice, TotalAmount)
SELECT
    -- Dimensioonide surrogate key'd
    dd.DateKey,
    ds.StoreKey,
    dp.ProductKey,
    dc.CustomerKey,
    dpm.PaymentKey,
    -- Moodikud
    od.quantity,
    od.unit_price,
    od.line_total
FROM oltp.sales_order_detail od
-- OLTP tabelite JOIN-id (andmete kogumine)
JOIN oltp.sales_order_header oh  ON od.order_id       = oh.order_id
JOIN oltp.product p              ON od.product_id      = p.product_id
JOIN oltp.product_category pc    ON p.category_id      = pc.category_id
JOIN oltp.customer cust          ON oh.customer_id     = cust.customer_id
JOIN oltp.customer_address ca    ON cust.customer_id   = ca.customer_id
                                AND ca.address_type = 'billing' AND ca.is_primary = true
JOIN oltp.store st               ON oh.store_id        = st.store_id
JOIN oltp.payment_transaction pt ON oh.order_id        = pt.order_id
JOIN oltp.payment_method pm      ON pt.payment_method_id = pm.payment_method_id
-- Dimensioonide mapping (surrogate key otsing)
JOIN DimDate     dd  ON oh.order_date    = dd.FullDate
JOIN DimStore    ds  ON st.store_name    = ds.StoreName
JOIN DimProduct  dp  ON p.product_name   = dp.ProductName
JOIN DimCustomer dc  ON cust.customer_id = dc.CustomerID
JOIN DimPayment  dpm ON pm.method_name   = dpm.PaymentType;

-- ------------------------------------------------------------
-- 3. Kontrolli tulemust
-- Ridade arvud peavad klappima CSV variandiga (02_load_data.sql)
-- ------------------------------------------------------------

SELECT 'DimDate' AS tabel, COUNT(*) AS ridu FROM DimDate
UNION ALL SELECT 'DimStore', COUNT(*) FROM DimStore
UNION ALL SELECT 'DimProduct', COUNT(*) FROM DimProduct
UNION ALL SELECT 'DimCustomer', COUNT(*) FROM DimCustomer
UNION ALL SELECT 'DimPayment', COUNT(*) FROM DimPayment
UNION ALL SELECT 'FactSales', COUNT(*) FROM FactSales
ORDER BY tabel;
