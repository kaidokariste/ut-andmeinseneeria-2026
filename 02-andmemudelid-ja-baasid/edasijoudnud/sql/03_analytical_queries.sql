-- ============================================================
-- 03_analytical_queries.sql
-- Analyutilised paringud star schema pealt
--
-- Demonstreerib star schema eeliseid:
-- - Lihtsad JOIN-id faktist dimensioonidesse
-- - Kiired agregatsioonid
-- - Aknafunktsioonid ja edetabelid
-- ============================================================

-- ------------------------------------------------------------
-- 1. Paevatase muuk poodide kaupa
-- Naitab: JOIN DimDate + DimStore, GROUP BY, ORDER BY
-- ------------------------------------------------------------
SELECT
    d.FullDate,
    d.DayOfWeek,
    s.StoreName,
    s.City,
    COUNT(f.SaleID)    AS tehinguid,
    SUM(f.Quantity)     AS kogus_kokku,
    SUM(f.TotalAmount)  AS muuk_kokku
FROM FactSales f
JOIN DimDate  d ON f.DateKey  = d.DateKey
JOIN DimStore s ON f.StoreKey = s.StoreKey
GROUP BY d.FullDate, d.DayOfWeek, s.StoreName, s.City
ORDER BY d.FullDate, s.StoreName;

-- ------------------------------------------------------------
-- 2. Muuk kategooriate kaupa koos keskmise tehingusummaga
-- Naitab: JOIN DimProduct, AVG agregatsioon
-- ------------------------------------------------------------
SELECT
    p.Category,
    COUNT(f.SaleID)             AS tehinguid,
    SUM(f.Quantity)              AS kogus_kokku,
    SUM(f.TotalAmount)           AS muuk_kokku,
    ROUND(AVG(f.TotalAmount), 2) AS keskmine_tehingu_summa
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.Category
ORDER BY muuk_kokku DESC;

-- ------------------------------------------------------------
-- 3. Top 10 toodet muugi jargi
-- Naitab: LIMIT, mitmene dimensiooni atribuut
-- ------------------------------------------------------------
SELECT
    p.ProductName,
    p.Category,
    p.Brand,
    SUM(f.Quantity)    AS kogus_muudud,
    SUM(f.TotalAmount) AS tulu_kokku
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName, p.Category, p.Brand
ORDER BY tulu_kokku DESC
LIMIT 10;

-- ------------------------------------------------------------
-- 4. Muuk makseviisi kaupa koos osakaaluga
-- Naitab: alampaering protsentuaalse osakaalu jaoks
-- ------------------------------------------------------------
SELECT
    pm.PaymentType,
    COUNT(f.SaleID)    AS tehinguid,
    SUM(f.TotalAmount) AS muuk_kokku,
    ROUND(100.0 * SUM(f.TotalAmount) /
        (SELECT SUM(TotalAmount) FROM FactSales), 1) AS osakaal_pct
FROM FactSales f
JOIN DimPayment pm ON f.PaymentKey = pm.PaymentKey
GROUP BY pm.PaymentType
ORDER BY muuk_kokku DESC;

-- ------------------------------------------------------------
-- 5. Kliendisegmendi analuus
-- Naitab: JOIN DimCustomer, segment-tasemel agregatsioonid
-- ------------------------------------------------------------
SELECT
    c.Segment,
    COUNT(DISTINCT c.CustomerKey) AS kliente,
    COUNT(f.SaleID)               AS tehinguid,
    SUM(f.TotalAmount)            AS muuk_kokku,
    ROUND(AVG(f.TotalAmount), 2)  AS keskmine_tehingu_summa,
    ROUND(SUM(f.TotalAmount) / COUNT(DISTINCT c.CustomerKey), 2) AS muuk_kliendi_kohta
FROM FactSales f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
GROUP BY c.Segment
ORDER BY muuk_kokku DESC;

-- ------------------------------------------------------------
-- 6. Muuk regioonide kaupa aknafunktsiooniga (jooksev summa)
-- Naitab: WINDOW funktsioon, SUM() OVER, ORDER BY kuupaeva jargi
-- ------------------------------------------------------------
SELECT
    d.FullDate,
    s.Region,
    SUM(f.TotalAmount) AS paeva_muuk,
    SUM(SUM(f.TotalAmount)) OVER (
        PARTITION BY s.Region
        ORDER BY d.FullDate
    ) AS jooksev_summa
FROM FactSales f
JOIN DimDate  d ON f.DateKey  = d.DateKey
JOIN DimStore s ON f.StoreKey = s.StoreKey
GROUP BY d.FullDate, s.Region
ORDER BY s.Region, d.FullDate;

-- ------------------------------------------------------------
-- 7. Toodete edetabel kategooria sees (RANK aknafunktsioon)
-- Naitab: RANK() OVER (PARTITION BY ... ORDER BY ...)
-- ------------------------------------------------------------
SELECT
    p.Category,
    p.ProductName,
    SUM(f.TotalAmount) AS tulu_kokku,
    RANK() OVER (
        PARTITION BY p.Category
        ORDER BY SUM(f.TotalAmount) DESC
    ) AS koht_kategoorias
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.Category, p.ProductName
ORDER BY p.Category, koht_kategoorias;
