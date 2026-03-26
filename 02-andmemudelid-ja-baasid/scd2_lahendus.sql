-- ============================================================
-- 04_scd2.sql
-- Ülesanne 1: SCD Type 2 implementeerimine
-- ============================================================
--
-- MIS ON SCD TYPE 2?
--
-- Slowly Changing Dimension (SCD) on dimensionaalmudeli muster,
-- mis kirjeldab, kuidas käsitleda dimensiooniväärtuste muutumist ajas.
--
-- Tüübid:
--   Type 0: Ei uuenda. Algne väärtus jääb.
--   Type 1: Uuenda otse. Ajalugu kaob.
--   Type 2: Lisa uus rida. Ajalugu säilitatakse.
--   Type 3: Lisa uus veerg (nt OldCity, NewCity). Piiratud ajalugu.
--
-- Type 2 on kõige levinum andmeladudes, sest see säilitab täieliku ajaloo.
-- Iga muudatus loob uue rea koos ValidFrom/ValidTo väärtustega.
--
-- ============================================================

-- ============================================================
-- STSENAARIUM: Alice Smith kolib Tallinnast Tartusse.
-- ============================================================

-- Kontrolli praegust seisu:
SELECT * FROM DimCustomer ORDER BY CustomerKey;

-- ============================================================
-- SAMM 1: Lisa SCD2 väljad DimCustomer tabelisse
-- ============================================================
-- Praegu on DimCustomer lihtne dimensioon ilma ajalooväljadeta.
-- Lisa ValidFrom, ValidTo ja IsCurrent väljad.

-- TEIE LAHENDUS SIIA:
-- ALTER TABLE DimCustomer ADD COLUMN ValidFrom DATE ...
-- ALTER TABLE DimCustomer ADD COLUMN ValidTo DATE ...

-- ============================================================
-- SAMM 2: Sea olemasolevate kirjete vaikimisi väärtused
-- ============================================================
-- Kõik praegused kliendid on aktiivsed alates 2025-01-01.

-- TEIE LAHENDUS SIIA:
UPDATE DimCustomer SET valid_from = '2025-01-01', valid_to = '9999-12-31';

-- Kontrolli vahetulemust:
SELECT * FROM DimCustomer ORDER BY CustomerKey;

-- ============================================================
-- SAMM 3: Sulge Alice'i vana kirje
-- ============================================================
-- Alice'i praegune kirje (Tallinn) tuleb sulgeda:
-- sea ValidTo = CURRENT_DATE - INTERVAL '1 day'
--
-- Vihjed:
--   - Alice'i CustomerID on 1
--   - Aktiivne kirje: ValidTo = '9999-12-31'
--   - Kasuta WHERE tingimust, mis tabab ainult aktiivset kirjet

-- TEIE LAHENDUS SIIA:
UPDATE DimCustomer SET valid_to = CURRENT_DATE - INTERVAL '1 day' WHERE CustomerID = 1;

-- ============================================================
-- SAMM 4: Lisa uus kirje Alice'ile uue aadressiga
-- ============================================================
-- Uus rida:
--   - Uus CustomerKey (surrogate key!) — järgmine vaba võti
--   - Sama CustomerID = 1 (äriline ID jääb samaks)
--   - Uus linn: Tartu
--   - ValidFrom = CURRENT_DATE
--   - ValidTo = '9999-12-31' (aktiivne)
--
-- Miks uus CustomerKey? Sest surrogate key identifitseerib
-- konkreetset VERSIOONI kliendist, mitte klienti ennast.

-- TEIE LAHENDUS SIIA:
INSERT INTO DimCustomer (customerid, firstname, lastname, segment, city, valid_from, valid_to) VALUES (1,'Alice','Smith','Regular','Tallinn',CURRENT_DATE, '9999-12-31');

-- ============================================================
-- SAMM 5: Kontrolli tulemust
-- ============================================================

-- Kõik kliendid:
SELECT * FROM DimCustomer ORDER BY CustomerID, valid_from;

-- Alice'i ajalugu:
SELECT
    CustomerKey,
    CustomerID,
    FirstName || ' ' || LastName AS nimi,
    City,
    valid_from,
    valid_to,
    CASE WHEN valid_to = '9999-12-31' THEN 'Aktiivne' ELSE 'Ajalugu' END AS staatus
FROM DimCustomer
WHERE CustomerID = 1
ORDER BY valid_to;

-- ============================================================
-- SAMM 6: Lisa müügitehing Alice'i uuele kirjele (Tartu)
-- ============================================================
-- Lisa uus FactSales rida, mis viitab Alice'i uuele CustomerKey-le.
-- See näitab, et ajaloolised müügid jäävad vana CustomerKey juurde
-- ja uued müügid lähevad uuele.

-- TEIE LAHENDUS SIIA:
INSERT
INTO factsales (datekey, storekey, productkey, customerkey, paymentkey, quantity, unitprice, totalamount)
VALUES         (20260922, 3,17,21,1, 3, 17.17, 34);
select * from FactSales where customerkey = 21;
-- ============================================================
-- SAMM 7: Päring — müük linnade kaupa (läbi ajaloo)
-- ============================================================
-- Kirjuta päring, mis näitab Alice'i müüke linnade kaupa.
-- Peaksid nägema nii Tallinna kui Tartu müüke.

-- TEIE LAHENDUS SIIA:
SELECT city, SUM(unitprice)
FROM factsales f
         JOIN dimcustomer c ON f.customerkey = c.customerkey
WHERE c.customerid = 1
GROUP BY city;

-- ============================================================
-- KONTROLLKÜSIMUSED
-- ============================================================
--
-- 1. Miks on Alice'il uus CustomerKey, mitte endiselt sama?
-- 2. Mis juhtub, kui pärid FactSales JOIN DimCustomer
--    WHERE FirstName = 'Alice' ja EI filtreeri ValidTo järgi?
-- 3. Miks kasutame surrogate key'd (CustomerKey) ja mitte
--    business key'd (CustomerID) FactSales tabelis?
-- 4. Kuidas käituks Type 1 selle stsenaariumi korral?
--    Mis läheks kaduma?