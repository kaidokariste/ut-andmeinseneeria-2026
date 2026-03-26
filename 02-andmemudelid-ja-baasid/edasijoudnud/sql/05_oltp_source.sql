-- ============================================================
-- 05_oltp_source.sql
-- Normaliseeritud OLTP allikandmebaasi loomine ja taitmein
--
-- See skript loob "oltp" skeemi, kuhu luuakse 11 tabelit,
-- mis moodustavad tuupilise OLTP susteemi (nt kauplus/e-pood).
--
-- Andmed taidetakse samast CSV allikast (source_sales.csv),
-- aga jaotatakse normaliseeritud tabelitesse.
-- ============================================================

-- Idempotentsus: kustutame olemasoleva skeemi koos koigi tabelitega
DROP SCHEMA IF EXISTS oltp CASCADE;
CREATE SCHEMA oltp;

-- ============================================================
-- TOOTEKATALOOG
-- ============================================================

-- Kategooriate klassifikaator
CREATE TABLE oltp.product_category (
    category_id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE
);

-- Tooted (viitab kategooriale)
CREATE TABLE oltp.product (
    product_id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku          VARCHAR(20)  NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    base_price   NUMERIC(10,2) NOT NULL,
    category_id  INTEGER NOT NULL REFERENCES oltp.product_category(category_id),
    brand        VARCHAR(50),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- KLIENDIHALDUS
-- ============================================================

-- Kliendi pohiandmed
CREATE TABLE oltp.customer (
    customer_id    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name     VARCHAR(50) NOT NULL,
    last_name      VARCHAR(50) NOT NULL,
    personal_code  VARCHAR(11) UNIQUE,
    segment        VARCHAR(50),
    registered_at  DATE DEFAULT CURRENT_DATE
);

-- Kliendi aadressid (1:N — arveldus- ja tarneaadress)
CREATE TABLE oltp.customer_address (
    address_id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  INTEGER NOT NULL REFERENCES oltp.customer(customer_id),
    address_type VARCHAR(20) NOT NULL CHECK (address_type IN ('billing', 'shipping')),
    city         VARCHAR(50) NOT NULL,
    street       VARCHAR(200),
    postal_code  VARCHAR(10),
    is_primary   BOOLEAN DEFAULT true
);

-- Kontaktandmed (1:N — e-post, telefon)
CREATE TABLE oltp.contact_info (
    contact_id    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id   INTEGER NOT NULL REFERENCES oltp.customer(customer_id),
    contact_type  VARCHAR(20) NOT NULL CHECK (contact_type IN ('email', 'phone')),
    contact_value VARCHAR(100) NOT NULL
);

-- ============================================================
-- POED JA PERSONAL
-- ============================================================

-- Poe pohiandmed
CREATE TABLE oltp.store (
    store_id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    city       VARCHAR(50),
    region     VARCHAR(50)
);

-- Tootajad (viitab poele)
CREATE TABLE oltp.employee (
    employee_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id    INTEGER NOT NULL REFERENCES oltp.store(store_id),
    first_name  VARCHAR(50) NOT NULL,
    last_name   VARCHAR(50) NOT NULL,
    position    VARCHAR(50),
    hired_at    DATE DEFAULT CURRENT_DATE
);

-- ============================================================
-- MAKSED
-- ============================================================

-- Makseviisi klassifikaator
CREATE TABLE oltp.payment_method (
    payment_method_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    method_name       VARCHAR(30) NOT NULL UNIQUE
);

-- ============================================================
-- MUUK JA TELLIMUSED
-- Andmeaida uks faktirida on OLTP susteemis alati jaotatud
-- vahemalt kaheks tabeliks (pais ja read).
-- SAP-is: VBAK (pais) ja VBAP (read).
-- ============================================================

-- Tellimuse pais (uks rida = uks tellimus)
CREATE TABLE oltp.sales_order_header (
    order_id     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_number VARCHAR(20) NOT NULL UNIQUE,
    order_date   DATE NOT NULL,
    status       VARCHAR(20) DEFAULT 'completed'
                     CHECK (status IN ('pending', 'processing', 'completed', 'cancelled')),
    customer_id  INTEGER NOT NULL REFERENCES oltp.customer(customer_id),
    store_id     INTEGER NOT NULL REFERENCES oltp.store(store_id),
    employee_id  INTEGER REFERENCES oltp.employee(employee_id),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tellimuse read (uks rida = uks toode tellimuses)
CREATE TABLE oltp.sales_order_detail (
    order_line_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id      INTEGER NOT NULL REFERENCES oltp.sales_order_header(order_id),
    product_id    INTEGER NOT NULL REFERENCES oltp.product(product_id),
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(10,2) NOT NULL,
    line_discount NUMERIC(10,2) DEFAULT 0.00,
    line_total    NUMERIC(10,2) NOT NULL
);

-- Maksetehingud (1:N — uhel tellimusel voib olla mitu makset)
CREATE TABLE oltp.payment_transaction (
    payment_id        INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id          INTEGER NOT NULL REFERENCES oltp.sales_order_header(order_id),
    payment_method_id INTEGER NOT NULL REFERENCES oltp.payment_method(payment_method_id),
    amount            NUMERIC(10,2) NOT NULL,
    paid_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    authorization_code VARCHAR(20)
);

-- ============================================================
-- CSV andmete laadimine vahemälutabelisse
-- DuckDB loeb CSV faili ja loob tabeli, PostgreSQL kasutab tulemust
-- ============================================================

DROP TABLE IF EXISTS _csv_source;
CREATE TABLE _csv_source AS
SELECT * FROM read_csv('/data/source_sales.csv', header := true);

-- ============================================================
-- ANDMETE LAADIMINE CSV-st OLTP tabelitesse
-- ============================================================

-- 1. Tootekategooriad
INSERT INTO oltp.product_category (category_name)
SELECT DISTINCT product_category
FROM _csv_source
ORDER BY product_category;

-- 2. Tooted (koos genereeritud SKU-ga)
INSERT INTO oltp.product (sku, product_name, base_price, category_id, brand)
SELECT
    'SKU-' || LPAD(ROW_NUMBER() OVER (ORDER BY src.product_name)::TEXT, 4, '0'),
    src.product_name,
    src.unit_price::NUMERIC(10,2),
    pc.category_id,
    src.product_brand
FROM (
    SELECT DISTINCT product_name, product_category, product_brand,
           -- Baashind: voetakse esimene CSV-s esinenud hind
           MIN(unit_price) AS unit_price
    FROM _csv_source
    GROUP BY product_name, product_category, product_brand
) src
JOIN oltp.product_category pc ON src.product_category = pc.category_name
ORDER BY src.product_name;

-- 3. Kliendid (koos genereeritud isikukoodiga)
-- NB: sisestame ORDER BY customer_id jargi, nii et IDENTITY vaartused klapivad CSV customer_id-ga
INSERT INTO oltp.customer (first_name, last_name, personal_code, segment, registered_at)
SELECT
    first_name, last_name, personal_code, segment, registered_at
FROM (
    SELECT DISTINCT
        customer_id::INT AS cid,
        split_part(customer_name, ' ', 1) AS first_name,
        split_part(customer_name, ' ', 2) AS last_name,
        -- Lihtsustatud isikukood (mitte parisformaat): 3 + 10 numbrit
        '3' || LPAD((customer_id * 7919 % 10000000000)::BIGINT::TEXT, 10, '0') AS personal_code,
        customer_segment AS segment,
        '2024-01-01'::DATE + (customer_id * 17 % 365)::INT AS registered_at  -- genereeritud registreerimiskuupaev
    FROM _csv_source
) sub
ORDER BY cid;

-- 4. Klientide aadressid (arveldus + tarne)
-- Arveldusaadress (linn CSV-st, genereeritud tanav ja postiindeks)
INSERT INTO oltp.customer_address (customer_id, address_type, city, street, postal_code, is_primary)
SELECT
    c.customer_id,
    'billing',
    src.customer_city,
    (ARRAY['Tamme', 'Pargi', 'Kooli', 'Mere', 'Kesk', 'Vabaduse', 'Pikk', 'Lai'])[1 + c.customer_id % 8]
        || ' ' || (1 + c.customer_id % 50)::TEXT,
    CASE src.customer_city
        WHEN 'Tallinn'  THEN '10'
        WHEN 'Tartu'    THEN '50'
        WHEN 'Parnu'    THEN '80'
        WHEN 'Narva'    THEN '20'
        WHEN 'Viljandi' THEN '71'
        ELSE '90'
    END || LPAD((c.customer_id * 7 % 999)::TEXT, 3, '0'),
    true
FROM (
    SELECT DISTINCT customer_id::INT AS cid, customer_city
    FROM _csv_source
) src
JOIN oltp.customer c ON c.customer_id = src.cid;

-- Tarneaadress (sama linn, teine tanav)
INSERT INTO oltp.customer_address (customer_id, address_type, city, street, postal_code, is_primary)
SELECT
    c.customer_id,
    'shipping',
    src.customer_city,
    (ARRAY['Viru', 'Liiva', 'Metsa', 'Roheline', 'Turu', 'Uus', 'Vana', 'Kalda'])[1 + c.customer_id % 8]
        || ' ' || (10 + c.customer_id % 30)::TEXT,
    CASE src.customer_city
        WHEN 'Tallinn'  THEN '10'
        WHEN 'Tartu'    THEN '50'
        WHEN 'Parnu'    THEN '80'
        WHEN 'Narva'    THEN '20'
        WHEN 'Viljandi' THEN '71'
        ELSE '90'
    END || LPAD((c.customer_id * 13 % 999)::TEXT, 3, '0'),
    false
FROM (
    SELECT DISTINCT customer_id::INT AS cid, customer_city
    FROM _csv_source
) src
JOIN oltp.customer c ON c.customer_id = src.cid;

-- 5. Kontaktandmed (e-post + telefon)
INSERT INTO oltp.contact_info (customer_id, contact_type, contact_value)
SELECT
    customer_id,
    'email',
    LOWER(first_name || '.' || last_name) || '@example.com'
FROM oltp.customer;

INSERT INTO oltp.contact_info (customer_id, contact_type, contact_value)
SELECT
    customer_id,
    'phone',
    '+372 5' || LPAD((customer_id * 1234567 % 10000000)::TEXT, 7, '0')
FROM oltp.customer;

-- 6. Poed
INSERT INTO oltp.store (store_name, city, region)
SELECT DISTINCT store_name, store_city, store_region
FROM _csv_source
ORDER BY store_name;

-- 7. Tootajad (2-3 genereeritud tootajat poe kohta)
INSERT INTO oltp.employee (store_id, first_name, last_name, position, hired_at)
SELECT
    s.store_id,
    fn,
    ln,
    pos,
    '2023-01-15'::DATE + (s.store_id * 30 + e_nr * 45)
FROM oltp.store s
CROSS JOIN (VALUES
    (1, 'Kairi',  'Tamm',     'Juhataja'),
    (2, 'Margus', 'Kask',     'Kassapidaja'),
    (3, 'Liina',  'Lepik',    'Kassapidaja')
) AS emp(e_nr, fn, ln, pos);

-- 8. Makseviisid
INSERT INTO oltp.payment_method (method_name)
SELECT DISTINCT payment_type
FROM _csv_source
ORDER BY payment_type;

-- 9. Tellimuse paised
-- NB: order_id CSV-st kasutame tellimuse numbrina (ORD-XXXXXX)
INSERT INTO oltp.sales_order_header (order_number, order_date, status, customer_id, store_id, employee_id)
SELECT DISTINCT ON (src.order_id)
    'ORD-' || LPAD(src.order_id::TEXT, 6, '0'),
    src.order_date::DATE,
    'completed',
    src.customer_id::INT,
    s.store_id,
    -- Maarame kassapidaja (esimene kassapidaja selles poes)
    (SELECT e.employee_id FROM oltp.employee e
     WHERE e.store_id = s.store_id AND e.position = 'Kassapidaja'
     LIMIT 1)
FROM _csv_source src
JOIN oltp.store s ON src.store_name = s.store_name
ORDER BY src.order_id;

-- 10. Tellimuse read
INSERT INTO oltp.sales_order_detail (order_id, product_id, quantity, unit_price, line_discount, line_total)
SELECT
    oh.order_id,
    p.product_id,
    src.quantity::INT,
    src.unit_price::NUMERIC(10,2),
    0.00,
    src.total_amount::NUMERIC(10,2)
FROM _csv_source src
JOIN oltp.sales_order_header oh ON 'ORD-' || LPAD(src.order_id::TEXT, 6, '0') = oh.order_number
JOIN oltp.product p ON src.product_name = p.product_name;

-- 11. Maksetehingud (uks makse tellimuse kohta)
INSERT INTO oltp.payment_transaction (order_id, payment_method_id, amount, paid_at, authorization_code)
SELECT
    oh.order_id,
    pm.payment_method_id,
    -- Tellimuse kogusumma
    SUM(od.line_total),
    oh.order_date::TIMESTAMP + INTERVAL '1 hour' * (oh.order_id % 12),
    -- Autoriseerimiskood ainult kaardimaksetele
    CASE WHEN pm.method_name IN ('Kaart', 'Kinkekaart')
         THEN UPPER(SUBSTR(MD5(oh.order_id::TEXT || pm.method_name), 1, 8))
         ELSE NULL
    END
FROM oltp.sales_order_header oh
JOIN oltp.sales_order_detail od ON oh.order_id = od.order_id
JOIN (
    -- Leia makseviis CSV-st (esimene rida tellimuse kohta)
    SELECT DISTINCT ON (order_id) order_id, payment_type
    FROM _csv_source
    ORDER BY order_id
) src ON 'ORD-' || LPAD(src.order_id::TEXT, 6, '0') = oh.order_number
JOIN oltp.payment_method pm ON src.payment_type = pm.method_name
GROUP BY oh.order_id, pm.payment_method_id, oh.order_date, pm.method_name;

-- ============================================================
-- KORISTUS JA KONTROLL
-- ============================================================

DROP TABLE IF EXISTS _csv_source;

SELECT 'oltp.product_category'   AS tabel, COUNT(*) AS ridu FROM oltp.product_category
UNION ALL SELECT 'oltp.product',          COUNT(*) FROM oltp.product
UNION ALL SELECT 'oltp.customer',         COUNT(*) FROM oltp.customer
UNION ALL SELECT 'oltp.customer_address', COUNT(*) FROM oltp.customer_address
UNION ALL SELECT 'oltp.contact_info',     COUNT(*) FROM oltp.contact_info
UNION ALL SELECT 'oltp.store',            COUNT(*) FROM oltp.store
UNION ALL SELECT 'oltp.employee',         COUNT(*) FROM oltp.employee
UNION ALL SELECT 'oltp.payment_method',   COUNT(*) FROM oltp.payment_method
UNION ALL SELECT 'oltp.sales_order_header',  COUNT(*) FROM oltp.sales_order_header
UNION ALL SELECT 'oltp.sales_order_detail',  COUNT(*) FROM oltp.sales_order_detail
UNION ALL SELECT 'oltp.payment_transaction', COUNT(*) FROM oltp.payment_transaction
ORDER BY tabel;
