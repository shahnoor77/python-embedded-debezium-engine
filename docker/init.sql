-- PostgreSQL Initialization Script for Debezium CDC Setup
-- Sample file if you are using docker-containers for databases

-- 1. Ensure the user has replication privileges
ALTER USER source_user WITH REPLICATION;

-- 2. Create the publication (needed for pgoutput plugin)
-- The 'FOR ALL TABLES' option ensures all current and future tables in the schema are included.
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- 3. Create the logical replication slot manually
-- This must match 'slot_name' and 'plugin_name' in config.yaml
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- 4. Create Tables and Insert Initial Data

-- Table 1: Products
CREATE TABLE IF NOT EXISTS public.products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

INSERT INTO public.products (sku, name, price) VALUES 
('LPT-001', 'Laptop Pro 2025', 1899.99),
('ACC-005', 'Wireless Mouse', 35.50),
('ACC-010', 'Mechanical Keyboard', 120.00)
ON CONFLICT (id) DO NOTHING;

-- Table 2: Customers
CREATE TABLE IF NOT EXISTS public.customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    registered_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

INSERT INTO public.customers (first_name, last_name, email) VALUES 
('Alice', 'Smith', 'alice.smith@example.com'),
('Bob', 'Jones', 'bob.jones@example.com'),
('Charlie', 'Brown', 'charlie.brown@example.com')
ON CONFLICT (id) DO NOTHING;

-- Table 3: Orders (Foreign key to Customers and Products)
CREATE TABLE IF NOT EXISTS public.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES public.customers(id),
    order_date TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    total_amount NUMERIC(10, 2) NOT NULL
);

INSERT INTO public.orders (customer_id, order_date, total_amount) VALUES 
(1, NOW() - INTERVAL '2 days', 1899.99), -- Alice bought the Laptop
(2, NOW() - INTERVAL '1 day', 155.50)    -- Bob bought the Mouse and Keyboard
ON CONFLICT (id) DO NOTHING;