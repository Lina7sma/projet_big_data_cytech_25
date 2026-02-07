-- ==========================================================
-- ARCHITECTURE DU DATA WAREHOUSE (MODÈLE EN FLOCON)
-- Ce script crée la structure pour l'analyse décisionnelle.
-- ==========================================================

DROP TABLE IF EXISTS
    fact_trips,
    dim_payment,
    dim_rate_code,
    dim_vendor,
    dim_zone
    CASCADE;

-- 1. Table de Dimension : Vendeurs
-- Contient les noms des entreprises de taxi pour éviter la redondance.
CREATE TABLE dim_vendor (
                            vendor_id INT PRIMARY KEY,      -- Identifiant unique du vendeur
                            vendor_name VARCHAR(100)        -- Nom complet de l'entreprise
);

-- 2. Table de Dimension : Types de Paiement
-- Permet de traduire les codes chiffrés en libellés lisibles (ex: 1 -> Credit card).
CREATE TABLE dim_payment (
                             payment_type_id INT PRIMARY KEY, -- Identifiant du type de paiement
                             payment_name VARCHAR(50)         -- Description (Cash, Credit card, etc.)
);

-- 3. Table de Dimension : Codes Tarifaires
-- Répertorie les différents tarifs appliqués (Standard, JFK, etc.).
CREATE TABLE dim_rate_code (
                               rate_code_id INT PRIMARY KEY,    -- Identifiant du tarif
                               rate_name VARCHAR(50)            -- Description du tarif
);

-- 4. Table de Dimension : Zones de Taxi (récupéré via .csv)
CREATE TABLE dim_zone (
                          location_id INT PRIMARY KEY,
                          borough VARCHAR(50),     -- Le grand district (ex: Manhattan, Brooklyn)
                          zone VARCHAR(100),        -- Le quartier précis (ex: Central Park, JFK Airport)
                          service_zone VARCHAR(50)
);

-- 5.
-- ==========================================================
-- TABLE DE FAIT : fact_trips (Version Analyse Exhaustive)
-- Cette table centralise toutes les mesures de performance.
-- ==========================================================

CREATE TABLE fact_trips (
                            trip_id SERIAL PRIMARY KEY,
                            trip_month INT,
                            pickup_datetime TIMESTAMP,
                            dropoff_datetime TIMESTAMP,
                            total_amount DOUBLE PRECISION,
                            passenger_count INT,
                            trip_distance DOUBLE PRECISION,
                            vendor_id INT,
                            pickup_location_id INT,
                            dropoff_location_id INT,
                            fare_amount DOUBLE PRECISION,
                            tip_amount DOUBLE PRECISION,
                            rate_code_id INT,
                            payment_type_id INT,
                            tolls_amount DOUBLE PRECISION,
                            extra DOUBLE PRECISION,
                            mta_tax DOUBLE PRECISION,
                            improvement_surcharge DOUBLE PRECISION,
                            congestion_surcharge DOUBLE PRECISION,
                            airport_fee DOUBLE PRECISION
);