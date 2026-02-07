-- ==========================================================
-- INSERTION DES DONNÉES DE RÉFÉRENCE (DIMENSIONS)
-- Ce script remplit les tables qui servent de dictionnaire.
-- ==========================================================

DROP TABLE IF EXISTS
    trips_dec_raw,
    trips_june_raw
    CASCADE;

-- 1. Remplissage de la dimension Vendeurs
-- On regroupe les vendeurs initiaux et ceux découverts lors de l'ingestion.
INSERT INTO dim_vendor (vendor_id, vendor_name) VALUES
                                                    (1, 'Creative Mobile Technologies'),
                                                    (2, 'VeriFone Inc.'),
                                                    (6, 'Myle Technologies Inc'),
                                                    (7, 'Helix');

-- 2. Remplissage de la dimension Paiements
-- On inclut tous les types de paiement du dictionnaire TLC.
INSERT INTO dim_payment (payment_type_id, payment_name) VALUES
                                                            (0, 'Flex Fare trip'),
                                                            (1, 'Credit card'),
                                                            (2, 'Cash'),
                                                            (3, 'No charge'),
                                                            (4, 'Dispute'),
                                                            (5, 'Unknown'),
                                                            (6, 'Voided trip');

-- 3. Remplissage de la dimension Tarifs (Rate Codes)
-- On inclut les tarifs standards et les codes spéciaux (99).
INSERT INTO dim_rate_code (rate_code_id, rate_name) VALUES
                                                        (1, 'Standard rate'),
                                                        (2, 'JFK'),
                                                        (3, 'Newark'),
                                                        (4, 'Nassau or Westchester'),
                                                        (5, 'Negotiated fare'),
                                                        (6, 'Group ride'),
                                                        (99, 'Unknown');