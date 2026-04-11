-- Ajout des champs issus du scraping de la page détail Licitor.fr
-- Exécuter après alter_licitor_brut_add_parsed_fields.sql

ALTER TABLE foncier.licitor_brut
    ADD COLUMN IF NOT EXISTS date_vente          DATE,
    ADD COLUMN IF NOT EXISTS description_longue   TEXT,
    ADD COLUMN IF NOT EXISTS adresse              TEXT,
    ADD COLUMN IF NOT EXISTS mise_a_prix          INTEGER,
    ADD COLUMN IF NOT EXISTS tribunal             TEXT,
    ADD COLUMN IF NOT EXISTS statut_occupation    VARCHAR(30),
    ADD COLUMN IF NOT EXISTS avocat_nom           TEXT,
    ADD COLUMN IF NOT EXISTS avocat_tel           VARCHAR(30),
    ADD COLUMN IF NOT EXISTS avocat_adresse       TEXT,
    ADD COLUMN IF NOT EXISTS has_cave             BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_parking_dep      BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_jardin           BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_balcon           BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_terrasse         BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_garage           BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_licitor_brut_date_vente
    ON foncier.licitor_brut (date_vente);
