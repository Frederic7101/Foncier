-- Migration : ajout des champs parsés depuis desc_courte dans licitor_brut
-- Exécuter : psql -U postgres -d foncier -f alter_licitor_brut_add_parsed_fields.sql
-- Puis lancer : python scripts/import/enrich_licitor_brut.py

ALTER TABLE foncier.licitor_brut
    ADD COLUMN IF NOT EXISTS type_local    VARCHAR(30),
    ADD COLUMN IF NOT EXISTS surf_bati     NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS surf_non_bati NUMERIC(10,2);

-- Index sur type_local pour les requêtes de filtrage
CREATE INDEX IF NOT EXISTS idx_licitor_brut_type_local
    ON foncier.licitor_brut (type_local);

-- Valeurs possibles pour type_local :
--   'appartement', 'maison', 'parking', 'dependance',
--   'local_indus_comm', 'terrain', 'immeuble', 'autre'
