DROP TABLE IF EXISTS foncier.rpls_logements;

CREATE TABLE IF NOT EXISTS foncier.rpls_logements (
    id                      BIGSERIAL PRIMARY KEY,
    annee_rpls              INTEGER NOT NULL,          	-- ex: 2024 (dérivée du nom de fichier)
    reg_code				VARCHAR(3),					-- code région
	dep_code				VARCHAR(3),					-- code départemement
	epci_code               VARCHAR(20),                -- code EPCI
	code_postal             VARCHAR(10),
	commune                 TEXT,                      	-- LIBCOM
	num_voie                VARCHAR(20),
    type_voie               VARCHAR(50),
    nom_voie                TEXT,
    etage                   VARCHAR(20),
	surface_habitable_m2    NUMERIC(7,2),
    nombre_pieces           SMALLINT,
    annee_construction      SMALLINT,
    annee_premiere_location SMALLINT
);

-- Index géographiques
CREATE INDEX IF NOT EXISTS idx_rpls_logements_commune
    ON foncier.rpls_logements (commune);

CREATE INDEX IF NOT EXISTS idx_rpls_logements_code_postal
    ON foncier.rpls_logements (code_postal);

CREATE INDEX IF NOT EXISTS idx_rpls_logements_epci_code
    ON foncier.rpls_logements (epci_code);

CREATE INDEX IF NOT EXISTS idx_rpls_logements_dep_code
    ON foncier.rpls_logements (dep_code);

CREATE INDEX IF NOT EXISTS idx_rpls_logements_reg_code
    ON foncier.rpls_logements (reg_code);

-- Index temporels
CREATE INDEX IF NOT EXISTS idx_rpls_logements_annee_construction
    ON foncier.rpls_logements (annee_construction);

CREATE INDEX IF NOT EXISTS idx_rpls_logements_annee_premiere_location
    ON foncier.rpls_logements (annee_premiere_location);

CREATE INDEX IF NOT EXISTS idx_rpls_logements_annee_rpls
    ON foncier.rpls_logements (annee_rpls);

-- Pour stats par commune / année
CREATE INDEX IF NOT EXISTS idx_rpls_logements_commune_annee
    ON foncier.rpls_logements (commune, annee_rpls);