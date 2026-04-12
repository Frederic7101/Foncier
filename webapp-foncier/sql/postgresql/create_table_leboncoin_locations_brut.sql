-- Table de stockage brut des annonces de location scrappées depuis Leboncoin
-- Une ligne par annonce, dédupliquée par url_annonce

CREATE TABLE IF NOT EXISTS foncier.leboncoin_locations_brut (
    id                  SERIAL PRIMARY KEY,
    url_annonce         TEXT NOT NULL,
    ref_leboncoin       TEXT,                       -- identifiant interne LBC
    type_bien           TEXT,                       -- Appartement, Maison, Parking, Local, etc.
    titre               TEXT,
    commune             TEXT NOT NULL,
    code_postal         TEXT NOT NULL,
    code_dept           TEXT,
    nb_pieces           INTEGER,
    nb_chambres         INTEGER,
    surface             NUMERIC(10,2),              -- surface en m²
    loyer               INTEGER,                    -- loyer mensuel en euros
    loyer_hc            INTEGER,                    -- loyer hors charges (si disponible)
    charges             INTEGER,                    -- montant des charges (si disponible)
    charges_incluses    BOOLEAN,                    -- true si loyer = charges comprises
    meuble              BOOLEAN,                    -- true si meublé
    dpe_classe          TEXT,                       -- classe DPE (A-G)
    dpe_valeur          INTEGER,                    -- valeur DPE en kWh/m²/an
    ges_classe          TEXT,                       -- classe GES (A-G)
    ges_valeur          INTEGER,                    -- valeur GES en kgCO2/m²/an
    annonceur_type      TEXT,                       -- 'pro' ou 'particulier'
    annonceur_nom       TEXT,
    lien_source         TEXT,                       -- URL canonique de l'annonce
    description         TEXT,
    date_publication    DATE,
    date_scraping       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT leboncoin_loc_url_unique UNIQUE (url_annonce)
);

CREATE INDEX IF NOT EXISTS idx_lbc_loc_commune
    ON foncier.leboncoin_locations_brut (code_dept, commune);

CREATE INDEX IF NOT EXISTS idx_lbc_loc_code_postal
    ON foncier.leboncoin_locations_brut (code_postal);

CREATE INDEX IF NOT EXISTS idx_lbc_loc_date_pub
    ON foncier.leboncoin_locations_brut (date_publication);

CREATE INDEX IF NOT EXISTS idx_lbc_loc_type_bien
    ON foncier.leboncoin_locations_brut (type_bien);
