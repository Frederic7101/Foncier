-- Table de stockage brut des annonces de vente scrappées depuis Castorus
-- Une ligne par annonce, dédupliquée par url_annonce

CREATE TABLE IF NOT EXISTS foncier.castorus_brut (
    id              SERIAL PRIMARY KEY,
    url_annonce     TEXT NOT NULL,
    ref_castorus    TEXT,                       -- identifiant interne Castorus (ex: ref105718341)
    type_bien       TEXT,                       -- Appartement, Maison, Terrain, Commerce, etc.
    titre           TEXT,                       -- titre complet de l'annonce
    commune         TEXT NOT NULL,
    code_postal     TEXT NOT NULL,
    code_dept       TEXT,
    nb_pieces       INTEGER,
    surface         NUMERIC(10,2),              -- surface en m²
    prix            INTEGER,                    -- prix demandé en euros
    prix_m2         NUMERIC(10,2),              -- prix au m²
    rendement       NUMERIC(5,2),               -- rendement locatif estimé (%)
    date_publication DATE,                      -- date "Vu le" (date de publication/dernière activité)
    agence          TEXT,                        -- nom de l'agence (depuis page détail)
    lien_source     TEXT,                        -- lien vers l'annonce originale
    description     TEXT,                        -- description (depuis page détail)
    date_scraping   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT castorus_brut_url_unique UNIQUE (url_annonce)
);

CREATE INDEX IF NOT EXISTS idx_castorus_brut_commune
    ON foncier.castorus_brut (code_dept, commune);

CREATE INDEX IF NOT EXISTS idx_castorus_brut_code_postal
    ON foncier.castorus_brut (code_postal);

CREATE INDEX IF NOT EXISTS idx_castorus_brut_date_pub
    ON foncier.castorus_brut (date_publication);

CREATE INDEX IF NOT EXISTS idx_castorus_brut_type_bien
    ON foncier.castorus_brut (type_bien);
