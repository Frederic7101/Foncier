DROP TABLE IF EXISTS foncier.fiscalite_locale;

CREATE TABLE IF NOT EXISTS foncier.fiscalite_locale (
    id                  BIGSERIAL PRIMARY KEY,
    code_reg            VARCHAR(3)  NOT NULL,
    code_dep            VARCHAR(3)  NOT NULL,
    code_insee          VARCHAR(5)  NOT NULL,
    annee               INTEGER     NOT NULL,
    commune_majuscule   TEXT,
    Taux_Global_TFNB    NUMERIC(8,4),
    Taux_Global_TFB     NUMERIC(8,4),
    Taux_TEOM           NUMERIC(8,4),
    Taux_Global_TH      NUMERIC(8,4)
);

-- Clé d'unicité (REG, DEP, INSEE COM, EXERCICE)
ALTER TABLE foncier.fiscalite_locale
    ADD CONSTRAINT ux_fiscalite_locale_geo_annee
    UNIQUE (code_reg, code_dep, code_insee, annee);

-- Index pour les requêtes courantes
CREATE INDEX IF NOT EXISTS idx_fiscalite_locale_insee_annee
    ON foncier.fiscalite_locale (code_insee, annee);

CREATE INDEX IF NOT EXISTS idx_fiscalite_locale_dep_annee
    ON foncier.fiscalite_locale (code_dep, annee);

CREATE INDEX IF NOT EXISTS idx_fiscalite_locale_reg_annee
    ON foncier.fiscalite_locale (code_reg, annee);