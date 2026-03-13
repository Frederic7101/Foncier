CREATE TABLE IF NOT EXISTS foncier.aides_logement_part (
    id              BIGSERIAL PRIMARY KEY,
    codegeo         VARCHAR(10) NOT NULL,
    libgeo          TEXT        NOT NULL,
    an              INTEGER     NOT NULL,
    perc_apl        NUMERIC(6,3),   -- part de ménages avec APL
    perc_aide_logt  NUMERIC(6,3)    -- part de ménages avec autre aide logement
);

-- Clé d'unicité commune aux 2 fichiers
ALTER TABLE foncier.aides_logement_part
    ADD CONSTRAINT ux_aides_logement_part_geo_an
    UNIQUE (codegeo, libgeo, an);

-- Index pour les requêtes classiques
CREATE INDEX IF NOT EXISTS idx_aides_logement_part_geo
    ON foncier.aides_logement_part (codegeo, an);

CREATE INDEX IF NOT EXISTS idx_aides_logement_part_libgeo
    ON foncier.aides_logement_part (libgeo);

CREATE INDEX IF NOT EXISTS idx_aides_logement_part_annee
    ON foncier.aides_logement_part (an);

