

CREATE TABLE IF NOT EXISTS foncier.loyers_communes (
    id_zone         integer,
    insee_c         varchar(5)  NOT NULL,
    libgeo          text        NOT NULL,
    epci            varchar(20),
    dep             varchar(3),
    reg             varchar(3),
    loypredm2       numeric,
    lwr_ipm2        numeric,
    upr_ipm2        numeric,
    typpred         text,          -- 'commune' / 'maille'
    nbobs_com       integer,
    nbobs_mail      integer,
    r2_adj          numeric,
    type_bien       text    NOT NULL,  -- 'appartement', 'maison', ...
    segment_surface text    NOT NULL,  -- 'all', '1-2_pieces', '3_plus_pieces', ...
    annee           integer NOT NULL,
    -- clé technique auto si besoin
    id              bigserial PRIMARY KEY
);

ALTER TABLE foncier.loyers_communes
    ADD CONSTRAINT ux_loyers_communes_unique
    UNIQUE (annee, insee_c, type_bien, segment_surface, typpred);

-- Filtrage par année
CREATE INDEX IF NOT EXISTS idx_loyers_communes_annee
    ON foncier.loyers_communes (annee);

-- Filtrage par commune (INSEE)
CREATE INDEX IF NOT EXISTS idx_loyers_communes_insee
    ON foncier.loyers_communes (insee_c);

-- Filtrage par type de bien
CREATE INDEX IF NOT EXISTS idx_loyers_communes_type_bien
    ON foncier.loyers_communes (type_bien);

-- Filtrage par segment de surface
CREATE INDEX IF NOT EXISTS idx_loyers_communes_segment_surface
    ON foncier.loyers_communes (segment_surface);

-- Combinaisons fréquentes : année + type_bien + segment_surface
CREATE INDEX IF NOT EXISTS idx_loyers_communes_annee_type_surface
    ON foncier.loyers_communes (annee, type_bien, segment_surface);

-- Si tu interroges souvent par EPCI
CREATE INDEX IF NOT EXISTS idx_loyers_communes_epci
    ON foncier.loyers_communes (epci);

-- Si tu fais beaucoup de recherches textuelles sur nom de commune (libgeo),
-- un simple index btree aide pour les égalités :
CREATE INDEX IF NOT EXISTS idx_loyers_communes_libgeo
    ON foncier.loyers_communes (libgeo);