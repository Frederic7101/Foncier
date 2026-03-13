CREATE TABLE IF NOT EXISTS foncier.agreg_communes_dvf (
    insee_com      varchar(5)  NOT NULL,  -- code INSEE de la commune
    annee          integer     NOT NULL,  -- année de référence (redondant avec le nom de fichier, mais utile)
    nb_mutations   integer,               -- nombre total de mutations DVF
    nb_maisons     integer,               -- nombre de mutations de maisons
    nb_apparts     integer,               -- nombre de mutations d'appartements
    prop_maison    numeric,               -- pourcentage (0-100) de maisons
    prop_appart    numeric,               -- pourcentage (0-100) d'appartements
    prix_moyen     numeric,               -- prix moyen (en euros)
    prixm2_moyen   numeric,               -- prix moyen au m²
    surface_moy    numeric,               -- surface moyenne (en m²)
    CONSTRAINT pk_agreg_communes_dvf PRIMARY KEY (insee_com, annee)
);

-- Index pour filtrer rapidement par année
CREATE INDEX IF NOT EXISTS idx_agreg_communes_dvf_annee
    ON foncier.agreg_communes_dvf (annee);