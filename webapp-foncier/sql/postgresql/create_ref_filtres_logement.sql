-- Références pour les filtres « type de logement », « surface », « nb de pièces » (comparaison_scores, stats).
-- Schéma foncier (conventions projet).

CREATE TABLE IF NOT EXISTS foncier.ref_type_logts (
    code                TEXT PRIMARY KEY,
    libelle             TEXT NOT NULL,
    sort_order          INTEGER NOT NULL DEFAULT 0,
    -- Filtre optionnel sur vf_communes.type_local (ILIKE, NULL = pas de filtre côté requête)
    type_local_pattern  TEXT
);

COMMENT ON TABLE foncier.ref_type_logts IS 'Types de biens affichés en sélecteur ; type_local_pattern pour jointures futures sur vf_communes.type_local';

INSERT INTO foncier.ref_type_logts (code, libelle, sort_order, type_local_pattern) VALUES
    ('TOUS', 'Tous', 0, NULL),
    ('MAISON', 'Maisons', 1, 'Maison'),
    ('APPART', 'Appartements', 2, 'Appartement'),
    ('LOCAL_INDUS', 'Locaux indus. / comm.', 3, '%Local%industriel%'),
    ('PARKING', 'Dépendances', 4, '%Dépendance%'),
    ('TERRAIN', 'Terrains', 5, '%Terrain%'),
    ('IMMEUBLE', 'Immeubles', 6, '%Immeuble%')
ON CONFLICT (code) DO UPDATE SET
    libelle = EXCLUDED.libelle,
    sort_order = EXCLUDED.sort_order,
    type_local_pattern = EXCLUDED.type_local_pattern;

CREATE TABLE IF NOT EXISTS foncier.ref_type_surf (
    code        TEXT PRIMARY KEY,
    libelle     TEXT NOT NULL,
    sort_order  INTEGER NOT NULL DEFAULT 0,
    -- Colonnes vf_communes agrégées (S1..S5) quand filtre actif
    vf_suffix   TEXT
);

COMMENT ON TABLE foncier.ref_type_surf IS 'Tranches de surface ; vf_suffix = S1..S5 pour prix_med_s*, surf_med_s*, etc.';

INSERT INTO foncier.ref_type_surf (code, libelle, sort_order, vf_suffix) VALUES
    ('TOUTES', 'Toutes', 0, NULL),
    ('S1', '<25 m2', 1, 'S1'),
    ('S2', '25 à 35 m2', 2, 'S2'),
    ('S3', '35 à 45 m2', 3, 'S3'),
    ('S4', '45 à 55 m2', 4, 'S4'),
    ('S5', '>55 m2', 5, 'S5')
ON CONFLICT (code) DO UPDATE SET
    libelle = EXCLUDED.libelle,
    sort_order = EXCLUDED.sort_order,
    vf_suffix = EXCLUDED.vf_suffix;

CREATE TABLE IF NOT EXISTS foncier.ref_nb_pieces (
    code        TEXT PRIMARY KEY,
    libelle     TEXT NOT NULL,
    sort_order  INTEGER NOT NULL DEFAULT 0,
    vf_suffix   TEXT
);

COMMENT ON TABLE foncier.ref_nb_pieces IS 'Nombre de pièces (T1..T5) ; vf_suffix pour colonnes vf_communes';

INSERT INTO foncier.ref_nb_pieces (code, libelle, sort_order, vf_suffix) VALUES
    ('TOUS', 'Tous', 0, NULL),
    ('T1', '1 pièce (T1)', 1, 'T1'),
    ('T2', '2 pièces (T2)', 2, 'T2'),
    ('T3', '3 pièces (T3)', 3, 'T3'),
    ('T4', '4 pièces (T4)', 4, 'T4'),
    ('T5', '5 pièces et + (T5)', 5, 'T5')
ON CONFLICT (code) DO UPDATE SET
    libelle = EXCLUDED.libelle,
    sort_order = EXCLUDED.sort_order,
    vf_suffix = EXCLUDED.vf_suffix;
