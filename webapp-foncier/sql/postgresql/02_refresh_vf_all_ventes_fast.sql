-- Étape 2 (version rapide) : refresh vf_all_ventes via table de staging SANS index.
-- Insérer 5 M lignes sans maintenir les index, puis construire les index en une fois et swap.
-- Prérequis : vf_all_ventes doit déjà exister (sinon exécuter une fois 02_refresh_vf_all_ventes.sql).
-- Pour accélérer encore le SELECT : index sur valeursfoncieres (nature_mutation, type_local, date_mutation, ...).
-- Modification : 06/04/2026 ajout des autres types de locaux pour récupérer toutes les ventes de vf_all_ventes et les visualiser dans le FO + conversion sous PostgreSQL 


-- 1) Table de staging : même structure, aucun index (insert maximalement rapide)
DROP TABLE IF EXISTS vf_all_ventes_staging;

DROP TABLE IF EXISTS vf_all_ventes_staging;

CREATE TABLE vf_all_ventes_staging (
    code_dept               VARCHAR(5)   NOT NULL,
    code_postal             VARCHAR(10)  NOT NULL,
    commune                 VARCHAR(100) NOT NULL,
    type_local              VARCHAR(50)  NOT NULL,
    annee                   INT          NOT NULL,
    date_mutation           DATE         NOT NULL,
    adresse_norm            VARCHAR(512),
    voie                    VARCHAR(255),
    no_voie                 VARCHAR(20),
    nb_lignes               INT          NOT NULL,
    valeur_fonciere         DECIMAL(15,2),
    surface_reelle_bati     DECIMAL(15,2),
    prix_m2                 DECIMAL(15,2),
    nombre_pieces_principales INT,
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    last_refreshed          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2) Agrégation : une seule grosse écriture, pas de mise à jour d’index
INSERT INTO vf_all_ventes_staging (
  code_dept, code_postal, commune, type_local, annee, date_mutation,
  adresse_norm, voie, no_voie, nb_lignes, valeur_fonciere, surface_reelle_bati,
  prix_m2, nombre_pieces_principales, latitude, longitude
)
SELECT
  v.code_departement     AS code_dept,
  v.code_postal         AS code_postal,
  v.commune             AS commune,
  v.type_local          AS type_local,
  EXTRACT(YEAR FROM v.date_mutation)     AS annee,               -- ← remplacement de YEAR()
  v.date_mutation       AS date_mutation,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm,
  MAX(v.voie)           AS voie,
  MAX(v.no_voie)        AS no_voie,
  COUNT(*)              AS nb_lignes,
  ROUND(AVG(v.valeur_fonciere), 2) AS valeur_fonciere,
  ROUND(AVG(v.surface_reelle_bati), 2) AS surface_reelle_bati,
  ROUND(SUM(v.valeur_fonciere) / NULLIF(SUM(v.surface_reelle_bati), 0), 2) AS prix_m2,
  ROUND(AVG(v.nombre_pieces_principales), 0) AS nombre_pieces_principales,
  MAX(v.latitude)       AS latitude,
  MAX(v.longitude)      AS longitude
FROM valeursfoncieres v
WHERE v.nature_mutation = 'Vente'
  AND v.type_local IN ('Appartement', 'Maison', 'Dépendance', 'Local industriel. commercial ou assimilé')
GROUP BY
  v.code_departement,
  v.code_postal,
  v.commune,
  v.type_local,
  v.date_mutation,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune);

-- 3) Construire les index une fois les données en place (souvent plus rapide que 5 M mises à jour d’index)
ALTER TABLE vf_all_ventes_staging
  ADD UNIQUE KEY ux_vf_agg (annee, type_local, code_dept, code_postal, commune, date_mutation, adresse_norm(255)),
  ADD KEY idx_vf_all_date_type_commune (date_mutation, type_local, code_dept, code_postal, commune),
  ADD KEY idx_vf_all_annee_type_commune (annee, type_local, code_dept, code_postal, commune);

-- 4) Swap atomique : l’ancienne table disparaît, la nouvelle prend son nom
RENAME TABLE vf_all_ventes TO vf_all_ventes_old, vf_all_ventes_staging TO vf_all_ventes;
-- DROP TABLE vf_all_ventes_old;