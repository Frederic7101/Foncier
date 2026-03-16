-- Étape 2 : (re)remplir vf_all_ventes en agrégant valeursfoncieres (Vente) par transaction
-- (lieu + date + type_local). On conserve la table : création si absente, sinon TRUNCATE + INSERT.
-- Si la table a encore l’ancienne structure (nb_mutations, prix_moyen…), exécuter une fois 02_alter_vf_all_ventes.sql.

-- Créer la table uniquement si elle n’existe pas (structure cible)
CREATE TABLE IF NOT EXISTS vf_all_ventes (
  code_dept              VARCHAR(5)   NOT NULL,
  code_postal            VARCHAR(10)  NOT NULL,
  commune                VARCHAR(100) NOT NULL,
  type_local             VARCHAR(50)  NOT NULL,
  annee                  INT          NOT NULL,
  date_mutation          DATE         NOT NULL,
  adresse_norm           VARCHAR(512) DEFAULT NULL COMMENT 'Adresse normalisée (lieu de la transaction)',
  voie                   VARCHAR(255) DEFAULT NULL,
  no_voie                VARCHAR(20)  DEFAULT NULL,
  nb_lignes              INT          NOT NULL COMMENT 'Nombre de lignes valeursfoncieres agrégées',
  valeur_fonciere        DECIMAL(15,2) DEFAULT NULL COMMENT 'Moyenne valeur_fonciere',
  surface_reelle_bati    DECIMAL(15,2) DEFAULT NULL COMMENT 'Moyenne surface_reelle_bati',
  prix_m2                DECIMAL(15,2) DEFAULT NULL COMMENT 'valeur_fonciere/surface_reelle_bati (pondéré)',
  nombre_pieces_principales INT       DEFAULT NULL COMMENT 'Moyenne nombre_pieces_principales',
  latitude               DOUBLE       DEFAULT NULL,
  longitude              DOUBLE       DEFAULT NULL,
  last_refreshed         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY ux_vf_agg (annee, type_local, code_dept, code_postal, commune, date_mutation, adresse_norm(255)),
  KEY idx_vf_all_date_type_commune (date_mutation, type_local, code_dept, code_postal, commune),
  KEY idx_vf_all_annee_type_commune (annee, type_local, code_dept, code_postal, commune)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Vider la table puis réinsérer les données agrégées
TRUNCATE TABLE vf_all_ventes;

-- Agrégation : une ligne par (date_mutation, adresse normalisée, type_local)
INSERT INTO vf_all_ventes (
  code_dept,
  code_postal,
  commune,
  type_local,
  annee,
  date_mutation,
  adresse_norm,
  voie,
  no_voie,
  nb_lignes,
  valeur_fonciere,
  surface_reelle_bati,
  prix_m2,
  nombre_pieces_principales,
  latitude,
  longitude
)
SELECT
  v.code_departement     AS code_dept,
  v.code_postal         AS code_postal,
  v.commune             AS commune,
  v.type_local          AS type_local,
  YEAR(v.date_mutation) AS annee,
  v.date_mutation       AS date_mutation,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm,
  MAX(v.voie)           AS voie,
  MAX(v.no_voie)        AS no_voie,
  COUNT(*)              AS nb_lignes,
  ROUND(AVG(v.valeur_fonciere), 2) AS valeur_fonciere,
  ROUND(AVG(v.surface_reelle_bati), 2) AS surface_reelle_bati,
  ROUND(SUM(v.valeur_fonciere) / NULLIF(SUM(v.surface_reelle_bati), 0), 2) AS prix_m2,
  ROUND(AVG(v.nombre_pieces_principales), 0) AS nombre_pieces_principales,
  MAX(v.latitude)      AS latitude,
  MAX(v.longitude)      AS longitude
FROM valeursfoncieres v
WHERE v.nature_mutation = 'Vente'
  AND v.type_local IN ('Appartement', 'Maison')
GROUP BY
  v.code_departement,
  v.code_postal,
  v.commune,
  v.type_local,
  v.date_mutation,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune);
