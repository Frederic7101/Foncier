-- Pré-remplissage du cache adresses_geocodees ANNÉE PAR ANNÉE (optimisé, par lots).
-- Chaque bloc insère au plus 25000 lignes par exécution pour éviter "Lock wait timeout exceeded".
-- Pour chaque année : exécuter le bloc en boucle jusqu'à ce qu'aucune ligne ne soit insérée.
-- Arrêter geocode_ban.py pendant l'exécution pour limiter les conflits de verrous.
--
-- Optionnel (avant la 1re exécution) :
--   SET SESSION innodb_lock_wait_timeout = 120;

-- ========== Année 2020 (répéter jusqu'à 0 ligne insérée) ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT d.code_postal, d.commune, d.voie, d.type_de_voie, d.no_voie, d.adresse_norm, 0.0, 0.0
FROM (
  SELECT d2.code_postal, d2.commune, d2.voie, d2.type_de_voie, d2.no_voie, d2.adresse_norm
  FROM (
    SELECT DISTINCT
      v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm
    FROM valeursfoncieres v
    WHERE (v.latitude IS NULL OR v.longitude IS NULL)
      AND v.date_mutation >= '2020-01-01' AND v.date_mutation < '2021-01-01'
  ) d2
  LEFT JOIN adresses_geocodees a ON a.adresse_norm = d2.adresse_norm
  WHERE a.id IS NULL
  LIMIT 25000
) d;

-- ========== Année 2021 (répéter jusqu'à 0 ligne insérée) ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT d.code_postal, d.commune, d.voie, d.type_de_voie, d.no_voie, d.adresse_norm, 0.0, 0.0
FROM (
  SELECT d2.code_postal, d2.commune, d2.voie, d2.type_de_voie, d2.no_voie, d2.adresse_norm
  FROM (
    SELECT DISTINCT
      v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm
    FROM valeursfoncieres v
    WHERE (v.latitude IS NULL OR v.longitude IS NULL)
      AND v.date_mutation >= '2021-01-01' AND v.date_mutation < '2022-01-01'
  ) d2
  LEFT JOIN adresses_geocodees a ON a.adresse_norm = d2.adresse_norm
  WHERE a.id IS NULL
  LIMIT 25000
) d;

-- ========== Année 2022 (répéter jusqu'à 0 ligne insérée) ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT d.code_postal, d.commune, d.voie, d.type_de_voie, d.no_voie, d.adresse_norm, 0.0, 0.0
FROM (
  SELECT d2.code_postal, d2.commune, d2.voie, d2.type_de_voie, d2.no_voie, d2.adresse_norm
  FROM (
    SELECT DISTINCT
      v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm
    FROM valeursfoncieres v
    WHERE (v.latitude IS NULL OR v.longitude IS NULL)
      AND v.date_mutation >= '2022-01-01' AND v.date_mutation < '2023-01-01'
  ) d2
  LEFT JOIN adresses_geocodees a ON a.adresse_norm = d2.adresse_norm
  WHERE a.id IS NULL
  LIMIT 25000
) d;

-- ========== Année 2023 (répéter jusqu'à 0 ligne insérée) ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT d.code_postal, d.commune, d.voie, d.type_de_voie, d.no_voie, d.adresse_norm, 0.0, 0.0
FROM (
  SELECT d2.code_postal, d2.commune, d2.voie, d2.type_de_voie, d2.no_voie, d2.adresse_norm
  FROM (
    SELECT DISTINCT
      v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm
    FROM valeursfoncieres v
    WHERE (v.latitude IS NULL OR v.longitude IS NULL)
      AND v.date_mutation >= '2023-01-01' AND v.date_mutation < '2024-01-01'
  ) d2
  LEFT JOIN adresses_geocodees a ON a.adresse_norm = d2.adresse_norm
  WHERE a.id IS NULL
  LIMIT 25000
) d;

-- ========== Année 2024 (répéter jusqu'à 0 ligne insérée) ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT d.code_postal, d.commune, d.voie, d.type_de_voie, d.no_voie, d.adresse_norm, 0.0, 0.0
FROM (
  SELECT d2.code_postal, d2.commune, d2.voie, d2.type_de_voie, d2.no_voie, d2.adresse_norm
  FROM (
    SELECT DISTINCT
      v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm
    FROM valeursfoncieres v
    WHERE (v.latitude IS NULL OR v.longitude IS NULL)
      AND v.date_mutation >= '2024-01-01' AND v.date_mutation < '2025-01-01'
  ) d2
  LEFT JOIN adresses_geocodees a ON a.adresse_norm = d2.adresse_norm
  WHERE a.id IS NULL
  LIMIT 25000
) d;

-- ========== Année 2025 (répéter jusqu'à 0 ligne insérée) ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT d.code_postal, d.commune, d.voie, d.type_de_voie, d.no_voie, d.adresse_norm, 0.0, 0.0
FROM (
  SELECT d2.code_postal, d2.commune, d2.voie, d2.type_de_voie, d2.no_voie, d2.adresse_norm
  FROM (
    SELECT DISTINCT
      v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm
    FROM valeursfoncieres v
    WHERE (v.latitude IS NULL OR v.longitude IS NULL)
      AND v.date_mutation >= '2025-01-01' AND v.date_mutation < '2026-01-01'
  ) d2
  LEFT JOIN adresses_geocodees a ON a.adresse_norm = d2.adresse_norm
  WHERE a.id IS NULL
  LIMIT 25000
) d;
