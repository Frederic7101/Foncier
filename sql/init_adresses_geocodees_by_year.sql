-- Pré-remplissage du cache adresses_geocodees ANNÉE PAR ANNÉE
-- pour rester sous ~15 min par exécution (GROUP BY sur ~1–1,5 M lignes/an).
-- Exécuter un bloc à la fois dans dBeaver, ou tout le fichier en une fois.

-- ========== Année 2020 ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT
  v.code_postal,
  v.commune,
  v.voie,
  v.type_de_voie,
  v.no_voie,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm,
  0.0, 0.0
FROM valeursfoncieres v
LEFT JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND a.id IS NULL
  AND v.date_mutation >= '2020-01-01' AND v.date_mutation < '2021-01-01'
GROUP BY v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie;

-- ========== Année 2021 ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT
  v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune),
  0.0, 0.0
FROM valeursfoncieres v
LEFT JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
WHERE (v.latitude IS NULL OR v.longitude IS NULL) AND a.id IS NULL
  AND v.date_mutation >= '2021-01-01' AND v.date_mutation < '2022-01-01'
GROUP BY v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie;

-- ========== Année 2022 ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT
  v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune),
  0.0, 0.0
FROM valeursfoncieres v
LEFT JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
WHERE (v.latitude IS NULL OR v.longitude IS NULL) AND a.id IS NULL
  AND v.date_mutation >= '2022-01-01' AND v.date_mutation < '2023-01-01'
GROUP BY v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie;

-- ========== Année 2023 ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT
  v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune),
  0.0, 0.0
FROM valeursfoncieres v
LEFT JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
WHERE (v.latitude IS NULL OR v.longitude IS NULL) AND a.id IS NULL
  AND v.date_mutation >= '2023-01-01' AND v.date_mutation < '2024-01-01'
GROUP BY v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie;

-- ========== Année 2024 ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT
  v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune),
  0.0, 0.0
FROM valeursfoncieres v
LEFT JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
WHERE (v.latitude IS NULL OR v.longitude IS NULL) AND a.id IS NULL
  AND v.date_mutation >= '2024-01-01' AND v.date_mutation < '2025-01-01'
GROUP BY v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie;

-- ========== Année 2025 ==========
INSERT IGNORE INTO adresses_geocodees (
  code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude
)
SELECT
  v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune),
  0.0, 0.0
FROM valeursfoncieres v
LEFT JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
WHERE (v.latitude IS NULL OR v.longitude IS NULL) AND a.id IS NULL
  AND v.date_mutation >= '2025-01-01' AND v.date_mutation < '2026-01-01'
GROUP BY v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie;
