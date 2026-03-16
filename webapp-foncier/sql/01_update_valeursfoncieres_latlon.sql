-- Étape 1.2 : recopier latitude/longitude de adresses_geocodees vers valeursfoncieres.
-- À exécuter après avoir fini le géocodage (geocode_ban.py).
-- Une requête par année (2020 à 2025) pour limiter la charge.

UPDATE valeursfoncieres v
JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
SET v.latitude = a.latitude, v.longitude = a.longitude
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND (a.latitude != 0 OR a.longitude != 0)
  AND v.date_mutation >= '2020-01-01' AND v.date_mutation < '2021-01-01';

UPDATE valeursfoncieres v
JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
SET v.latitude = a.latitude, v.longitude = a.longitude
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND (a.latitude != 0 OR a.longitude != 0)
  AND v.date_mutation >= '2021-01-01' AND v.date_mutation < '2022-01-01';

UPDATE valeursfoncieres v
JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
SET v.latitude = a.latitude, v.longitude = a.longitude
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND (a.latitude != 0 OR a.longitude != 0)
  AND v.date_mutation >= '2022-01-01' AND v.date_mutation < '2023-01-01';

UPDATE valeursfoncieres v
JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
SET v.latitude = a.latitude, v.longitude = a.longitude
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND (a.latitude != 0 OR a.longitude != 0)
  AND v.date_mutation >= '2023-01-01' AND v.date_mutation < '2024-01-01';

UPDATE valeursfoncieres v
JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
SET v.latitude = a.latitude, v.longitude = a.longitude
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND (a.latitude != 0 OR a.longitude != 0)
  AND v.date_mutation >= '2024-01-01' AND v.date_mutation < '2025-01-01';

UPDATE valeursfoncieres v
JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune)
SET v.latitude = a.latitude, v.longitude = a.longitude
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND (a.latitude != 0 OR a.longitude != 0)
  AND v.date_mutation >= '2025-01-01' AND v.date_mutation < '2026-01-01';
