-- Mise à jour des colonnes latitude / longitude dans valeursfoncieres
-- pour une année donnée (remplacer :year_par_exemple par l'année voulue).

UPDATE valeursfoncieres v
JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ',
        COALESCE(v.no_voie, ''),
        COALESCE(v.type_de_voie, ''),
        COALESCE(v.voie, ''),
        v.code_postal,
        v.commune
     )
SET
  v.latitude  = a.latitude,
  v.longitude = a.longitude
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND v.date_mutation >= '2020-01-01'
  AND v.date_mutation <  '2021-01-01';

