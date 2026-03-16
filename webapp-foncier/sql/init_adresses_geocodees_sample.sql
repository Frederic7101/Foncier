-- Version limitée : on lit d'abord un petit nombre de lignes (LIMIT),
-- puis on en déduit les adresses distinctes. Ainsi le GROUP BY ne porte
-- que sur cet échantillon, pas sur toute la table.

INSERT IGNORE INTO adresses_geocodees (
  code_postal,
  commune,
  voie,
  type_de_voie,
  no_voie,
  adresse_norm,
  latitude,
  longitude
)
SELECT
  v.code_postal,
  v.commune,
  v.voie,
  v.type_de_voie,
  v.no_voie,
  CONCAT_WS(' ',
    COALESCE(v.no_voie, ''),
    COALESCE(v.type_de_voie, ''),
    COALESCE(v.voie, ''),
    v.code_postal,
    v.commune
  ) AS adresse_norm,
  0.0 AS latitude,
  0.0 AS longitude
FROM (
  SELECT code_postal, commune, voie, type_de_voie, no_voie
  FROM (
    SELECT code_postal, commune, voie, type_de_voie, no_voie
    FROM valeursfoncieres
    WHERE latitude IS NULL
    LIMIT 10000
  ) AS chunk
  GROUP BY code_postal, commune, voie, type_de_voie, no_voie
) v
LEFT JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ',
        COALESCE(v.no_voie, ''),
        COALESCE(v.type_de_voie, ''),
        COALESCE(v.voie, ''),
        v.code_postal,
        v.commune
     )
WHERE a.id IS NULL;
