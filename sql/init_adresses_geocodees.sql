-- Table de cache pour les adresses géocodées via BAN

CREATE TABLE IF NOT EXISTS adresses_geocodees (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  code_postal     VARCHAR(10) NOT NULL,
  commune         VARCHAR(100) NOT NULL,
  voie            VARCHAR(255) NULL,
  type_de_voie    VARCHAR(50) NULL,
  no_voie         VARCHAR(20) NULL,
  adresse_norm    VARCHAR(512) NOT NULL,
  latitude        DOUBLE NOT NULL DEFAULT 0,
  longitude       DOUBLE NOT NULL DEFAULT 0,
  geocode_failed  TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1 = BAN interrogé sans résultat',
  last_refreshed  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY ux_adresse_norm (adresse_norm)
);

-- Pré-remplissage optimisé : d’abord les adresses distinctes (sous-requête), puis une seule jointure.
-- Évite le GROUP BY sur des millions de lignes et ne calcule adresse_norm qu’une fois par adresse.

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
  d.code_postal,
  d.commune,
  d.voie,
  d.type_de_voie,
  d.no_voie,
  d.adresse_norm,
  0.0,
  0.0
FROM (
  SELECT DISTINCT
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
    ) AS adresse_norm
  FROM valeursfoncieres v
  WHERE (v.latitude IS NULL OR v.longitude IS NULL)
) d
LEFT JOIN adresses_geocodees a ON a.adresse_norm = d.adresse_norm
WHERE a.id IS NULL;
