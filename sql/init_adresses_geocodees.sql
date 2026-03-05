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
  last_refreshed  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY ux_adresse_norm (adresse_norm)
);

-- Pré‑remplissage du cache avec les adresses distinctes manquantes

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
FROM valeursfoncieres v
LEFT JOIN adresses_geocodees a
  ON a.adresse_norm = CONCAT_WS(' ',
        COALESCE(v.no_voie, ''),
        COALESCE(v.type_de_voie, ''),
        COALESCE(v.voie, ''),
        v.code_postal,
        v.commune
     )
WHERE (v.latitude IS NULL OR v.longitude IS NULL)
  AND a.id IS NULL
GROUP BY
  v.code_postal,
  v.commune,
  v.voie,
  v.type_de_voie,
  v.no_voie;

