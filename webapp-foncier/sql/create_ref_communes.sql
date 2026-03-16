-- Table de référence des communes (code_dept, code_postal, commune) pour listes déroulantes rapides.
-- Peut être alimentée depuis vf_communes. À rafraîchir après mise à jour de vf_communes si besoin.

CREATE TABLE IF NOT EXISTS ref_communes (
  code_dept   VARCHAR(5)   NOT NULL,
  code_postal VARCHAR(10)  NOT NULL,
  commune     VARCHAR(100) NOT NULL,
  PRIMARY KEY (code_dept, code_postal, commune),
  KEY idx_commune_postal (commune, code_postal)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Alimenter depuis vf_communes (à exécuter une fois, puis après chaque refresh important de vf_communes)
INSERT IGNORE INTO ref_communes (code_dept, code_postal, commune)
SELECT DISTINCT code_dept, code_postal, commune
FROM vf_communes
ORDER BY commune, code_postal;
