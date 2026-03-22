-- Table des indicateurs précalculés par commune pour la page comparaison des scores.
-- Une ligne par commune (code_insee). Permet d'afficher comparaison_scores sans appeler get_fiche_logement/get_stats pour chaque commune.
-- Alimentée par un script de refresh ou à la volée (lazy) lorsqu'une commune manquante est demandée.
-- Schéma : foncier (convention projet).
DROP TABLE IF EXISTS foncier.indicateurs_communes;
CREATE TABLE IF NOT EXISTS foncier.indicateurs_communes (
  code_insee           TEXT PRIMARY KEY,
  code_dept            TEXT NOT NULL,
  code_postal          TEXT NOT NULL,
  commune              TEXT NOT NULL,
  reg_nom              TEXT,
  dep_nom              TEXT,
  population           INTEGER,
  renta_brute          NUMERIC(6,2),
  renta_nette          NUMERIC(6,2),
  renta_brute_maisons  NUMERIC(6,2),
  renta_nette_maisons  NUMERIC(6,2),
  renta_brute_appts    NUMERIC(6,2),
  renta_nette_appts    NUMERIC(6,2),
  renta_brute_parking  NUMERIC(6,2),
  renta_nette_parking  NUMERIC(6,2),
  renta_brute_local_indus NUMERIC(6,2),
  renta_nette_local_indus NUMERIC(6,2),
  renta_brute_terrain  NUMERIC(6,2),
  renta_nette_terrain  NUMERIC(6,2),
  renta_brute_immeuble NUMERIC(6,2),
  renta_nette_immeuble NUMERIC(6,2),
  taux_tfb             NUMERIC(8,4),
  taux_teom            NUMERIC(8,4),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_indicateurs_communes_code_dept_postal
  ON foncier.indicateurs_communes (code_dept, code_postal);

COMMENT ON TABLE foncier.indicateurs_communes IS 'Indicateurs par commune pour comparaison_scores (rentabilité, fiscalité). Rempli depuis fiche_logement_cache + ref_communes ou à la volée.';
