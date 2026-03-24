-- Table des indicateurs agrégés par département (pré-calcul pour comparaison_scores mode=departements).
-- Une ligne par code_dept. Schéma : foncier (convention projet).
DROP TABLE IF EXISTS foncier.indicateurs_depts;
CREATE TABLE IF NOT EXISTS foncier.indicateurs_depts (
  code_dept            TEXT PRIMARY KEY,
  dep_nom              TEXT,
  reg_nom              TEXT,
  code_region          TEXT,
  population           BIGINT,
  nb_locaux            BIGINT,
  nb_ventes_dvf        BIGINT,
  indicateurs_par_periode JSONB,
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

CREATE INDEX IF NOT EXISTS idx_indicateurs_depts_region
  ON foncier.indicateurs_depts (code_region);

COMMENT ON TABLE foncier.indicateurs_depts IS 'Indicateurs agrégés par département pour comparaison_scores.';
