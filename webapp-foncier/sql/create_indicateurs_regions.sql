-- Table des indicateurs agrégés par région (pré-calcul pour comparaison_scores mode=regions).
-- Une ligne par code_region. Schéma : foncier (convention projet).
DROP TABLE IF EXISTS foncier.indicateurs_regions;
CREATE TABLE IF NOT EXISTS foncier.indicateurs_regions (
  code_region          TEXT PRIMARY KEY,
  reg_nom              TEXT,
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

COMMENT ON TABLE foncier.indicateurs_regions IS 'Indicateurs agrégés par région pour comparaison_scores.';
