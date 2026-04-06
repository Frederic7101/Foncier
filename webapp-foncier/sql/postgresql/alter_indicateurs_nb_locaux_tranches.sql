-- Phase 2 : ajout colonnes nb_locaux par tranche surface S1–S5 et pièces T1–T5.
-- Nomenclature : nb_locaux_{maisons|appts|agg}_{s1..s5|t1..t5}
-- Permet d'afficher le nb de locaux vendus exact quand l'utilisateur choisit
-- un niveau détaillé surface ou nb_pièces dans comparaison_scores.
-- Pré-requis : alter_indicateurs_nb_locaux_by_type.sql (Phase 1) déjà exécuté.

-- Surface S1..S5
ALTER TABLE foncier.indicateurs_communes
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s1 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s1   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s1     INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s2 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s2   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s2     INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s3 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s3   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s3     INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s4 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s4   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s4     INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s5 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s5   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s5     INTEGER;

-- Pièces T1..T5
ALTER TABLE foncier.indicateurs_communes
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t1 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t1   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t1     INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t2 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t2   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t2     INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t3 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t3   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t3     INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t4 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t4   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t4     INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t5 INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t5   INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t5     INTEGER;

-- idem pour indicateurs_depts
ALTER TABLE foncier.indicateurs_depts
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s1 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s1   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s1     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s2 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s2   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s2     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s3 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s3   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s3     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s4 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s4   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s4     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s5 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s5   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s5     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t1 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t1   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t1     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t2 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t2   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t2     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t3 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t3   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t3     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t4 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t4   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t4     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t5 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t5   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t5     BIGINT;

-- idem pour indicateurs_regions
ALTER TABLE foncier.indicateurs_regions
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s1 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s1   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s1     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s2 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s2   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s2     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s3 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s3   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s3     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s4 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s4   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s4     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_s5 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_s5   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_s5     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t1 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t1   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t1     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t2 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t2   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t2     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t3 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t3   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t3     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t4 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t4   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t4     BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons_t5 BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts_t5   BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_agg_t5     BIGINT;
