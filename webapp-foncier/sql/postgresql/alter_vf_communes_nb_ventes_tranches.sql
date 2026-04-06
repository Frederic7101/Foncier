-- Phase 2 : ajout des colonnes nb_ventes par tranche surface S1–S5 et pièces T1–T5 dans vf_communes.
-- Ces colonnes permettent de connaître le nb exact de ventes DVF pour chaque tranche (surface ou pièces),
-- et ainsi d'afficher la colonne "Nb locaux vendus" correctement dans comparaison_scores
-- quand l'utilisateur choisit le niveau détaillé surface ou nb_pièces.
-- À exécuter une fois ; les valeurs NULL jusqu'au prochain refresh via sp_refresh_vf_communes_agg.

ALTER TABLE foncier.vf_communes
  ADD COLUMN IF NOT EXISTS nb_ventes_s1 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_s2 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_s3 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_s4 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_s5 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_t1 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_t2 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_t3 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_t4 INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS nb_ventes_t5 INTEGER DEFAULT NULL;
