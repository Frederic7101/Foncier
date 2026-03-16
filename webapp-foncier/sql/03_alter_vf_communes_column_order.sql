-- Reordonner les colonnes de vf_communes (à exécuter après 03_alter_vf_communes.sql).
-- Rapide si la table est vide (TRUNCATE). Ordre cible :
--   ... prix_moyen, prix_median, surface_moyenne, surface_mediane, prix_moyen_m2, ...
--   ... prix_m2_w_s5, prix_med_T1, surf_med_T1, prix_m2_w_T1, ... prix_med_T5, surf_med_T5, prix_m2_w_T5, ...
--   ... var_pct, last_refreshed

ALTER TABLE vf_communes
  MODIFY COLUMN prix_median DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane valeur_fonciere' AFTER prix_moyen,
  MODIFY COLUMN surface_mediane DECIMAL(15,2) DEFAULT NULL AFTER surface_moyenne,
  MODIFY COLUMN prix_med_T1 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T1 (1 piece)' AFTER prix_m2_w_s5,
  MODIFY COLUMN surf_med_T1 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T1' AFTER prix_med_T1,
  MODIFY COLUMN prix_m2_w_T1 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T1' AFTER surf_med_T1,
  MODIFY COLUMN prix_med_T2 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T2 (2 pieces)' AFTER prix_m2_w_T1,
  MODIFY COLUMN surf_med_T2 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T2' AFTER prix_med_T2,
  MODIFY COLUMN prix_m2_w_T2 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T2' AFTER surf_med_T2,
  MODIFY COLUMN prix_med_T3 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T3 (3 pieces)' AFTER prix_m2_w_T2,
  MODIFY COLUMN surf_med_T3 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T3' AFTER prix_med_T3,
  MODIFY COLUMN prix_m2_w_T3 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T3' AFTER surf_med_T3,
  MODIFY COLUMN prix_med_T4 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T4 (4 pieces)' AFTER prix_m2_w_T3,
  MODIFY COLUMN surf_med_T4 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T4' AFTER prix_med_T4,
  MODIFY COLUMN prix_m2_w_T4 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T4' AFTER surf_med_T4,
  MODIFY COLUMN prix_med_T5 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T5 (5+ pieces)' AFTER prix_m2_w_T4,
  MODIFY COLUMN surf_med_T5 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T5' AFTER prix_med_T5,
  MODIFY COLUMN prix_m2_w_T5 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T5' AFTER surf_med_T5;
