-- À exécuter une fois : ajouter prix_median (médiane des prix) et tranches T1..T5 (par nombre de pièces).
-- Ignorer si les colonnes existent déjà (erreur duplicate column).
--
-- Variante RAPIDE : colonnes en fin de table (sans AFTER) + ALGORITHM=INSTANT si MySQL 8.0.
-- En cas d'erreur sur INSTANT (ex. MySQL 5.7), retirer la ligne ", ALGORITHM=INSTANT".

ALTER TABLE vf_communes
  ADD COLUMN prix_median DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane valeur_fonciere',
  ADD COLUMN prix_med_T1 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T1 (1 piece)',
  ADD COLUMN surf_med_T1 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T1',
  ADD COLUMN prix_med_T2 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T2 (2 pieces)',
  ADD COLUMN surf_med_T2 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T2',
  ADD COLUMN prix_med_T3 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T3 (3 pieces)',
  ADD COLUMN surf_med_T3 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T3',
  ADD COLUMN prix_med_T4 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T4 (4 pieces)',
  ADD COLUMN surf_med_T4 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T4',
  ADD COLUMN prix_med_T5 DECIMAL(15,2) DEFAULT NULL COMMENT 'Mediane prix T5 (5+ pieces)',
  ADD COLUMN surf_med_T5 DECIMAL(15,2) DEFAULT NULL COMMENT 'Surface mediane T5',
  ADD COLUMN prix_m2_w_T1 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T1',
  ADD COLUMN prix_m2_w_T2 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T2',
  ADD COLUMN prix_m2_w_T3 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T3',
  ADD COLUMN prix_m2_w_T4 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T4',
  ADD COLUMN prix_m2_w_T5 DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix moyen/m2 pondere T5',
  ALGORITHM=INSTANT;
