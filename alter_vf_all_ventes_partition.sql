-- ============================================================
-- Partitionnement de vf_all_ventes par année (date_mutation)
-- Base : foncier
-- À exécuter dans dBeaver (éditeur SQL) sur la base foncier.
-- Prévoir un délai si la table est volumineuse (réécriture).
-- ============================================================

USE foncier;

ALTER TABLE vf_all_ventes
PARTITION BY RANGE (YEAR(date_mutation)) (
  PARTITION p2020 VALUES LESS THAN (2021),
  PARTITION p2021 VALUES LESS THAN (2022),
  PARTITION p2022 VALUES LESS THAN (2023),
  PARTITION p2023 VALUES LESS THAN (2024),
  PARTITION p2024 VALUES LESS THAN (2025),
  PARTITION p2025 VALUES LESS THAN (2026),
  PARTITION p2026 VALUES LESS THAN (2027),
  PARTITION pmax  VALUES LESS THAN MAXVALUE
);

-- Vérification (optionnel) :
-- SELECT PARTITION_NAME, TABLE_ROWS FROM information_schema.PARTITIONS
-- WHERE TABLE_SCHEMA = 'foncier' AND TABLE_NAME = 'vf_all_ventes';
