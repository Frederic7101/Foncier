-- Créer la base ventes_notaire AVANT d'importer ventes_notaire_dump.sql
-- (évite ERROR 1046: no database selected à la ligne du premier CREATE TABLE)

CREATE DATABASE IF NOT EXISTS `ventes_notaire`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;
