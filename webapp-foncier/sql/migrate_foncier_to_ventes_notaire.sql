-- Migration : renommer le schéma (base) 'foncier' en 'ventes_notaire'.
-- MySQL ne permet pas RENAME DATABASE ; il faut créer la nouvelle base
-- puis déplacer les objets (tables, vues, procédures).
--
-- À exécuter manuellement ou via un script (ex. mysqldump puis import).
--
-- Étape 1 : créer la base ventes_notaire
CREATE DATABASE IF NOT EXISTS `ventes_notaire`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

-- Étape 2 : déplacer toutes les tables de foncier vers ventes_notaire
-- (à adapter selon les tables réelles présentes dans foncier)
-- Exemple pour chaque table :
-- RENAME TABLE foncier.adresses_geocodees TO ventes_notaire.adresses_geocodees;
-- RENAME TABLE foncier.valeursfoncieres TO ventes_notaire.valeursfoncieres;
-- ... (vf_all_ventes, vf_communes, vf_staging_year, vf_staging_year_prev, etc.)
--
-- Étape 3 : recréer les vues et procédures dans ventes_notaire (définitions
-- à reprendre depuis MySQL/foncier_schema.sql en remplaçant le schéma).
--
-- Étape 4 : une fois tout migré et vérifié, supprimer l’ancienne base
-- DROP DATABASE foncier;
--
-- Alternative par dump puis restauration :
--   1. Dump : mysqldump -u root -p foncier > foncier_dump.sql
--   2. Remplacer foncier par ventes_notaire : .\remplace_foncier_ventes_notaire.ps1
--   3. Créer la base AVANT import (sinon ERROR 1046 no database selected) :
--      mysql -u root -p < sql\create_ventes_notaire.sql
--   4. Import : mysql -u root -p ventes_notaire < ventes_notaire_dump.sql
--   5. Après vérification : mysql -u root -p -e "DROP DATABASE foncier;"
