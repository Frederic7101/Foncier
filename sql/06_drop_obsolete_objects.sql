-- Optionnel : supprimer objets obsolètes si présents en base (procédures/tables anciennes).
-- À exécuter à la main si tu nettoies une base qui contenait l’ancien schéma.
-- Ne pas lancer en automatique sans vérifier.

-- Procédure qui insérait dans valeursfoncieres_agg (table non utilisée)
DROP PROCEDURE IF EXISTS sp_refresh_valeursfoncieres_agg;

-- Procédure simple vf_communes (5 colonnes) ; on utilise sp_refresh_vf_communes_agg
DROP PROCEDURE IF EXISTS sp_refresh_vf_communes;

-- Tables staging éventuellement créées par d’anciens scripts
DROP TABLE IF EXISTS vf_staging_year_prev;
DROP TABLE IF EXISTS vf_staging_year;

-- Table agrégat ancienne (si elle existait)
DROP TABLE IF EXISTS valeursfoncieres_agg;
