-- Vues vw_valeursfoncieres_agg_YYYY non utilisées : on s'appuie sur vf_all_ventes et vf_communes.
-- Ce script supprime les vues par année si elles existent (éviter qu'elles soient utilisées par erreur).

DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2000;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2014;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2015;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2016;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2017;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2018;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2019;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2020;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2021;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2022;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2023;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2024;
DROP VIEW IF EXISTS vw_valeursfoncieres_agg_2025;
