-- Étape 3 : mettre à jour vf_communes à partir de vf_all_ventes.
--
-- Spécification (clé d'agrégation : code_dept, code_postal, commune, annee, type_local) :
--   1. Nombre de ventes : SUM(nb_lignes) des lignes vf_all_ventes agrégées sur la clé.
--   2. Moyennes et médianes : prix_moyen, prix_median, surface_moyenne, surface_mediane,
--      prix_moyen_m2, prix_mediane_m2 ; quartiles Q1 et Q3 du prix/m² (prix_m2_q1, prix_m2_q3).
--   3. Tranches par surface réelle bati : S1 < 25 m², S2 25–35 m², S3 35–45 m², S4 45–55 m², S5 > 55 m² ;
--      pour chaque tranche : médianes (et quartiles) des prix et des surfaces.
--   4. Tranches par nombre de pièces principales : T1 (1), T2 (2), T3 (3), T4 (4), T5 (5+) ;
--      pour chaque tranche : médianes des prix et des surfaces.
--   5. Variations par rapport à l'année N-1 (en %) : nb_ventes, prix moyen et médian,
--      surface moyenne et médiane, prix_moyen_m2 et prix_mediane_m2.
--
-- Prérequis :
--   1. Exécuter une fois 03_alter_vf_communes.sql (ajout prix_median, T1..T5).
--   2. Exécuter une fois 03_sp_refresh_vf_communes_agg.sql (création des procédures).
--
-- Exécution : TRUNCATE puis CALL. Pour une seule année : CALL sp_refresh_vf_communes_agg(2024, NULL, NULL);
-- Pour toutes les années : CALL sp_refresh_vf_communes_all(2014, 2025);

TRUNCATE TABLE vf_communes;

CALL sp_refresh_vf_communes_all(2020, 2025);
