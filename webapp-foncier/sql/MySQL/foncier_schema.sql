-- MySQL dump 10.13  Distrib 8.0.28, for Win64 (x86_64)
--
-- Host: localhost    Database: foncier
-- ------------------------------------------------------
-- Server version	8.0.28

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `adresses_geocodees`
--

DROP TABLE IF EXISTS `adresses_geocodees`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `adresses_geocodees` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `code_postal` varchar(10) NOT NULL,
  `commune` varchar(100) NOT NULL,
  `voie` varchar(255) DEFAULT NULL,
  `type_de_voie` varchar(50) DEFAULT NULL,
  `no_voie` varchar(20) DEFAULT NULL,
  `adresse_norm` varchar(512) NOT NULL,
  `latitude` double NOT NULL DEFAULT '0',
  `longitude` double NOT NULL DEFAULT '0',
  `last_refreshed` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_adresse_norm` (`adresse_norm`)
) ENGINE=InnoDB AUTO_INCREMENT=1114105 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `valeursfoncieres`
--

DROP TABLE IF EXISTS `valeursfoncieres`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `valeursfoncieres` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `date_mutation` date NOT NULL,
  `nature_mutation` varchar(100) NOT NULL,
  `valeur_fonciere` decimal(15,2) NOT NULL,
  `no_voie` varchar(20) DEFAULT NULL,
  `type_de_voie` varchar(50) DEFAULT NULL,
  `voie` varchar(255) DEFAULT NULL,
  `code_postal` varchar(10) NOT NULL,
  `commune` varchar(100) NOT NULL,
  `code_departement` varchar(3) NOT NULL,
  `code_type_local` varchar(10) NOT NULL,
  `type_local` varchar(50) NOT NULL,
  `surface_reelle_bati` decimal(15,2) DEFAULT NULL,
  `nombre_pieces_principales` int DEFAULT NULL,
  `surface_terrain` decimal(15,2) DEFAULT NULL,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `dedup_key_hash` char(64) GENERATED ALWAYS AS (sha2(concat_ws(_utf8mb4'|',coalesce(`code_departement`,_utf8mb4''),coalesce(`code_postal`,_utf8mb4''),coalesce(`nature_mutation`,_utf8mb4''),coalesce(cast(`date_mutation` as char charset utf8mb4),_utf8mb4''),coalesce(`type_local`,_utf8mb4''),coalesce(cast(`valeur_fonciere` as char charset utf8mb4),_utf8mb4''),nullif(trim(`voie`),_utf8mb4''),nullif(trim(`type_de_voie`),_utf8mb4''),nullif(trim(`no_voie`),_utf8mb4''),(case when (`surface_reelle_bati` is null) then NULL else cast(`surface_reelle_bati` as char charset utf8mb4) end)),256)) STORED,
  PRIMARY KEY (`id`,`date_mutation`),
  UNIQUE KEY `ux_ValeursFoncieres_dt_hash` (`date_mutation`,`dedup_key_hash`),
  KEY `idx_ValeursFoncieres_type_local` (`type_local`),
  KEY `idx_ValeursFoncieres_code_departement` (`code_departement`),
  KEY `idx_ValeursFoncieres_code_postal` (`code_postal`),
  KEY `idx_ValeursFoncieres_commune` (`commune`),
  KEY `idx_ValeursFoncieres_date` (`date_mutation`),
  KEY `idx_vf_lat_lon` (`latitude`,`longitude`)
) ENGINE=InnoDB AUTO_INCREMENT=9131365 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
/*!50100 PARTITION BY RANGE (year(`date_mutation`))
(PARTITION p2020 VALUES LESS THAN (2021) ENGINE = InnoDB,
 PARTITION p2021 VALUES LESS THAN (2022) ENGINE = InnoDB,
 PARTITION p2022 VALUES LESS THAN (2023) ENGINE = InnoDB,
 PARTITION p2023 VALUES LESS THAN (2024) ENGINE = InnoDB,
 PARTITION p2024 VALUES LESS THAN (2025) ENGINE = InnoDB,
 PARTITION p2025 VALUES LESS THAN (2026) ENGINE = InnoDB,
 PARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `vf_all_ventes`
--

DROP TABLE IF EXISTS `vf_all_ventes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `vf_all_ventes` (
  `code_dept` varchar(5) NOT NULL,
  `code_postal` varchar(5) NOT NULL,
  `commune` varchar(50) NOT NULL,
  `type_local` varchar(20) NOT NULL,
  `annee` int NOT NULL,
  `date_mutation` date NOT NULL,
  `voie` varchar(50) DEFAULT NULL,
  `no_voie` varchar(10) DEFAULT NULL,
  `nb_mutations` int NOT NULL,
  `prix_moyen` decimal(15,2) DEFAULT NULL,
  `surface_moyenne` decimal(15,2) DEFAULT NULL,
  `prix_moyen_m2` decimal(15,2) DEFAULT NULL,
  `last_refreshed` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `ux_vf_agg` (`annee`,`type_local`,`code_dept`,`code_postal`,`commune`,`date_mutation`,`voie`,`no_voie`),
  KEY `idx_vf_all_date_type_commune` (`date_mutation`,`type_local`,`code_dept`,`code_postal`,`commune`),
  KEY `idx_vf_all_annee_type_commune` (`annee`,`type_local`,`code_dept`,`code_postal`,`commune`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
/*!50100 PARTITION BY RANGE (year(`date_mutation`))
(PARTITION p2020 VALUES LESS THAN (2021) ENGINE = InnoDB,
 PARTITION p2021 VALUES LESS THAN (2022) ENGINE = InnoDB,
 PARTITION p2022 VALUES LESS THAN (2023) ENGINE = InnoDB,
 PARTITION p2023 VALUES LESS THAN (2024) ENGINE = InnoDB,
 PARTITION p2024 VALUES LESS THAN (2025) ENGINE = InnoDB,
 PARTITION p2025 VALUES LESS THAN (2026) ENGINE = InnoDB,
 PARTITION p2026 VALUES LESS THAN (2027) ENGINE = InnoDB,
 PARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `vf_communes`
--

DROP TABLE IF EXISTS `vf_communes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `vf_communes` (
  `code_dept` varchar(5) NOT NULL,
  `code_postal` varchar(5) NOT NULL,
  `commune` varchar(100) NOT NULL,
  `annee` int NOT NULL,
  `type_local` varchar(50) NOT NULL,
  `nb_ventes` int NOT NULL,
  `prix_moyen` decimal(15,2) DEFAULT NULL,
  `surface_moyenne` decimal(15,2) DEFAULT NULL,
  `prix_moyen_m2` decimal(15,2) DEFAULT NULL,
  `prix_m2_q1` decimal(15,2) DEFAULT NULL,
  `prix_m2_mediane` decimal(15,2) DEFAULT NULL,
  `prix_m2_q3` decimal(15,2) DEFAULT NULL,
  `surface_mediane` decimal(15,2) DEFAULT NULL,
  `prix_med_s1` decimal(15,2) DEFAULT NULL,
  `surf_med_s1` decimal(15,2) DEFAULT NULL,
  `prix_m2_w_s1` decimal(15,2) DEFAULT NULL,
  `prix_med_s2` decimal(15,2) DEFAULT NULL,
  `surf_med_s2` decimal(15,2) DEFAULT NULL,
  `prix_m2_w_s2` decimal(15,2) DEFAULT NULL,
  `prix_med_s3` decimal(15,2) DEFAULT NULL,
  `surf_med_s3` decimal(15,2) DEFAULT NULL,
  `prix_m2_w_s3` decimal(15,2) DEFAULT NULL,
  `prix_med_s4` decimal(15,2) DEFAULT NULL,
  `surf_med_s4` decimal(15,2) DEFAULT NULL,
  `prix_m2_w_s4` decimal(15,2) DEFAULT NULL,
  `prix_med_s5` decimal(15,2) DEFAULT NULL,
  `surf_med_s5` decimal(15,2) DEFAULT NULL,
  `prix_m2_w_s5` decimal(15,2) DEFAULT NULL,
  `nb_ventes_var_pct` decimal(9,2) DEFAULT NULL,
  `prix_moyen_var_pct` decimal(9,2) DEFAULT NULL,
  `surface_moyenne_var_pct` decimal(9,2) DEFAULT NULL,
  `prix_moyen_m2_var_pct` decimal(9,2) DEFAULT NULL,
  `prix_m2_mediane_var_pct` decimal(9,2) DEFAULT NULL,
  `surface_mediane_var_pct` decimal(9,2) DEFAULT NULL,
  `last_refreshed` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `ux_communes_agg` (`annee`,`code_dept`,`code_postal`,`commune`,`type_local`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `vf_staging_year`
--

DROP TABLE IF EXISTS `vf_staging_year`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `vf_staging_year` (
  `code_dept` varchar(5) NOT NULL,
  `code_postal` varchar(5) NOT NULL,
  `commune` varchar(100) NOT NULL,
  `type_local` varchar(50) NOT NULL,
  `date_mutation` date NOT NULL,
  `prix_moyen` decimal(15,2) NOT NULL,
  `surface_moyenne` decimal(15,2) NOT NULL,
  KEY `idx_staging_year_group` (`code_dept`,`code_postal`,`commune`,`type_local`),
  KEY `idx_staging_year_date` (`date_mutation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `vf_staging_year_prev`
--

DROP TABLE IF EXISTS `vf_staging_year_prev`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `vf_staging_year_prev` (
  `code_dept` varchar(5) NOT NULL,
  `code_postal` varchar(5) NOT NULL,
  `commune` varchar(100) NOT NULL,
  `type_local` varchar(50) NOT NULL,
  `date_mutation` date NOT NULL,
  `prix_moyen` decimal(15,2) NOT NULL,
  `surface_moyenne` decimal(15,2) NOT NULL,
  KEY `idx_staging_prev_group` (`code_dept`,`code_postal`,`commune`,`type_local`),
  KEY `idx_staging_prev_date` (`date_mutation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary view structure for view `vw_valeursfoncieres_agg_2020`
--

DROP TABLE IF EXISTS `vw_valeursfoncieres_agg_2020`;
/*!50001 DROP VIEW IF EXISTS `vw_valeursfoncieres_agg_2020`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `vw_valeursfoncieres_agg_2020` AS SELECT 
 1 AS `code_dept`,
 1 AS `code_postal`,
 1 AS `commune`,
 1 AS `type_local`,
 1 AS `date_mutation`,
 1 AS `voie`,
 1 AS `no_voie`,
 1 AS `nb_mutations`,
 1 AS `prix_moyen`,
 1 AS `surface_moyenne`,
 1 AS `prix_moyen_m2`,
 1 AS `CURRENT_TIMESTAMP`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `vw_vf_communes`
--

DROP TABLE IF EXISTS `vw_vf_communes`;
/*!50001 DROP VIEW IF EXISTS `vw_vf_communes`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `vw_vf_communes` AS SELECT 
 1 AS `code_dept`,
 1 AS `code_postal`,
 1 AS `commune`,
 1 AS `annee`,
 1 AS `type_local`,
 1 AS `nb_ventes`,
 1 AS `prix_moyen`,
 1 AS `surface_moyenne`,
 1 AS `prix_moyen_m2`*/;
SET character_set_client = @saved_cs_client;

--
-- Final view structure for view `vw_valeursfoncieres_agg_2020`
--

/*!50001 DROP VIEW IF EXISTS `vw_valeursfoncieres_agg_2020`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_valeursfoncieres_agg_2020` AS select `v`.`code_departement` AS `code_dept`,`v`.`code_postal` AS `code_postal`,`v`.`commune` AS `commune`,`v`.`type_local` AS `type_local`,`v`.`date_mutation` AS `date_mutation`,`v`.`voie` AS `voie`,`v`.`no_voie` AS `no_voie`,count(0) AS `nb_mutations`,round(avg(`v`.`valeur_fonciere`),2) AS `prix_moyen`,round(avg(`v`.`surface_reelle_bati`),2) AS `surface_moyenne`,round(avg((`v`.`valeur_fonciere` / nullif(`v`.`surface_reelle_bati`,0))),2) AS `prix_moyen_m2`,now() AS `CURRENT_TIMESTAMP` from `valeursfoncieres` `v` where ((`v`.`date_mutation` >= '2020-01-01') and (`v`.`date_mutation` < '2021-01-01') and (`v`.`nature_mutation` = 'Vente') and (`v`.`type_local` in ('Appartement','Maison'))) group by `code_dept`,`v`.`code_postal`,`v`.`commune`,`v`.`type_local`,`v`.`date_mutation`,`v`.`voie`,`v`.`no_voie` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vw_vf_communes`
--

/*!50001 DROP VIEW IF EXISTS `vw_vf_communes`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_vf_communes` AS select `vav`.`code_dept` AS `code_dept`,`vav`.`code_postal` AS `code_postal`,`vav`.`commune` AS `commune`,`vav`.`annee` AS `annee`,`vav`.`type_local` AS `type_local`,count(`vav`.`nb_mutations`) AS `nb_ventes`,round(avg(`vav`.`prix_moyen`),2) AS `prix_moyen`,round(avg(`vav`.`surface_moyenne`),2) AS `surface_moyenne`,round((sum(`vav`.`prix_moyen`) / nullif(sum(`vav`.`surface_moyenne`),0)),2) AS `prix_moyen_m2` from `vf_all_ventes` `vav` group by `vav`.`code_dept`,`vav`.`code_postal`,`vav`.`commune`,`vav`.`annee`,`vav`.`type_local` order by `vav`.`code_dept`,`vav`.`code_postal`,`vav`.`commune`,`vav`.`annee`,`vav`.`type_local` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-03-05  2:17:09
