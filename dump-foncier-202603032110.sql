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
  `dedup_key_hash` char(64) GENERATED ALWAYS AS (sha2(concat_ws(_utf8mb4'|',coalesce(`code_departement`,_utf8mb4''),coalesce(`code_postal`,_utf8mb4''),coalesce(`nature_mutation`,_utf8mb4''),coalesce(cast(`date_mutation` as char charset utf8mb4),_utf8mb4''),coalesce(`type_local`,_utf8mb4''),coalesce(cast(`valeur_fonciere` as char charset utf8mb4),_utf8mb4''),nullif(trim(`voie`),_utf8mb4''),nullif(trim(`type_de_voie`),_utf8mb4''),nullif(trim(`no_voie`),_utf8mb4''),(case when (`surface_reelle_bati` is null) then NULL else cast(`surface_reelle_bati` as char charset utf8mb4) end)),256)) STORED,
  PRIMARY KEY (`id`,`date_mutation`),
  UNIQUE KEY `ux_ValeursFoncieres_dt_hash` (`date_mutation`,`dedup_key_hash`),
  KEY `idx_ValeursFoncieres_type_local` (`type_local`),
  KEY `idx_ValeursFoncieres_code_departement` (`code_departement`),
  KEY `idx_ValeursFoncieres_code_postal` (`code_postal`),
  KEY `idx_ValeursFoncieres_commune` (`commune`),
  KEY `idx_ValeursFoncieres_date` (`date_mutation`)
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
  UNIQUE KEY `ux_vf_agg` (`annee`,`type_local`,`code_dept`,`code_postal`,`commune`,`date_mutation`,`voie`,`no_voie`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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
-- Dumping routines for database 'foncier'
--
/*!50003 DROP PROCEDURE IF EXISTS `sp_refresh_valeursfoncieres_agg` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_refresh_valeursfoncieres_agg`(IN p_year INT)
BEGIN
  /* Upsert les agrégats quotidiens par dimensions choisies */
  INSERT INTO valeursfoncieres_agg (
      code_dept, code_postal, commune, type_local, 
      annee, date_mutation, voie, no_voie, 
      nb_mutations, prix_moyen, surface_moyenne, prix_moyen_m2, last_refreshed
  )
  SELECT
	  v.code_departement 	as code_dept,
	  v.code_postal 		as code_postal,
	  v.commune 			as commune,
  	  v.type_local 			as type_local,
	  p_year 				as annee, v.date_mutation as date_mutation,
	  v.voie 				as voie,
	  v.no_voie 			as no_voie,
      COUNT(*) 				AS nb_mutations,
      ROUND(AVG(v.valeur_fonciere), 2) AS prix_moyen,
      ROUND(AVG(v.surface_reelle_bati), 2) AS surface_moyenne,
      ROUND(AVG(v.valeur_fonciere / NULLIF(v.surface_reelle_bati, 0)), 2) AS prix_moyen_m2,
      CURRENT_TIMESTAMP
  FROM valeursfoncieres v
  WHERE v.date_mutation >= STR_TO_DATE(CONCAT(p_year, '-01-01'), '%Y-%m-%d')
    AND v.date_mutation <  STR_TO_DATE(CONCAT(p_year+1, '-01-01'), '%Y-%m-%d')
    AND v.nature_mutation = 'Vente'
    AND type_local IN ('Appartement', 'Maison')
  GROUP BY code_dept, code_postal, commune, type_local, date_mutation, voie, no_voie
  ON DUPLICATE KEY UPDATE
      nb_mutations 		= VALUES(nb_mutations),
      prix_moyen 		= VALUES(prix_moyen),
      surface_moyenne 	= VALUES(surface_moyenne),
      prix_moyen_m2 	= VALUES(prix_moyen_m2),
      last_refreshed 	= VALUES(last_refreshed);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sp_refresh_vf_communes_agg` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_refresh_vf_communes_agg`(IN p_year INT)
BEGIN

  /* =============================
     INSERT … SELECT (source = sous-requêtes imbriquées)
     ============================= */
  INSERT INTO vf_communes (
    code_dept, code_postal, commune, annee, type_local,
    nb_ventes, prix_moyen, surface_moyenne, prix_moyen_m2,
    prix_m2_q1, prix_m2_mediane, prix_m2_q3, surface_mediane,
    prix_med_s1, surf_med_s1, prix_m2_w_s1,
    prix_med_s2, surf_med_s2, prix_m2_w_s2,
    prix_med_s3, surf_med_s3, prix_m2_w_s3,
    prix_med_s4, surf_med_s4, prix_m2_w_s4,
    prix_med_s5, surf_med_s5, prix_m2_w_s5,
    nb_ventes_var_pct, prix_moyen_var_pct, surface_moyenne_var_pct,
    prix_moyen_m2_var_pct, prix_m2_mediane_var_pct, surface_mediane_var_pct,
    last_refreshed
  )
  SELECT
    g.code_dept, g.code_postal, g.commune, p_year AS annee, g.type_local,

    /* agrégats globaux année N */
    g.nb_ventes, g.prix_moyen, g.surface_moyenne, g.prix_moyen_m2,

    /* quartiles p2m + médiane surface année N */
    q.prix_m2_q1, q.prix_m2_mediane, q.prix_m2_q3, ms.surface_mediane,

    /* tranches surface (médianes + p2m pondéré) année N */
    pvt.prix_med_s1, pvt.surf_med_s1, pvt.prix_m2_w_s1,
    pvt.prix_med_s2, pvt.surf_med_s2, pvt.prix_m2_w_s2,
    pvt.prix_med_s3, pvt.surf_med_s3, pvt.prix_m2_w_s3,
    pvt.prix_med_s4, pvt.surf_med_s4, pvt.prix_m2_w_s4,
    pvt.prix_med_s5, pvt.surf_med_s5, pvt.prix_m2_w_s5,

    /* variations N vs N-1 (en %) */
    ROUND((g.nb_ventes       - IFNULL(p.nb_ventes_prev,0))        / NULLIF(p.nb_ventes_prev,0)        * 100, 2) AS nb_ventes_var_pct,
    ROUND((g.prix_moyen      - IFNULL(p.prix_moyen_prev,0))       / NULLIF(p.prix_moyen_prev,0)       * 100, 2) AS prix_moyen_var_pct,
    ROUND((g.surface_moyenne - IFNULL(p.surface_moyenne_prev,0))  / NULLIF(p.surface_moyenne_prev,0)  * 100, 2) AS surface_moyenne_var_pct,
    ROUND((g.prix_moyen_m2   - IFNULL(p.prix_moyen_m2_prev,0))    / NULLIF(p.prix_moyen_m2_prev,0)    * 100, 2) AS prix_moyen_m2_var_pct,
    ROUND((q.prix_m2_mediane - IFNULL(p.prix_m2_mediane_prev,0))  / NULLIF(p.prix_m2_mediane_prev,0)  * 100, 2) AS prix_m2_mediane_var_pct,
    ROUND((ms.surface_mediane- IFNULL(p.surface_mediane_prev,0))  / NULLIF(p.surface_mediane_prev,0)  * 100, 2) AS surface_mediane_var_pct,

    CURRENT_TIMESTAMP AS last_refreshed

  FROM
  /* ======== Agrégats globaux (N) ======== */
  (
    SELECT
      b.code_dept, b.code_postal, b.commune, b.type_local,
      COUNT(*)                               AS nb_ventes,
      ROUND(AVG(b.prix),  2)                 AS prix_moyen,
      ROUND(AVG(b.surf),  2)                 AS surface_moyenne,
      ROUND(SUM(b.prix)/NULLIF(SUM(b.surf),0), 2) AS prix_moyen_m2
    FROM (
      SELECT
        v.code_dept,
        v.code_postal,
        v.commune,
        v.type_local,
        v.prix_moyen      AS prix,
        v.surface_moyenne AS surf
      FROM vf_all_ventes v
      WHERE v.date_mutation >= CONCAT(p_year,   '-01-01')
        AND v.date_mutation <  CONCAT(p_year+1, '-01-01')
        AND v.type_local IN ('Appartement','Maison')
        AND v.prix_moyen > 0
        AND v.surface_moyenne > 0
    ) AS b
    GROUP BY b.code_dept, b.code_postal, b.commune, b.type_local
  ) AS g

  /* ======== Quartiles p2m (N) ======== */
  LEFT JOIN
  (
    /* interpolation “continuous” sur p2m = prix/surf */
    SELECT
      z.code_dept, z.code_postal, z.commune, z.type_local,
      ROUND((1-z.q1_f)*p2m_q1_lo + z.q1_f*p2m_q1_hi, 2) AS prix_m2_q1,
      ROUND((1-z.q2_f)*p2m_q2_lo + z.q2_f*p2m_q2_hi, 2) AS prix_m2_mediane,
      ROUND((1-z.q3_f)*p2m_q3_lo + z.q3_f*p2m_q3_hi, 2) AS prix_m2_q3
    FROM (
      SELECT
        x.code_dept, x.code_postal, x.commune, x.type_local, x.n,
        (x.n-1)*0.25+1 AS q1_pos, (x.n-1)*0.50+1 AS q2_pos, (x.n-1)*0.75+1 AS q3_pos,
        ( (x.n-1)*0.25+1 - FLOOR((x.n-1)*0.25+1) ) AS q1_f,
        ( (x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1) ) AS q2_f,
        ( (x.n-1)*0.75+1 - FLOOR((x.n-1)*0.75+1) ) AS q3_f,
        /* valeurs aux rangs lo/hi pour q1, q2, q3 */
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.25+1) THEN x.p2m END) AS p2m_q1_lo,
        MAX(CASE WHEN x.rn = CEIL ((x.n-1)*0.25+1) THEN x.p2m END) AS p2m_q1_hi,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_q2_lo,
        MAX(CASE WHEN x.rn = CEIL ((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_q2_hi,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.75+1) THEN x.p2m END) AS p2m_q3_lo,
        MAX(CASE WHEN x.rn = CEIL ((x.n-1)*0.75+1) THEN x.p2m END) AS p2m_q3_hi
      FROM (
        SELECT
          b2.code_dept, b2.code_postal, b2.commune, b2.type_local,
          (b2.prix / b2.surf) AS p2m,
          ROW_NUMBER() OVER (
            PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local
            ORDER BY (b2.prix / b2.surf)
          ) AS rn,
          COUNT(*) OVER (
            PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local
          ) AS n
        FROM (
          SELECT
            v.code_dept, v.code_postal, v.commune, v.type_local,
            v.prix_moyen AS prix, v.surface_moyenne AS surf
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year,   '-01-01')
            AND v.date_mutation <  CONCAT(p_year+1, '-01-01')
            AND v.type_local IN ('Appartement','Maison')
            AND v.prix_moyen > 0 AND v.surface_moyenne > 0
        ) AS b2
      ) AS x
      GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
    ) AS z
  ) AS q
    ON q.code_dept=g.code_dept AND q.code_postal=g.code_postal
   AND q.commune=g.commune     AND q.type_local=g.type_local

  /* ======== Médiane de surface (N) ======== */
  LEFT JOIN
  (
    SELECT
      y.code_dept, y.code_postal, y.commune, y.type_local,
      ROUND((1-y.f)*surf_lo + y.f*surf_hi, 2) AS surface_mediane
    FROM (
      SELECT
        x.code_dept, x.code_postal, x.commune, x.type_local, x.n,
        (x.n-1)*0.50+1 AS pos,
        ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.surf END) AS surf_lo,
        MAX(CASE WHEN x.rn = CEIL ((x.n-1)*0.50+1) THEN x.surf END) AS surf_hi
      FROM (
        SELECT
          b3.code_dept, b3.code_postal, b3.commune, b3.type_local, b3.surf,
          ROW_NUMBER() OVER (
            PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local
            ORDER BY b3.surf
          ) AS rn,
          COUNT(*) OVER (
            PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local
          ) AS n
        FROM (
          SELECT
            v.code_dept, v.code_postal, v.commune, v.type_local,
            v.surface_moyenne AS surf
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year,   '-01-01')
            AND v.date_mutation <  CONCAT(p_year+1, '-01-01')
            AND v.type_local IN ('Appartement','Maison')
            AND v.surface_moyenne > 0
        ) AS b3
      ) AS x
      GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
    ) AS y
  ) AS ms
    ON ms.code_dept=g.code_dept AND ms.code_postal=g.code_postal
   AND ms.commune=g.commune     AND ms.type_local=g.type_local

  /* ======== Tranches surface (N) : médianes + p2m pondéré ======== */
  LEFT JOIN
  (
    SELECT
      code_dept, code_postal, commune, type_local,
      MAX(CASE WHEN bucket='S1' THEN prix_med END)  AS prix_med_s1,
      MAX(CASE WHEN bucket='S1' THEN surf_med END)  AS surf_med_s1,
      MAX(CASE WHEN bucket='S1' THEN p2m_w   END)  AS prix_m2_w_s1,
      MAX(CASE WHEN bucket='S2' THEN prix_med END)  AS prix_med_s2,
      MAX(CASE WHEN bucket='S2' THEN surf_med END)  AS surf_med_s2,
      MAX(CASE WHEN bucket='S2' THEN p2m_w   END)  AS prix_m2_w_s2,
      MAX(CASE WHEN bucket='S3' THEN prix_med END)  AS prix_med_s3,
      MAX(CASE WHEN bucket='S3' THEN surf_med END)  AS surf_med_s3,
      MAX(CASE WHEN bucket='S3' THEN p2m_w   END)  AS prix_m2_w_s3,
      MAX(CASE WHEN bucket='S4' THEN prix_med END)  AS prix_med_s4,
      MAX(CASE WHEN bucket='S4' THEN surf_med END)  AS surf_med_s4,
      MAX(CASE WHEN bucket='S4' THEN p2m_w   END)  AS prix_m2_w_s4,
      MAX(CASE WHEN bucket='S5' THEN prix_med END)  AS prix_med_s5,
      MAX(CASE WHEN bucket='S5' THEN surf_med END)  AS surf_med_s5,
      MAX(CASE WHEN bucket='S5' THEN p2m_w   END)  AS prix_m2_w_s5
    FROM (
      /* calcul par bucket */
      SELECT
        t.code_dept, t.code_postal, t.commune, t.type_local, t.bucket,
        /* médianes via interpolation */
        ROUND((1-t.f_prix)*prix_lo + t.f_prix*prix_hi, 2) AS prix_med,
        ROUND((1-t.f_surf)*surf_lo + t.f_surf*surf_hi, 2) AS surf_med,
        /* p2m pondéré surface */
        ROUND(t.sum_prix/NULLIF(t.sum_surf,0), 2) AS p2m_w
      FROM (
        /* pré-calcul par bucket : rangs & sommes */
        SELECT
          b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket,
          /* sommes pour p2m pondéré */
          SUM(b4.prix) AS sum_prix, SUM(b4.surf) AS sum_surf,
          /* rangs pour médianes prix/surface */
          /* prix */
          ROW_NUMBER() OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket
                             ORDER BY b4.prix) AS rn_prix,
          COUNT(*) OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket) AS n_prix,
          /* surf */
          ROW_NUMBER() OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket
                             ORDER BY b4.surf) AS rn_surf,
          COUNT(*) OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket) AS n_surf,
          b4.prix, b4.surf
        FROM (
          SELECT
            v.code_dept, v.code_postal, v.commune, v.type_local,
            v.prix_moyen AS prix, v.surface_moyenne AS surf,
            CASE
              WHEN v.surface_moyenne BETWEEN  0 AND 25 THEN 'S1'
              WHEN v.surface_moyenne BETWEEN 26 AND 35 THEN 'S2'
              WHEN v.surface_moyenne BETWEEN 36 AND 55 THEN 'S3'
              WHEN v.surface_moyenne BETWEEN 56 AND 85 THEN 'S4'
              WHEN v.surface_moyenne >= 86             THEN 'S5'
              ELSE NULL
            END AS bucket
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year,   '-01-01')
            AND v.date_mutation <  CONCAT(p_year+1, '-01-01')
            AND v.type_local IN ('Appartement','Maison')
            AND v.prix_moyen > 0 AND v.surface_moyenne > 0
        ) AS b4
        WHERE b4.bucket IS NOT NULL
      ) AS s
      /* interpolation médianes : on regroupe par clé + bucket */
      JOIN (
        SELECT
          code_dept, code_postal, commune, type_local, bucket,
          /* prix : positions + fractions */
          (n_prix-1)*0.50+1 AS pos_prix,
          ((n_prix-1)*0.50+1 - FLOOR((n_prix-1)*0.50+1)) AS f_prix,
          /* surf : positions + fractions */
          (n_surf-1)*0.50+1 AS pos_surf,
          ((n_surf-1)*0.50+1 - FLOOR((n_surf-1)*0.50+1)) AS f_surf,
          /* valeurs aux rangs planchers/plafonds — récupérées via agrégats conditionnels */
          MAX(CASE WHEN rn_prix = FLOOR((n_prix-1)*0.50+1) THEN prix END) AS prix_lo,
          MAX(CASE WHEN rn_prix = CEIL ((n_prix-1)*0.50+1) THEN prix END) AS prix_hi,
          MAX(CASE WHEN rn_surf = FLOOR((n_surf-1)*0.50+1) THEN surf END) AS surf_lo,
          MAX(CASE WHEN rn_surf = CEIL ((n_surf-1)*0.50+1) THEN surf END) AS surf_hi,
          /* sommes pour p2m pondéré (reprises ensuite) */
          MAX(sum_prix) AS sum_prix, MAX(sum_surf) AS sum_surf
        FROM (
          SELECT
            b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket,
            ROW_NUMBER() OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket
                               ORDER BY b4.prix) AS rn_prix,
            COUNT(*) OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket) AS n_prix,
            ROW_NUMBER() OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket
                               ORDER BY b4.surf) AS rn_surf,
            COUNT(*) OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket) AS n_surf,
            b4.prix, b4.surf,
            SUM(b4.prix) OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket) AS sum_prix,
            SUM(b4.surf) OVER (PARTITION BY b4.code_dept, b4.code_postal, b4.commune, b4.type_local, b4.bucket) AS sum_surf
          FROM (
            SELECT
              v.code_dept, v.code_postal, v.commune, v.type_local,
              v.prix_moyen AS prix, v.surface_moyenne AS surf,
              CASE
                WHEN v.surface_moyenne BETWEEN  0 AND 25 THEN 'S1'
                WHEN v.surface_moyenne BETWEEN 26 AND 35 THEN 'S2'
                WHEN v.surface_moyenne BETWEEN 36 AND 55 THEN 'S3'
                WHEN v.surface_moyenne BETWEEN 56 AND 85 THEN 'S4'
                WHEN v.surface_moyenne >= 86             THEN 'S5'
                ELSE NULL
              END AS bucket
            FROM vf_all_ventes v
            WHERE v.date_mutation >= CONCAT(p_year,   '-01-01')
              AND v.date_mutation <  CONCAT(p_year+1, '-01-01')
              AND v.type_local IN ('Appartement','Maison')
              AND v.prix_moyen > 0 AND v.surface_moyenne > 0
          ) AS b4
          WHERE b4.bucket IS NOT NULL
        ) AS r
        GROUP BY code_dept, code_postal, commune, type_local, bucket, n_prix, n_surf
      ) AS t
        ON t.code_dept=s.code_dept AND t.code_postal=s.code_postal
       AND t.commune=s.commune     AND t.type_local=s.type_local
       AND t.bucket = s.bucket
    ) AS allb
    GROUP BY code_dept, code_postal, commune, type_local
  ) AS pvt
    ON pvt.code_dept=g.code_dept AND pvt.code_postal=g.code_postal
   AND pvt.commune=g.commune     AND pvt.type_local=g.type_local

  /* ======== Agrégats année N-1 pour variations ======== */
  LEFT JOIN
  (
    SELECT
      a.code_dept, a.code_postal, a.commune, a.type_local,
      a.nb_ventes_prev, a.prix_moyen_prev, a.surface_moyenne_prev, a.prix_moyen_m2_prev,
      mpr.prix_m2_mediane_prev, msr.surface_mediane_prev
    FROM
    (
      SELECT
        b.code_dept, b.code_postal, b.commune, b.type_local,
        COUNT(*)                               AS nb_ventes_prev,
        ROUND(AVG(b.prix),  2)                 AS prix_moyen_prev,
        ROUND(AVG(b.surf),  2)                 AS surface_moyenne_prev,
        ROUND(SUM(b.prix)/NULLIF(SUM(b.surf),0), 2) AS prix_moyen_m2_prev
      FROM (
        SELECT
          v.code_dept, v.code_postal, v.commune, v.type_local,
          v.prix_moyen AS prix, v.surface_moyenne AS surf
        FROM vf_all_ventes v
        WHERE v.date_mutation >= CONCAT(p_year-1, '-01-01')
          AND v.date_mutation <  CONCAT(p_year,   '-01-01')
          AND v.type_local IN ('Appartement','Maison')
          AND v.prix_moyen > 0 AND v.surface_moyenne > 0
      ) AS b
      GROUP BY b.code_dept, b.code_postal, b.commune, b.type_local
    ) AS a
    /* médiane p2m N-1 */
    LEFT JOIN (
      SELECT
        z.code_dept, z.code_postal, z.commune, z.type_local,
        ROUND((1-z.f)*p2m_lo + z.f*p2m_hi, 2) AS prix_m2_mediane_prev
      FROM (
        SELECT
          x.code_dept, x.code_postal, x.commune, x.type_local, x.n,
          (x.n-1)*0.50+1 AS pos,
          ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
          MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_lo,
          MAX(CASE WHEN x.rn = CEIL ((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_hi
        FROM (
          SELECT
            b2.code_dept, b2.code_postal, b2.commune, b2.type_local,
            (b2.prix / b2.surf) AS p2m,
            ROW_NUMBER() OVER (
              PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local
              ORDER BY (b2.prix / b2.surf)
            ) AS rn,
            COUNT(*) OVER (
              PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local
            ) AS n
          FROM (
            SELECT
              v.code_dept, v.code_postal, v.commune, v.type_local,
              v.prix_moyen AS prix, v.surface_moyenne AS surf
            FROM vf_all_ventes v
            WHERE v.date_mutation >= CONCAT(p_year-1, '-01-01')
              AND v.date_mutation <  CONCAT(p_year,   '-01-01')
              AND v.type_local IN ('Appartement','Maison')
              AND v.prix_moyen > 0 AND v.surface_moyenne > 0
          ) AS b2
        ) AS x
        GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
      ) AS z
    ) AS mpr
      ON mpr.code_dept=a.code_dept AND mpr.code_postal=a.code_postal
     AND mpr.commune=a.commune     AND mpr.type_local=a.type_local
    /* médiane surface N-1 */
    LEFT JOIN (
      SELECT
        y.code_dept, y.code_postal, y.commune, y.type_local,
        ROUND((1-y.f)*surf_lo + y.f*surf_hi, 2) AS surface_mediane_prev
      FROM (
        SELECT
          x.code_dept, x.code_postal, x.commune, x.type_local, x.n,
          (x.n-1)*0.50+1 AS pos,
          ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
          MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.surf END) AS surf_lo,
          MAX(CASE WHEN x.rn = CEIL ((x.n-1)*0.50+1) THEN x.surf END) AS surf_hi
        FROM (
          SELECT
            b3.code_dept, b3.code_postal, b3.commune, b3.type_local, b3.surf,
            ROW_NUMBER() OVER (
              PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local
              ORDER BY b3.surf
            ) AS rn,
            COUNT(*) OVER (
              PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local
            ) AS n
          FROM (
            SELECT
              v.code_dept, v.code_postal, v.commune, v.type_local,
              v.surface_moyenne AS surf
            FROM vf_all_ventes v
            WHERE v.date_mutation >= CONCAT(p_year-1, '-01-01')
              AND v.date_mutation <  CONCAT(p_year,   '-01-01')
              AND v.type_local IN ('Appartement','Maison')
              AND v.surface_moyenne > 0
          ) AS b3
        ) AS x
        GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
      ) AS y
    ) AS msr
      ON msr.code_dept=a.code_dept AND msr.code_postal=a.code_postal
     AND msr.commune=a.commune     AND msr.type_local=a.type_local
  ) AS p
    ON p.code_dept=g.code_dept AND p.code_postal=g.code_postal
   AND p.commune=g.commune     AND p.type_local=g.type_local

  ON DUPLICATE KEY UPDATE
    nb_ventes        = VALUES(nb_ventes),
    prix_moyen       = VALUES(prix_moyen),
    surface_moyenne  = VALUES(surface_moyenne),
    prix_moyen_m2    = VALUES(prix_moyen_m2),
    prix_m2_q1       = VALUES(prix_m2_q1),
    prix_m2_mediane  = VALUES(prix_m2_mediane),
    prix_m2_q3       = VALUES(prix_m2_q3),
    surface_mediane  = VALUES(surface_mediane),
    prix_med_s1      = VALUES(prix_med_s1),
    surf_med_s1      = VALUES(surf_med_s1),
    prix_m2_w_s1     = VALUES(prix_m2_w_s1),
    prix_med_s2      = VALUES(prix_med_s2),
    surf_med_s2      = VALUES(surf_med_s2),
    prix_m2_w_s2     = VALUES(prix_m2_w_s2),
    prix_med_s3      = VALUES(prix_med_s3),
    surf_med_s3      = VALUES(surf_med_s3),
    prix_m2_w_s3     = VALUES(prix_m2_w_s3),
    prix_med_s4      = VALUES(prix_med_s4),
    surf_med_s4      = VALUES(surf_med_s4),
    prix_m2_w_s4     = VALUES(prix_m2_w_s4),
    prix_med_s5      = VALUES(prix_med_s5),
    surf_med_s5      = VALUES(surf_med_s5),
    prix_m2_w_s5     = VALUES(prix_m2_w_s5),
    nb_ventes_var_pct       = VALUES(nb_ventes_var_pct),
    prix_moyen_var_pct      = VALUES(prix_moyen_var_pct),
    surface_moyenne_var_pct = VALUES(surface_moyenne_var_pct),
    prix_moyen_m2_var_pct   = VALUES(prix_moyen_m2_var_pct),
    prix_m2_mediane_var_pct = VALUES(prix_m2_mediane_var_pct),
    surface_mediane_var_pct = VALUES(surface_mediane_var_pct),
    last_refreshed          = VALUES(last_refreshed);

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

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

-- Dump completed on 2026-03-03 21:10:44
