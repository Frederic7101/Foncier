-- Schéma 'encheres' pour les données Licitor (ventes aux enchères immobilières).
-- Base de données : foncier (même serveur que ventes_notaire).
-- Sous MySQL, un schéma = une base : on crée la base 'encheres'.

CREATE DATABASE IF NOT EXISTS `encheres`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE `encheres`;

-- Table des annonces Licitor (une ligne par annonce, dédoublonnée par url)
CREATE TABLE IF NOT EXISTS `licitor_annonces` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `url` varchar(512) NOT NULL,
  `region` varchar(100) DEFAULT NULL,
  `departement` varchar(20) DEFAULT NULL,
  `ville` varchar(255) DEFAULT NULL,
  `description` text,
  `prix` varchar(50) DEFAULT NULL COMMENT 'Mise à prix (texte brut)',
  `date_publication` varchar(100) DEFAULT NULL,
  `date_adjudication` varchar(100) DEFAULT NULL,
  `date_scraping` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_licitor_url` (`url`),
  KEY `idx_licitor_region` (`region`),
  KEY `idx_licitor_departement` (`departement`),
  KEY `idx_licitor_ville` (`ville`),
  KEY `idx_licitor_date_scraping` (`date_scraping`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
