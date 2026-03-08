-- Tables de référence région / département pour rattacher correctement chaque département à sa région.
-- Utilisées par l'API geo et les stats (ex. département 38 → Auvergne-Rhône-Alpes).
-- Enregistrer ce fichier en UTF-8. Exécuter avec : mysql --default-character-set=utf8mb4 -u ... -p foncier < create_ref_departements.sql

SET NAMES 'utf8mb4';
SET character_set_client = utf8mb4;
SET character_set_connection = utf8mb4;
SET character_set_results = utf8mb4;

-- Script réentrant : supprimer les tables si elles existent (enfant puis parent)
DROP TABLE IF EXISTS ref_departements;
DROP TABLE IF EXISTS ref_regions;

-- Régions métropolitaines (code INSEE région, nom)
CREATE TABLE ref_regions (
  code_region VARCHAR(5)   NOT NULL,
  nom_region  VARCHAR(100) NOT NULL,
  PRIMARY KEY (code_region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Départements par région (code département → code région, nom)
CREATE TABLE ref_departements (
  code_dept   VARCHAR(5)   NOT NULL,
  code_region VARCHAR(5)   NOT NULL,
  nom_dept    VARCHAR(80)  NOT NULL,
  PRIMARY KEY (code_dept),
  KEY idx_code_region (code_region),
  CONSTRAINT fk_ref_dept_region FOREIGN KEY (code_region) REFERENCES ref_regions (code_region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Données : régions
INSERT INTO ref_regions (code_region, nom_region) VALUES
('11', 'Île-de-France'),
('24', 'Centre-Val de Loire'),
('27', 'Bourgogne-Franche-Comté'),
('28', 'Normandie'),
('32', 'Hauts-de-France'),
('44', 'Grand Est'),
('52', 'Pays de la Loire'),
('53', 'Bretagne'),
('75', 'Nouvelle-Aquitaine'),
('76', 'Occitanie'),
('84', 'Auvergne-Rhône-Alpes'),
('93', 'Provence-Alpes-Côte d''Azur'),
('94', 'Corse');

-- Données : départements par région
INSERT INTO ref_departements (code_dept, code_region, nom_dept) VALUES
('75', '11', 'Paris'),
('77', '11', 'Seine-et-Marne'),
('78', '11', 'Yvelines'),
('91', '11', 'Essonne'),
('92', '11', 'Hauts-de-Seine'),
('93', '11', 'Seine-Saint-Denis'),
('94', '11', 'Val-de-Marne'),
('18', '24', 'Cher'),
('28', '24', 'Eure-et-Loir'),
('36', '24', 'Indre'),
('37', '24', 'Indre-et-Loire'),
('41', '24', 'Loir-et-Cher'),
('45', '24', 'Loiret'),
('21', '27', 'Côte-d''Or'),
('25', '27', 'Doubs'),
('39', '27', 'Jura'),
('58', '27', 'Nièvre'),
('70', '27', 'Haute-Saône'),
('71', '27', 'Saône-et-Loire'),
('89', '27', 'Yonne'),
('90', '27', 'Territoire de Belfort'),
('14', '28', 'Calvados'),
('27', '28', 'Eure'),
('50', '28', 'Manche'),
('61', '28', 'Orne'),
('76', '28', 'Seine-Maritime'),
('02', '32', 'Aisne'),
('59', '32', 'Nord'),
('60', '32', 'Oise'),
('62', '32', 'Pas-de-Calais'),
('80', '32', 'Somme'),
('08', '44', 'Ardennes'),
('10', '44', 'Aube'),
('51', '44', 'Marne'),
('52', '44', 'Haute-Marne'),
('54', '44', 'Meurthe-et-Moselle'),
('55', '44', 'Meuse'),
('57', '44', 'Moselle'),
('67', '44', 'Bas-Rhin'),
('68', '44', 'Haut-Rhin'),
('88', '44', 'Vosges'),
('44', '52', 'Loire-Atlantique'),
('49', '52', 'Maine-et-Loire'),
('53', '52', 'Mayenne'),
('72', '52', 'Sarthe'),
('85', '52', 'Vendée'),
('22', '53', 'Côtes-d''Armor'),
('29', '53', 'Finistère'),
('35', '53', 'Ille-et-Vilaine'),
('56', '53', 'Morbihan'),
('16', '75', 'Charente'),
('17', '75', 'Charente-Maritime'),
('19', '75', 'Corrèze'),
('23', '75', 'Creuse'),
('24', '75', 'Dordogne'),
('33', '75', 'Gironde'),
('40', '75', 'Landes'),
('47', '75', 'Lot-et-Garonne'),
('64', '75', 'Pyrénées-Atlantiques'),
('79', '75', 'Deux-Sèvres'),
('86', '75', 'Vienne'),
('87', '75', 'Haute-Vienne'),
('09', '76', 'Ariège'),
('11', '76', 'Aude'),
('12', '76', 'Aveyron'),
('30', '76', 'Gard'),
('31', '76', 'Haute-Garonne'),
('32', '76', 'Gers'),
('34', '76', 'Hérault'),
('46', '76', 'Lot'),
('48', '76', 'Lozère'),
('65', '76', 'Hautes-Pyrénées'),
('66', '76', 'Pyrénées-Orientales'),
('81', '76', 'Tarn'),
('82', '76', 'Tarn-et-Garonne'),
('01', '84', 'Ain'),
('03', '84', 'Allier'),
('07', '84', 'Ardèche'),
('15', '84', 'Cantal'),
('26', '84', 'Drôme'),
('38', '84', 'Isère'),
('42', '84', 'Loire'),
('43', '84', 'Haute-Loire'),
('63', '84', 'Puy-de-Dôme'),
('69', '84', 'Rhône'),
('73', '84', 'Savoie'),
('74', '84', 'Haute-Savoie'),
('06', '93', 'Alpes-Maritimes'),
('13', '93', 'Bouches-du-Rhône'),
('83', '93', 'Var'),
('84', '93', 'Vaucluse'),
('2A', '94', 'Corse-du-Sud'),
('2B', '94', 'Haute-Corse');
