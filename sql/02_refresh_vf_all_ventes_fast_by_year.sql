-- Étape 2 (version rapide, par année/batch) : refresh vf_all_ventes via procédure stockée.
-- Staging sans index, INSERT par année (partition pruning) et optionnellement par batch de N mois.
--
-- Procédures :
--   sp_refresh_vf_all_ventes_year(p_annee, p_nb_mois_par_batch)
--     Insère une année dans vf_all_ventes_new (table doit exister).
--     p_nb_mois_par_batch : 12 = une seule insertion par annee, 1 = 12 insertions (mois par mois), 3 = 4 insertions (trimestres).
--
--   sp_refresh_vf_all_ventes_fast(p_annee_min, p_annee_max, p_nb_mois_par_batch)
--     Refresh complet : recrée la staging, remplit par années puis batch de mois, ajoute les index, swap avec vf_all_ventes.
--
-- Exemples :
--   CALL sp_refresh_vf_all_ventes_fast(2014, 2025, 12);   -- une insertion par année
--   CALL sp_refresh_vf_all_ventes_fast(2014, 2025, 1);    -- mois par mois (petits lots)
--   CALL sp_refresh_vf_all_ventes_year(2024, 3);          -- une seule année, par trimestre (si staging existe)

DELIMITER $$

-- Insère une année dans vf_all_ventes_new (staging sans index).
-- p_nb_mois_par_batch : 12 = toute l annee en un bloc, 1 = 12 blocs (mois), 3 = 4 blocs (trimestres), etc.
DROP PROCEDURE IF EXISTS sp_refresh_vf_all_ventes_year$$

CREATE PROCEDURE sp_refresh_vf_all_ventes_year(
  IN p_annee              INT UNSIGNED,
  IN p_nb_mois_par_batch   TINYINT UNSIGNED
)
BEGIN
  DECLARE v_mois      TINYINT UNSIGNED DEFAULT 1;
  DECLARE v_date_deb  DATE;
  DECLARE v_date_fin  DATE;

  IF p_nb_mois_par_batch IS NULL OR p_nb_mois_par_batch = 0 THEN
    SET p_nb_mois_par_batch = 12;
  END IF;
  IF p_nb_mois_par_batch > 12 THEN
    SET p_nb_mois_par_batch = 12;
  END IF;

  WHILE v_mois <= 12 DO
    SET v_date_deb = MAKEDATE(p_annee, 1) + INTERVAL (v_mois - 1) MONTH;
    SET v_date_fin = v_date_deb + INTERVAL p_nb_mois_par_batch MONTH;
    -- Ne pas depasser la fin de l annee
    IF v_date_fin > MAKEDATE(p_annee + 1, 1) THEN
      SET v_date_fin = MAKEDATE(p_annee + 1, 1);
    END IF;

    INSERT INTO vf_all_ventes_new (
      code_dept, code_postal, commune, type_local, annee, date_mutation,
      adresse_norm, voie, no_voie, nb_lignes, valeur_fonciere, surface_reelle_bati,
      prix_m2, nombre_pieces_principales, latitude, longitude
    )
    SELECT
      v.code_departement,
      v.code_postal,
      v.commune,
      v.type_local,
      YEAR(v.date_mutation) AS annee,
      v.date_mutation,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm,
      MAX(v.voie)           AS voie,
      MAX(v.no_voie)        AS no_voie,
      COUNT(*)              AS nb_lignes,
      ROUND(AVG(v.valeur_fonciere), 2) AS valeur_fonciere,
      ROUND(AVG(v.surface_reelle_bati), 2) AS surface_reelle_bati,
      ROUND(SUM(v.valeur_fonciere) / NULLIF(SUM(v.surface_reelle_bati), 0), 2) AS prix_m2,
      ROUND(AVG(v.nombre_pieces_principales), 0) AS nombre_pieces_principales,
      MAX(v.latitude)       AS latitude,
      MAX(v.longitude)      AS longitude
    FROM valeursfoncieres v
    WHERE v.nature_mutation = 'Vente'
      AND v.type_local IN ('Appartement', 'Maison')
      AND v.date_mutation >= v_date_deb
      AND v.date_mutation < v_date_fin
    GROUP BY
      v.code_departement,
      v.code_postal,
      v.commune,
      v.type_local,
      v.date_mutation,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune);

    SET v_mois = v_mois + p_nb_mois_par_batch;
  END WHILE;
END$$

-- Refresh complet : staging, remplissage par années (et par batch de mois), index, swap.
DROP PROCEDURE IF EXISTS sp_refresh_vf_all_ventes_fast$$

CREATE PROCEDURE sp_refresh_vf_all_ventes_fast(
  IN p_annee_min           INT UNSIGNED,
  IN p_annee_max           INT UNSIGNED,
  IN p_nb_mois_par_batch   TINYINT UNSIGNED
)
BEGIN
  DECLARE v_annee     INT UNSIGNED;
  DECLARE v_tbl_count INT;

  IF p_nb_mois_par_batch IS NULL OR p_nb_mois_par_batch = 0 THEN
    SET p_nb_mois_par_batch = 12;
  END IF;
  IF p_nb_mois_par_batch > 12 THEN
    SET p_nb_mois_par_batch = 12;
  END IF;
  IF p_annee_min > p_annee_max THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_annee_min doit être <= p_annee_max';
  END IF;

  -- 1) Recréer la table de staging (sans index)
  DROP TABLE IF EXISTS vf_all_ventes_new;
  CREATE TABLE vf_all_ventes_new (
    code_dept              VARCHAR(5)   NOT NULL,
    code_postal            VARCHAR(10)  NOT NULL,
    commune                VARCHAR(100) NOT NULL,
    type_local             VARCHAR(50)  NOT NULL,
    annee                  INT          NOT NULL,
    date_mutation          DATE         NOT NULL,
    adresse_norm           VARCHAR(512) DEFAULT NULL,
    voie                   VARCHAR(255) DEFAULT NULL,
    no_voie                VARCHAR(20)  DEFAULT NULL,
    nb_lignes              INT          NOT NULL,
    valeur_fonciere        DECIMAL(15,2) DEFAULT NULL,
    surface_reelle_bati    DECIMAL(15,2) DEFAULT NULL,
    prix_m2                DECIMAL(15,2) DEFAULT NULL,
    nombre_pieces_principales INT       DEFAULT NULL,
    latitude               DOUBLE       DEFAULT NULL,
    longitude              DOUBLE       DEFAULT NULL,
    last_refreshed         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

  -- 2) Remplir par année (chaque année via sp_refresh_vf_all_ventes_year)
  SET v_annee = p_annee_min;
  WHILE v_annee <= p_annee_max DO
    CALL sp_refresh_vf_all_ventes_year(v_annee, p_nb_mois_par_batch);
    SET v_annee = v_annee + 1;
  END WHILE;

  -- 3) Ajouter les index
  ALTER TABLE vf_all_ventes_new
    ADD UNIQUE KEY ux_vf_agg (annee, type_local, code_dept, code_postal, commune, date_mutation, adresse_norm(255)),
    ADD KEY idx_vf_all_date_type_commune (date_mutation, type_local, code_dept, code_postal, commune),
    ADD KEY idx_vf_all_annee_type_commune (annee, type_local, code_dept, code_postal, commune);

  -- 4) Swap : remplacer vf_all_ventes par la staging (ou créer si première fois)
  SELECT COUNT(*) INTO v_tbl_count
  FROM information_schema.TABLES
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'vf_all_ventes';

  IF v_tbl_count > 0 THEN
    RENAME TABLE vf_all_ventes TO vf_all_ventes_old, vf_all_ventes_new TO vf_all_ventes;
    DROP TABLE vf_all_ventes_old;
  ELSE
    RENAME TABLE vf_all_ventes_new TO vf_all_ventes;
  END IF;
END$$

DELIMITER ;
