-- Procédure stockée : pré-remplissage adresses_geocodees pour une année, par lots.
-- Équivalent de init_adresses_geocodees_by_year.sql pour l'année donnée, avec boucle par batchs.
--
-- Paramètres:
--   p_year        : année (ex. 2020 → plage 2020-01-01 à 2021-01-01)
--   p_batch_size  : nombre max de lignes insérées par itération (ex. 25000)
--
-- Exemple d'appel :
--   CALL sp_init_adresses_geocodees_by_year(2020, 25000);
--   CALL sp_init_adresses_geocodees_by_year(2021, 10000);
--
-- Arrêter geocode_ban.py pendant l'exécution pour limiter les conflits de verrous.
-- Optionnel : SET SESSION innodb_lock_wait_timeout = 120;

DELIMITER //

DROP PROCEDURE IF EXISTS sp_init_adresses_geocodees_by_year//

CREATE PROCEDURE sp_init_adresses_geocodees_by_year(
  IN p_year        INT UNSIGNED,
  IN p_batch_size  INT UNSIGNED
)
BEGIN
  DECLARE v_rows       INT DEFAULT 1;
  DECLARE v_iteration  INT UNSIGNED DEFAULT 0;
  DECLARE v_total_rows INT UNSIGNED DEFAULT 0;
  DECLARE v_date_min   VARCHAR(10);
  DECLARE v_date_max   VARCHAR(10);

  SET v_date_min = CONCAT(p_year, '-01-01');
  SET v_date_max = CONCAT(p_year + 1, '-01-01');

  IF p_batch_size = 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_batch_size doit être > 0';
  END IF;

  SELECT CONCAT('Année ', p_year, ' : démarrage (batch ', p_batch_size, ')') AS progression;

  read_loop: WHILE v_rows > 0 DO
    SET v_iteration = v_iteration + 1;
    SET @sql = CONCAT(
      'INSERT IGNORE INTO adresses_geocodees (code_postal, commune, voie, type_de_voie, no_voie, adresse_norm, latitude, longitude) ',
      'SELECT d.code_postal, d.commune, d.voie, d.type_de_voie, d.no_voie, d.adresse_norm, 0.0, 0.0 FROM ( ',
      '  SELECT d2.code_postal, d2.commune, d2.voie, d2.type_de_voie, d2.no_voie, d2.adresse_norm FROM ( ',
      '    SELECT DISTINCT v.code_postal, v.commune, v.voie, v.type_de_voie, v.no_voie, ',
      "      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm ",
      '    FROM valeursfoncieres v ',
      '    WHERE (v.latitude IS NULL OR v.longitude IS NULL) ',
      '      AND v.date_mutation >= ''', v_date_min, ''' AND v.date_mutation < ''', v_date_max, ''' ',
      '  ) d2 ',
      '  LEFT JOIN adresses_geocodees a ON a.adresse_norm = d2.adresse_norm WHERE a.id IS NULL ',
      '  LIMIT ', p_batch_size,
      ' ) d'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    SET v_rows = ROW_COUNT();
    DEALLOCATE PREPARE stmt;

    SET v_total_rows = v_total_rows + v_rows;
    SELECT CONCAT('  Itération ', v_iteration, ' : ', v_rows, ' enregistrement(s) inséré(s)') AS progression;
  END WHILE read_loop;

  SELECT CONCAT('Année ', p_year, ' terminée : ', v_iteration, ' itération(s), ', v_total_rows, ' enregistrement(s) au total') AS progression;
END//

DELIMITER ;
