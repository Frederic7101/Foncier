-- Procédure : refresh vf_communes par année (version simple : 5 colonnes seulement).
-- DÉPRÉCIÉE : utiliser sp_refresh_vf_communes_agg (03_sp_refresh_vf_communes_agg.sql)
-- qui remplit toute la table (quartiles, tranches S/T, variations N-1).
-- Exemple : CALL sp_refresh_vf_communes(2014, 2025);

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_refresh_vf_communes$$

CREATE PROCEDURE sp_refresh_vf_communes(
  IN p_annee_min INT UNSIGNED,
  IN p_annee_max INT UNSIGNED
)
BEGIN
  DECLARE v_annee INT UNSIGNED;

  IF p_annee_min > p_annee_max THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_annee_min doit etre <= p_annee_max';
  END IF;

  TRUNCATE TABLE vf_communes;

  SET v_annee = p_annee_min;
  WHILE v_annee <= p_annee_max DO
    INSERT INTO vf_communes (
      code_dept,
      code_postal,
      commune,
      annee,
      type_local,
      nb_ventes,
      prix_moyen,
      surface_moyenne,
      prix_moyen_m2,
      last_refreshed
    )
    SELECT
      v.code_dept,
      v.code_postal,
      v.commune,
      v.annee,
      v.type_local,
      COUNT(*)                    AS nb_ventes,
      ROUND(AVG(v.valeur_fonciere), 2)     AS prix_moyen,
      ROUND(AVG(v.surface_reelle_bati), 2) AS surface_moyenne,
      ROUND(SUM(v.valeur_fonciere) / NULLIF(SUM(v.surface_reelle_bati), 0), 2) AS prix_moyen_m2,
      CURRENT_TIMESTAMP           AS last_refreshed
    FROM vf_all_ventes v
    WHERE v.valeur_fonciere > 0
      AND v.surface_reelle_bati > 0
      AND v.type_local IN ('Appartement', 'Maison')
      AND v.annee = v_annee
    GROUP BY v.code_dept, v.code_postal, v.commune, v.annee, v.type_local;

    SET v_annee = v_annee + 1;
  END WHILE;
END$$

DELIMITER ;
