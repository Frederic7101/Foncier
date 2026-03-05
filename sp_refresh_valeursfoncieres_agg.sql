CREATE DEFINER=`root`@`localhost` PROCEDURE `foncier`.`sp_refresh_valeursfoncieres_agg`(IN p_year INT)
BEGIN
  /* Upsert les agrégats quotidiens par dimensions choisies */
  INSERT INTO vf_all_ventes (
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
END;