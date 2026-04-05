-- Procédures : refresh vf_communes depuis vf_all_ventes (une année ou plage).
-- Utilise valeur_fonciere, surface_reelle_bati, prix_m2, nb_lignes, nombre_pieces_principales.
-- nb_ventes = SUM(nb_lignes). Tranches S : S1 <25, S2 25-35, S3 35-45, S4 45-55, S5 >55.
-- Tranches T : T1=1 piece, T2=2, T3=3, T4=4, T5=5+.
--
-- Exemple : CALL sp_refresh_vf_communes_agg(2024, NULL, NULL);  (toute l'année)
--           CALL sp_refresh_vf_communes_agg(2024, '75', NULL);  (un département, une année)
--           CALL sp_refresh_vf_communes_agg(2024, NULL, '75001');  (un code postal, une année)
--           CALL sp_refresh_vf_communes_agg_by_dept(2024);  (une année, tous les départements)
--           CALL sp_refresh_vf_communes_agg_dept_years('75', 2014, 2025);  (un département, plage d'années)
--           CALL sp_refresh_vf_communes_agg_postal_years('75001', 2014, 2025);  (un code postal, plage d'années)
--           CALL sp_refresh_vf_communes_all(2014, 2025);
--
-- Ordre de grandeur : exécution toute la France de 2020 (2e semestre) à 2025 (1er semestre) ≈ 3 h.

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_refresh_vf_communes_all$$
CREATE PROCEDURE sp_refresh_vf_communes_all(IN p_annee_min INT UNSIGNED, IN p_annee_max INT UNSIGNED)
BEGIN
  DECLARE v_annee INT UNSIGNED;
  SET v_annee = p_annee_min;
  WHILE v_annee <= p_annee_max DO
    CALL sp_refresh_vf_communes_agg(v_annee, NULL, NULL);
    SET v_annee = v_annee + 1;
  END WHILE;
END$$

DROP PROCEDURE IF EXISTS sp_refresh_vf_communes_agg$$
CREATE PROCEDURE sp_refresh_vf_communes_agg(IN p_year INT UNSIGNED, IN p_code_dept VARCHAR(5), IN p_code_postal VARCHAR(10))
BEGIN
  -- p_code_dept NULL = tous départements ; p_code_postal NULL = tous codes postaux (pour traitement par lot)
  INSERT INTO vf_communes (
    code_dept, code_postal, commune, annee, type_local,
    nb_ventes, prix_moyen, prix_q1, prix_median, prix_q3,
    surface_moyenne, surface_mediane, prix_m2_moyenne,
    prix_m2_q1, prix_m2_mediane, prix_m2_q3,
    prix_med_s1, surf_med_s1, prix_m2_w_s1, prix_med_s2, surf_med_s2, prix_m2_w_s2,
    prix_med_s3, surf_med_s3, prix_m2_w_s3, prix_med_s4, surf_med_s4, prix_m2_w_s4,
    prix_med_s5, surf_med_s5, prix_m2_w_s5,
    prix_med_T1, surf_med_T1, prix_m2_w_T1, prix_med_T2, surf_med_T2, prix_m2_w_T2,
    prix_med_T3, surf_med_T3, prix_m2_w_T3, prix_med_T4, surf_med_T4, prix_m2_w_T4,
    prix_med_T5, surf_med_T5, prix_m2_w_T5,
    nb_ventes_var_pct, prix_moyen_var_pct, surface_moyenne_var_pct,
    prix_moyen_m2_var_pct, prix_m2_mediane_var_pct, surface_mediane_var_pct,
    nb_ventes_s1, nb_ventes_s2, nb_ventes_s3, nb_ventes_s4, nb_ventes_s5,
    nb_ventes_t1, nb_ventes_t2, nb_ventes_t3, nb_ventes_t4, nb_ventes_t5,
    last_refreshed
  )
  SELECT
    g.code_dept, g.code_postal, g.commune, p_year AS annee, g.type_local,
    g.nb_ventes, g.prix_moyen, pq.prix_q1, g.prix_median, pq.prix_q3,
    g.surface_moyenne, ms.surface_mediane, g.prix_m2_moyenne,
    q.prix_m2_q1, q.prix_m2_mediane, q.prix_m2_q3,
    pvt_s.prix_med_s1, pvt_s.surf_med_s1, pvt_s.prix_m2_w_s1,
    pvt_s.prix_med_s2, pvt_s.surf_med_s2, pvt_s.prix_m2_w_s2,
    pvt_s.prix_med_s3, pvt_s.surf_med_s3, pvt_s.prix_m2_w_s3,
    pvt_s.prix_med_s4, pvt_s.surf_med_s4, pvt_s.prix_m2_w_s4,
    pvt_s.prix_med_s5, pvt_s.surf_med_s5, pvt_s.prix_m2_w_s5,
    pvt_t.prix_med_T1, pvt_t.surf_med_T1, pvt_t.prix_m2_w_T1, pvt_t.prix_med_T2, pvt_t.surf_med_T2, pvt_t.prix_m2_w_T2,
    pvt_t.prix_med_T3, pvt_t.surf_med_T3, pvt_t.prix_m2_w_T3, pvt_t.prix_med_T4, pvt_t.surf_med_T4, pvt_t.prix_m2_w_T4,
    pvt_t.prix_med_T5, pvt_t.surf_med_T5, pvt_t.prix_m2_w_T5,
    LEAST(GREATEST(ROUND((g.nb_ventes       - IFNULL(p.nb_ventes_prev,0))        / NULLIF(p.nb_ventes_prev,0)        * 100, 2), -9999999.99), 9999999.99),
    LEAST(GREATEST(ROUND((g.prix_moyen      - IFNULL(p.prix_moyen_prev,0))       / NULLIF(p.prix_moyen_prev,0)       * 100, 2), -9999999.99), 9999999.99),
    LEAST(GREATEST(ROUND((g.surface_moyenne - IFNULL(p.surface_moyenne_prev,0))  / NULLIF(p.surface_moyenne_prev,0)  * 100, 2), -9999999.99), 9999999.99),
    LEAST(GREATEST(ROUND((g.prix_m2_moyenne - IFNULL(p.prix_moyen_m2_prev,0))    / NULLIF(p.prix_moyen_m2_prev,0)    * 100, 2), -9999999.99), 9999999.99),
    LEAST(GREATEST(ROUND((q.prix_m2_mediane - IFNULL(p.prix_m2_mediane_prev,0))  / NULLIF(p.prix_m2_mediane_prev,0)  * 100, 2), -9999999.99), 9999999.99),
    LEAST(GREATEST(ROUND((ms.surface_mediane - IFNULL(p.surface_mediane_prev,0))  / NULLIF(p.surface_mediane_prev,0)  * 100, 2), -9999999.99), 9999999.99),
    cnt_s.nb_ventes_s1, cnt_s.nb_ventes_s2, cnt_s.nb_ventes_s3, cnt_s.nb_ventes_s4, cnt_s.nb_ventes_s5,
    cnt_t.nb_ventes_t1, cnt_t.nb_ventes_t2, cnt_t.nb_ventes_t3, cnt_t.nb_ventes_t4, cnt_t.nb_ventes_t5,
    CURRENT_TIMESTAMP
  FROM
  ( SELECT
      b.code_dept, b.code_postal, b.commune, b.type_local,
      COUNT(b.nb_lignes) AS nb_ventes,
      ROUND(SUM(b.prix) / NULLIF(COUNT(b.nb_lignes),0), 2) AS prix_moyen,
      ROUND((1 - pm.f) * pm.prix_lo + pm.f * pm.prix_hi, 2) AS prix_median,
      ROUND(SUM(b.surf) / NULLIF(COUNT(b.nb_lignes),0), 2) AS surface_moyenne,
      ROUND(SUM(b.prix) / NULLIF(SUM(b.surf),0), 2) AS prix_m2_moyenne
    FROM (
      SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix, surface_reelle_bati AS surf, nb_lignes
      FROM vf_all_ventes v
      WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
        AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
        AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
        AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0
    ) b
    LEFT JOIN (
      SELECT x.code_dept, x.code_postal, x.commune, x.type_local, x.n,
        ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.prix END) AS prix_lo,
        MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.50+1) THEN x.prix END) AS prix_hi
      FROM (
        SELECT b2.code_dept, b2.code_postal, b2.commune, b2.type_local, b2.prix,
          ROW_NUMBER() OVER (PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local ORDER BY b2.prix) AS rn,
          COUNT(*) OVER (PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local) AS n
        FROM (
          SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0
        ) b2
      ) x GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
    ) pm ON pm.code_dept = b.code_dept AND pm.code_postal = b.code_postal AND pm.commune = b.commune AND pm.type_local = b.type_local
    GROUP BY b.code_dept, b.code_postal, b.commune, b.type_local, pm.f, pm.prix_lo, pm.prix_hi
  ) g
  LEFT JOIN (
    SELECT z.code_dept, z.code_postal, z.commune, z.type_local,
      ROUND((1-z.q1_f)*z.prix_q1_lo + z.q1_f*z.prix_q1_hi, 2) AS prix_q1,
      ROUND((1-z.q3_f)*z.prix_q3_lo + z.q3_f*z.prix_q3_hi, 2) AS prix_q3
    FROM (
      SELECT x.code_dept, x.code_postal, x.commune, x.type_local, x.n,
        ((x.n-1)*0.25+1 - FLOOR((x.n-1)*0.25+1)) AS q1_f,
        ((x.n-1)*0.75+1 - FLOOR((x.n-1)*0.75+1)) AS q3_f,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.25+1) THEN x.prix END) AS prix_q1_lo,
        MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.25+1) THEN x.prix END) AS prix_q1_hi,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.75+1) THEN x.prix END) AS prix_q3_lo,
        MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.75+1) THEN x.prix END) AS prix_q3_hi
      FROM (
        SELECT b2.code_dept, b2.code_postal, b2.commune, b2.type_local, b2.prix,
          ROW_NUMBER() OVER (PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local ORDER BY b2.prix) AS rn,
          COUNT(*) OVER (PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local) AS n
        FROM (
          SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0
        ) b2
      ) x GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
    ) z
  ) pq ON pq.code_dept=g.code_dept AND pq.code_postal=g.code_postal AND pq.commune=g.commune AND pq.type_local=g.type_local
  LEFT JOIN (
    SELECT z.code_dept, z.code_postal, z.commune, z.type_local,
      ROUND((1-z.q1_f)*z.p2m_q1_lo + z.q1_f*z.p2m_q1_hi, 2) AS prix_m2_q1,
      ROUND((1-z.q2_f)*z.p2m_q2_lo + z.q2_f*z.p2m_q2_hi, 2) AS prix_m2_mediane,
      ROUND((1-z.q3_f)*z.p2m_q3_lo + z.q3_f*z.p2m_q3_hi, 2) AS prix_m2_q3
    FROM (
      SELECT x.code_dept, x.code_postal, x.commune, x.type_local, x.n,
        (x.n-1)*0.25+1 AS q1_pos, (x.n-1)*0.50+1 AS q2_pos, (x.n-1)*0.75+1 AS q3_pos,
        ((x.n-1)*0.25+1 - FLOOR((x.n-1)*0.25+1)) AS q1_f,
        ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS q2_f,
        ((x.n-1)*0.75+1 - FLOOR((x.n-1)*0.75+1)) AS q3_f,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.25+1) THEN x.p2m END) AS p2m_q1_lo,
        MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.25+1) THEN x.p2m END) AS p2m_q1_hi,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_q2_lo,
        MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_q2_hi,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.75+1) THEN x.p2m END) AS p2m_q3_lo,
        MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.75+1) THEN x.p2m END) AS p2m_q3_hi
      FROM (
        SELECT b2.code_dept, b2.code_postal, b2.commune, b2.type_local, (b2.prix/b2.surf) AS p2m,
          ROW_NUMBER() OVER (PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local ORDER BY (b2.prix/b2.surf)) AS rn,
          COUNT(*) OVER (PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local) AS n
        FROM (
          SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix, surface_reelle_bati AS surf
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0
        ) b2
      ) x GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
    ) z
  ) q ON q.code_dept=g.code_dept AND q.code_postal=g.code_postal AND q.commune=g.commune AND q.type_local=g.type_local
  LEFT JOIN (
    SELECT y.code_dept, y.code_postal, y.commune, y.type_local,
      ROUND((1-y.f)*y.surf_lo + y.f*y.surf_hi, 2) AS surface_mediane
    FROM (
      SELECT x.code_dept, x.code_postal, x.commune, x.type_local, x.n,
        ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
        MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.surf END) AS surf_lo,
        MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.50+1) THEN x.surf END) AS surf_hi
      FROM (
        SELECT b3.code_dept, b3.code_postal, b3.commune, b3.type_local, b3.surf,
          ROW_NUMBER() OVER (PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local ORDER BY b3.surf) AS rn,
          COUNT(*) OVER (PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local) AS n
        FROM (
          SELECT code_dept, code_postal, commune, type_local, surface_reelle_bati AS surf
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.surface_reelle_bati > 0
        ) b3
      ) x GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
    ) y
  ) ms ON ms.code_dept=g.code_dept AND ms.code_postal=g.code_postal AND ms.commune=g.commune AND ms.type_local=g.type_local
  LEFT JOIN (
    SELECT code_dept, code_postal, commune, type_local,
      MAX(CASE WHEN bucket='S1' THEN prix_med END) AS prix_med_s1, MAX(CASE WHEN bucket='S1' THEN surf_med END) AS surf_med_s1, MAX(CASE WHEN bucket='S1' THEN p2m_w END) AS prix_m2_w_s1,
      MAX(CASE WHEN bucket='S2' THEN prix_med END) AS prix_med_s2, MAX(CASE WHEN bucket='S2' THEN surf_med END) AS surf_med_s2, MAX(CASE WHEN bucket='S2' THEN p2m_w END) AS prix_m2_w_s2,
      MAX(CASE WHEN bucket='S3' THEN prix_med END) AS prix_med_s3, MAX(CASE WHEN bucket='S3' THEN surf_med END) AS surf_med_s3, MAX(CASE WHEN bucket='S3' THEN p2m_w END) AS prix_m2_w_s3,
      MAX(CASE WHEN bucket='S4' THEN prix_med END) AS prix_med_s4, MAX(CASE WHEN bucket='S4' THEN surf_med END) AS surf_med_s4, MAX(CASE WHEN bucket='S4' THEN p2m_w END) AS prix_m2_w_s4,
      MAX(CASE WHEN bucket='S5' THEN prix_med END) AS prix_med_s5, MAX(CASE WHEN bucket='S5' THEN surf_med END) AS surf_med_s5, MAX(CASE WHEN bucket='S5' THEN p2m_w END) AS prix_m2_w_s5
    FROM (
      SELECT s2.code_dept, s2.code_postal, s2.commune, s2.type_local, s2.bucket,
        MAX(ROUND((1-t.f_prix)*t.prix_lo + t.f_prix*t.prix_hi, 2)) AS prix_med,
        MAX(ROUND((1-t.f_surf)*t.surf_lo + t.f_surf*t.surf_hi, 2)) AS surf_med,
        MAX(ROUND(t.sum_prix/NULLIF(t.sum_surf,0), 2)) AS p2m_w
      FROM (
        SELECT s.code_dept, s.code_postal, s.commune, s.type_local, s.bucket, s.prix, s.surf,
          ROW_NUMBER() OVER (PARTITION BY s.code_dept, s.code_postal, s.commune, s.type_local, s.bucket ORDER BY s.prix) AS rn_prix,
          COUNT(*) OVER (PARTITION BY s.code_dept, s.code_postal, s.commune, s.type_local, s.bucket) AS n_prix,
          ROW_NUMBER() OVER (PARTITION BY s.code_dept, s.code_postal, s.commune, s.type_local, s.bucket ORDER BY s.surf) AS rn_surf,
          COUNT(*) OVER (PARTITION BY s.code_dept, s.code_postal, s.commune, s.type_local, s.bucket) AS n_surf,
          SUM(s.prix) OVER (PARTITION BY s.code_dept, s.code_postal, s.commune, s.type_local, s.bucket) AS sum_prix,
          SUM(s.surf) OVER (PARTITION BY s.code_dept, s.code_postal, s.commune, s.type_local, s.bucket) AS sum_surf
        FROM (
          SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix, surface_reelle_bati AS surf,
            CASE WHEN surface_reelle_bati < 25 THEN 'S1' WHEN surface_reelle_bati >= 25 AND surface_reelle_bati < 35 THEN 'S2'
                 WHEN surface_reelle_bati >= 35 AND surface_reelle_bati < 45 THEN 'S3' WHEN surface_reelle_bati >= 45 AND surface_reelle_bati <= 55 THEN 'S4'
                 WHEN surface_reelle_bati > 55 THEN 'S5' END AS bucket
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0
        ) s WHERE s.bucket IS NOT NULL
      ) s2
      JOIN (
        SELECT code_dept, code_postal, commune, type_local, bucket, n_prix, n_surf,
          ((n_prix-1)*0.50+1 - FLOOR((n_prix-1)*0.50+1)) AS f_prix,
          ((n_surf-1)*0.50+1 - FLOOR((n_surf-1)*0.50+1)) AS f_surf,
          MAX(CASE WHEN rn_prix = FLOOR((n_prix-1)*0.50+1) THEN prix END) AS prix_lo,
          MAX(CASE WHEN rn_prix = CEIL((n_prix-1)*0.50+1) THEN prix END) AS prix_hi,
          MAX(CASE WHEN rn_surf = FLOOR((n_surf-1)*0.50+1) THEN surf END) AS surf_lo,
          MAX(CASE WHEN rn_surf = CEIL((n_surf-1)*0.50+1) THEN surf END) AS surf_hi,
          MAX(sum_prix) AS sum_prix, MAX(sum_surf) AS sum_surf
        FROM (
          SELECT code_dept, code_postal, commune, type_local, bucket, prix, surf,
            ROW_NUMBER() OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket ORDER BY prix) AS rn_prix,
            COUNT(*) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket) AS n_prix,
            ROW_NUMBER() OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket ORDER BY surf) AS rn_surf,
            COUNT(*) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket) AS n_surf,
            SUM(prix) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket) AS sum_prix,
            SUM(surf) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket) AS sum_surf
          FROM (
            SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix, surface_reelle_bati AS surf,
              CASE WHEN surface_reelle_bati < 25 THEN 'S1' WHEN surface_reelle_bati >= 25 AND surface_reelle_bati < 35 THEN 'S2'
                   WHEN surface_reelle_bati >= 35 AND surface_reelle_bati < 45 THEN 'S3' WHEN surface_reelle_bati >= 45 AND surface_reelle_bati <= 55 THEN 'S4'
                   WHEN surface_reelle_bati > 55 THEN 'S5' END AS bucket
            FROM vf_all_ventes v
            WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0
          ) b4 WHERE bucket IS NOT NULL
        ) r GROUP BY code_dept, code_postal, commune, type_local, bucket, n_prix, n_surf
      ) t ON t.code_dept = s2.code_dept AND t.code_postal = s2.code_postal AND t.commune = s2.commune AND t.type_local = s2.type_local AND t.bucket = s2.bucket
      GROUP BY s2.code_dept, s2.code_postal, s2.commune, s2.type_local, s2.bucket
    ) alls GROUP BY code_dept, code_postal, commune, type_local
  ) pvt_s ON pvt_s.code_dept=g.code_dept AND pvt_s.code_postal=g.code_postal AND pvt_s.commune=g.commune AND pvt_s.type_local=g.type_local
  LEFT JOIN (
    SELECT code_dept, code_postal, commune, type_local,
      MAX(CASE WHEN bucket='T1' THEN prix_med END) AS prix_med_T1, MAX(CASE WHEN bucket='T1' THEN surf_med END) AS surf_med_T1, MAX(CASE WHEN bucket='T1' THEN p2m_w END) AS prix_m2_w_T1,
      MAX(CASE WHEN bucket='T2' THEN prix_med END) AS prix_med_T2, MAX(CASE WHEN bucket='T2' THEN surf_med END) AS surf_med_T2, MAX(CASE WHEN bucket='T2' THEN p2m_w END) AS prix_m2_w_T2,
      MAX(CASE WHEN bucket='T3' THEN prix_med END) AS prix_med_T3, MAX(CASE WHEN bucket='T3' THEN surf_med END) AS surf_med_T3, MAX(CASE WHEN bucket='T3' THEN p2m_w END) AS prix_m2_w_T3,
      MAX(CASE WHEN bucket='T4' THEN prix_med END) AS prix_med_T4, MAX(CASE WHEN bucket='T4' THEN surf_med END) AS surf_med_T4, MAX(CASE WHEN bucket='T4' THEN p2m_w END) AS prix_m2_w_T4,
      MAX(CASE WHEN bucket='T5' THEN prix_med END) AS prix_med_T5, MAX(CASE WHEN bucket='T5' THEN surf_med END) AS surf_med_T5, MAX(CASE WHEN bucket='T5' THEN p2m_w END) AS prix_m2_w_T5
    FROM (
      SELECT s3.code_dept, s3.code_postal, s3.commune, s3.type_local, s3.bucket,
        MAX(ROUND((1-t.f_prix)*t.prix_lo + t.f_prix*t.prix_hi, 2)) AS prix_med,
        MAX(ROUND((1-t.f_surf)*t.surf_lo + t.f_surf*t.surf_hi, 2)) AS surf_med,
        MAX(ROUND(t.sum_prix/NULLIF(t.sum_surf,0), 2)) AS p2m_w
      FROM (
        SELECT s2.code_dept, s2.code_postal, s2.commune, s2.type_local, s2.bucket, s2.prix, s2.surf,
          ROW_NUMBER() OVER (PARTITION BY s2.code_dept, s2.code_postal, s2.commune, s2.type_local, s2.bucket ORDER BY s2.prix) AS rn_prix,
          COUNT(*) OVER (PARTITION BY s2.code_dept, s2.code_postal, s2.commune, s2.type_local, s2.bucket) AS n_prix,
          ROW_NUMBER() OVER (PARTITION BY s2.code_dept, s2.code_postal, s2.commune, s2.type_local, s2.bucket ORDER BY s2.surf) AS rn_surf,
          COUNT(*) OVER (PARTITION BY s2.code_dept, s2.code_postal, s2.commune, s2.type_local, s2.bucket) AS n_surf
        FROM (
          SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix, surface_reelle_bati AS surf,
            CASE WHEN COALESCE(nombre_pieces_principales,0) < 1.5 THEN 'T1' WHEN nombre_pieces_principales >= 1.5 AND nombre_pieces_principales < 2.5 THEN 'T2'
                 WHEN nombre_pieces_principales >= 2.5 AND nombre_pieces_principales < 3.5 THEN 'T3' WHEN nombre_pieces_principales >= 3.5 AND nombre_pieces_principales < 4.5 THEN 'T4'
                 WHEN nombre_pieces_principales >= 4.5 THEN 'T5' END AS bucket
          FROM vf_all_ventes v
          WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0 AND v.nombre_pieces_principales IS NOT NULL
        ) s2 WHERE s2.bucket IS NOT NULL
      ) s3
      JOIN (
        SELECT code_dept, code_postal, commune, type_local, bucket, n_prix, n_surf,
          ((n_prix-1)*0.50+1 - FLOOR((n_prix-1)*0.50+1)) AS f_prix,
          ((n_surf-1)*0.50+1 - FLOOR((n_surf-1)*0.50+1)) AS f_surf,
          MAX(CASE WHEN rn_prix = FLOOR((n_prix-1)*0.50+1) THEN prix END) AS prix_lo,
          MAX(CASE WHEN rn_prix = CEIL((n_prix-1)*0.50+1) THEN prix END) AS prix_hi,
          MAX(CASE WHEN rn_surf = FLOOR((n_surf-1)*0.50+1) THEN surf END) AS surf_lo,
          MAX(CASE WHEN rn_surf = CEIL((n_surf-1)*0.50+1) THEN surf END) AS surf_hi,
          MAX(sum_prix) AS sum_prix, MAX(sum_surf) AS sum_surf
        FROM (
          SELECT code_dept, code_postal, commune, type_local, bucket, prix, surf,
            ROW_NUMBER() OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket ORDER BY prix) AS rn_prix,
            COUNT(*) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket) AS n_prix,
            ROW_NUMBER() OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket ORDER BY surf) AS rn_surf,
            COUNT(*) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket) AS n_surf,
            SUM(prix) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket) AS sum_prix,
            SUM(surf) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket) AS sum_surf
          FROM (
            SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix, surface_reelle_bati AS surf,
              CASE WHEN COALESCE(nombre_pieces_principales,0) < 1.5 THEN 'T1' WHEN nombre_pieces_principales >= 1.5 AND nombre_pieces_principales < 2.5 THEN 'T2'
                   WHEN nombre_pieces_principales >= 2.5 AND nombre_pieces_principales < 3.5 THEN 'T3' WHEN nombre_pieces_principales >= 3.5 AND nombre_pieces_principales < 4.5 THEN 'T4'
                   WHEN nombre_pieces_principales >= 4.5 THEN 'T5' END AS bucket
            FROM vf_all_ventes v
            WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0 AND v.nombre_pieces_principales IS NOT NULL
          ) b4 WHERE bucket IS NOT NULL
        ) r GROUP BY code_dept, code_postal, commune, type_local, bucket, n_prix, n_surf
      ) t ON t.code_dept = s3.code_dept AND t.code_postal = s3.code_postal AND t.commune = s3.commune AND t.type_local = s3.type_local AND t.bucket = s3.bucket
      GROUP BY s3.code_dept, s3.code_postal, s3.commune, s3.type_local, s3.bucket
    ) allt GROUP BY code_dept, code_postal, commune, type_local
  ) pvt_t ON pvt_t.code_dept=g.code_dept AND pvt_t.code_postal=g.code_postal AND pvt_t.commune=g.commune AND pvt_t.type_local=g.type_local
  LEFT JOIN (
    -- Comptages par tranche surface S1..S5
    SELECT code_dept, code_postal, commune, type_local,
      SUM(CASE WHEN bucket='S1' THEN cnt ELSE 0 END) AS nb_ventes_s1,
      SUM(CASE WHEN bucket='S2' THEN cnt ELSE 0 END) AS nb_ventes_s2,
      SUM(CASE WHEN bucket='S3' THEN cnt ELSE 0 END) AS nb_ventes_s3,
      SUM(CASE WHEN bucket='S4' THEN cnt ELSE 0 END) AS nb_ventes_s4,
      SUM(CASE WHEN bucket='S5' THEN cnt ELSE 0 END) AS nb_ventes_s5
    FROM (
      SELECT code_dept, code_postal, commune, type_local,
        CASE WHEN surface_reelle_bati < 25 THEN 'S1'
             WHEN surface_reelle_bati < 35 THEN 'S2'
             WHEN surface_reelle_bati < 45 THEN 'S3'
             WHEN surface_reelle_bati <= 55 THEN 'S4'
             ELSE 'S5' END AS bucket,
        COUNT(*) AS cnt
      FROM vf_all_ventes v
      WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
        AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
        AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
        AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0
      GROUP BY code_dept, code_postal, commune, type_local, bucket
    ) sc WHERE bucket IS NOT NULL
    GROUP BY code_dept, code_postal, commune, type_local
  ) cnt_s ON cnt_s.code_dept=g.code_dept AND cnt_s.code_postal=g.code_postal AND cnt_s.commune=g.commune AND cnt_s.type_local=g.type_local
  LEFT JOIN (
    -- Comptages par tranche pièces T1..T5
    SELECT code_dept, code_postal, commune, type_local,
      SUM(CASE WHEN bucket='T1' THEN cnt ELSE 0 END) AS nb_ventes_t1,
      SUM(CASE WHEN bucket='T2' THEN cnt ELSE 0 END) AS nb_ventes_t2,
      SUM(CASE WHEN bucket='T3' THEN cnt ELSE 0 END) AS nb_ventes_t3,
      SUM(CASE WHEN bucket='T4' THEN cnt ELSE 0 END) AS nb_ventes_t4,
      SUM(CASE WHEN bucket='T5' THEN cnt ELSE 0 END) AS nb_ventes_t5
    FROM (
      SELECT code_dept, code_postal, commune, type_local,
        CASE WHEN COALESCE(nombre_pieces_principales,0) < 1.5 THEN 'T1'
             WHEN nombre_pieces_principales < 2.5 THEN 'T2'
             WHEN nombre_pieces_principales < 3.5 THEN 'T3'
             WHEN nombre_pieces_principales < 4.5 THEN 'T4'
             ELSE 'T5' END AS bucket,
        COUNT(*) AS cnt
      FROM vf_all_ventes v
      WHERE v.date_mutation >= CONCAT(p_year, '-01-01') AND v.date_mutation < CONCAT(p_year+1, '-01-01')
        AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
        AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
        AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0
        AND v.nombre_pieces_principales IS NOT NULL
      GROUP BY code_dept, code_postal, commune, type_local, bucket
    ) tc WHERE bucket IS NOT NULL
    GROUP BY code_dept, code_postal, commune, type_local
  ) cnt_t ON cnt_t.code_dept=g.code_dept AND cnt_t.code_postal=g.code_postal AND cnt_t.commune=g.commune AND cnt_t.type_local=g.type_local
  LEFT JOIN (
    SELECT a.code_dept, a.code_postal, a.commune, a.type_local,
      a.nb_ventes_prev, a.prix_moyen_prev, a.surface_moyenne_prev, a.prix_moyen_m2_prev,
      mpr.prix_m2_mediane_prev, msr.surface_mediane_prev
    FROM (
      SELECT b.code_dept, b.code_postal, b.commune, b.type_local,
        COUNT(b.nb_lignes) AS nb_ventes_prev,
        ROUND(SUM(b.prix) / NULLIF(COUNT(b.nb_lignes),0), 2) AS prix_moyen_prev,
        ROUND(SUM(b.surf) / NULLIF(COUNT(b.nb_lignes),0), 2) AS surface_moyenne_prev,
        ROUND(SUM(b.prix) / NULLIF(SUM(b.surf),0), 2) AS prix_moyen_m2_prev
      FROM (
        SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix, surface_reelle_bati AS surf, nb_lignes
        FROM vf_all_ventes v
        WHERE v.date_mutation >= CONCAT(p_year-1, '-01-01') AND v.date_mutation < CONCAT(p_year, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0
      ) b GROUP BY b.code_dept, b.code_postal, b.commune, b.type_local
    ) a
    LEFT JOIN (
      SELECT z.code_dept, z.code_postal, z.commune, z.type_local,
        ROUND((1-z.f)*z.p2m_lo + z.f*z.p2m_hi, 2) AS prix_m2_mediane_prev
      FROM (
        SELECT x.code_dept, x.code_postal, x.commune, x.type_local, x.n, ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
          MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_lo,
          MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_hi
        FROM (
          SELECT b2.code_dept, b2.code_postal, b2.commune, b2.type_local, (b2.prix/b2.surf) AS p2m,
            ROW_NUMBER() OVER (PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local ORDER BY (b2.prix/b2.surf)) AS rn,
            COUNT(*) OVER (PARTITION BY b2.code_dept, b2.code_postal, b2.commune, b2.type_local) AS n
          FROM (
            SELECT code_dept, code_postal, commune, type_local, valeur_fonciere AS prix, surface_reelle_bati AS surf
            FROM vf_all_ventes v
            WHERE v.date_mutation >= CONCAT(p_year-1, '-01-01') AND v.date_mutation < CONCAT(p_year, '-01-01')
            AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
            AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
            AND v.type_local IN ('Appartement','Maison') AND v.valeur_fonciere > 0 AND v.surface_reelle_bati > 0
          ) b2
        ) x GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
      ) z
    ) mpr ON mpr.code_dept=a.code_dept AND mpr.code_postal=a.code_postal AND mpr.commune=a.commune AND mpr.type_local=a.type_local
    LEFT JOIN (
      SELECT y.code_dept, y.code_postal, y.commune, y.type_local,
        ROUND((1-y.f)*y.surf_lo + y.f*y.surf_hi, 2) AS surface_mediane_prev
      FROM (
        SELECT x.code_dept, x.code_postal, x.commune, x.type_local, x.n, ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
          MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.surf END) AS surf_lo,
          MAX(CASE WHEN x.rn = CEIL((x.n-1)*0.50+1) THEN x.surf END) AS surf_hi
        FROM (
          SELECT b3.code_dept, b3.code_postal, b3.commune, b3.type_local, b3.surf,
          ROW_NUMBER() OVER (PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local ORDER BY b3.surf) AS rn,
          COUNT(*) OVER (PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local) AS n
          FROM (
            SELECT code_dept, code_postal, commune, type_local, surface_reelle_bati AS surf
            FROM vf_all_ventes v
            WHERE v.date_mutation >= CONCAT(p_year-1, '-01-01') AND v.date_mutation < CONCAT(p_year, '-01-01')
              AND (p_code_dept IS NULL OR v.code_dept = p_code_dept)
              AND (p_code_postal IS NULL OR v.code_postal = p_code_postal)
              AND v.type_local IN ('Appartement','Maison') AND v.surface_reelle_bati > 0
          ) b3
        ) x GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
      ) y
    ) msr ON msr.code_dept=a.code_dept AND msr.code_postal=a.code_postal AND msr.commune=a.commune AND msr.type_local=a.type_local
  ) p ON p.code_dept=g.code_dept AND p.code_postal=g.code_postal AND p.commune=g.commune AND p.type_local=g.type_local
  ON DUPLICATE KEY UPDATE
    nb_ventes = VALUES(nb_ventes), prix_moyen = VALUES(prix_moyen), prix_q1 = VALUES(prix_q1), prix_median = VALUES(prix_median), prix_q3 = VALUES(prix_q3),
    surface_moyenne = VALUES(surface_moyenne), surface_mediane = VALUES(surface_mediane), prix_m2_moyenne = VALUES(prix_m2_moyenne),
    prix_m2_q1 = VALUES(prix_m2_q1), prix_m2_mediane = VALUES(prix_m2_mediane), prix_m2_q3 = VALUES(prix_m2_q3),
    prix_med_s1 = VALUES(prix_med_s1), surf_med_s1 = VALUES(surf_med_s1), prix_m2_w_s1 = VALUES(prix_m2_w_s1),
    prix_med_s2 = VALUES(prix_med_s2), surf_med_s2 = VALUES(surf_med_s2), prix_m2_w_s2 = VALUES(prix_m2_w_s2),
    prix_med_s3 = VALUES(prix_med_s3), surf_med_s3 = VALUES(surf_med_s3), prix_m2_w_s3 = VALUES(prix_m2_w_s3),
    prix_med_s4 = VALUES(prix_med_s4), surf_med_s4 = VALUES(surf_med_s4), prix_m2_w_s4 = VALUES(prix_m2_w_s4),
    prix_med_s5 = VALUES(prix_med_s5), surf_med_s5 = VALUES(surf_med_s5), prix_m2_w_s5 = VALUES(prix_m2_w_s5),
    prix_med_T1 = VALUES(prix_med_T1), surf_med_T1 = VALUES(surf_med_T1), prix_m2_w_T1 = VALUES(prix_m2_w_T1),
    prix_med_T2 = VALUES(prix_med_T2), surf_med_T2 = VALUES(surf_med_T2), prix_m2_w_T2 = VALUES(prix_m2_w_T2),
    prix_med_T3 = VALUES(prix_med_T3), surf_med_T3 = VALUES(surf_med_T3), prix_m2_w_T3 = VALUES(prix_m2_w_T3),
    prix_med_T4 = VALUES(prix_med_T4), surf_med_T4 = VALUES(surf_med_T4), prix_m2_w_T4 = VALUES(prix_m2_w_T4),
    prix_med_T5 = VALUES(prix_med_T5), surf_med_T5 = VALUES(surf_med_T5), prix_m2_w_T5 = VALUES(prix_m2_w_T5),
    nb_ventes_var_pct = VALUES(nb_ventes_var_pct), prix_moyen_var_pct = VALUES(prix_moyen_var_pct),
    surface_moyenne_var_pct = VALUES(surface_moyenne_var_pct), prix_moyen_m2_var_pct = VALUES(prix_moyen_m2_var_pct),
    prix_m2_mediane_var_pct = VALUES(prix_m2_mediane_var_pct), surface_mediane_var_pct = VALUES(surface_mediane_var_pct),
    nb_ventes_s1 = VALUES(nb_ventes_s1), nb_ventes_s2 = VALUES(nb_ventes_s2), nb_ventes_s3 = VALUES(nb_ventes_s3),
    nb_ventes_s4 = VALUES(nb_ventes_s4), nb_ventes_s5 = VALUES(nb_ventes_s5),
    nb_ventes_t1 = VALUES(nb_ventes_t1), nb_ventes_t2 = VALUES(nb_ventes_t2), nb_ventes_t3 = VALUES(nb_ventes_t3),
    nb_ventes_t4 = VALUES(nb_ventes_t4), nb_ventes_t5 = VALUES(nb_ventes_t5),
    last_refreshed = VALUES(last_refreshed);
END$$

-- Traite une année par lot (un département à la fois) pour limiter charge et durée par exécution.
DROP PROCEDURE IF EXISTS sp_refresh_vf_communes_agg_by_dept$$
CREATE PROCEDURE sp_refresh_vf_communes_agg_by_dept(IN p_year INT UNSIGNED)
BEGIN
  DECLARE v_done INT DEFAULT 0;
  DECLARE v_dept VARCHAR(5);
  DECLARE cur CURSOR FOR
    SELECT DISTINCT code_dept
    FROM vf_all_ventes
    WHERE date_mutation >= CONCAT(p_year, '-01-01') AND date_mutation < CONCAT(p_year + 1, '-01-01');
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

  DELETE FROM vf_communes WHERE annee = p_year;
  OPEN cur;
  read_loop: LOOP
    FETCH cur INTO v_dept;
    IF v_done THEN LEAVE read_loop; END IF;
    CALL sp_refresh_vf_communes_agg(p_year, v_dept, NULL);
  END LOOP;
  CLOSE cur;
END$$

-- Plage d'années : chaque année est traitée par département (lots plus petits).
DROP PROCEDURE IF EXISTS sp_refresh_vf_communes_all_by_dept$$
CREATE PROCEDURE sp_refresh_vf_communes_all_by_dept(IN p_annee_min INT UNSIGNED, IN p_annee_max INT UNSIGNED)
BEGIN
  DECLARE v_annee INT UNSIGNED;
  SET v_annee = p_annee_min;
  WHILE v_annee <= p_annee_max DO
    CALL sp_refresh_vf_communes_agg_by_dept(v_annee);
    SET v_annee = v_annee + 1;
  END WHILE;
END$$

-- Un département donné sur une plage d'années (ex. CALL sp_refresh_vf_communes_agg_dept_years('75', 2014, 2025)).
DROP PROCEDURE IF EXISTS sp_refresh_vf_communes_agg_dept_years$$
CREATE PROCEDURE sp_refresh_vf_communes_agg_dept_years(IN p_code_dept VARCHAR(5), IN p_annee_min INT UNSIGNED, IN p_annee_max INT UNSIGNED)
BEGIN
  DECLARE v_annee INT UNSIGNED;
  SET v_annee = p_annee_min;
  WHILE v_annee <= p_annee_max DO
    CALL sp_refresh_vf_communes_agg(v_annee, p_code_dept, NULL);
    SET v_annee = v_annee + 1;
  END WHILE;
END$$

-- Un code postal donné sur une plage d'années (ex. CALL sp_refresh_vf_communes_agg_postal_years('75001', 2014, 2025)).
DROP PROCEDURE IF EXISTS sp_refresh_vf_communes_agg_postal_years$$
CREATE PROCEDURE sp_refresh_vf_communes_agg_postal_years(IN p_code_postal VARCHAR(10), IN p_annee_min INT UNSIGNED, IN p_annee_max INT UNSIGNED)
BEGIN
  DECLARE v_annee INT UNSIGNED;
  SET v_annee = p_annee_min;
  WHILE v_annee <= p_annee_max DO
    CALL sp_refresh_vf_communes_agg(v_annee, NULL, p_code_postal);
    SET v_annee = v_annee + 1;
  END WHILE;
END$$

DELIMITER ;
