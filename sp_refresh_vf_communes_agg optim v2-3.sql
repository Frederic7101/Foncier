DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE `foncier`.`sp_refresh_vf_communes_agg`(IN p_year INT)
BEGIN

  /* =============================
     STAGING : tables permanentes (évite erreur 1137 "Can't reopen table")
     Une table temporaire ne peut être lue qu'une fois par requête en MySQL.
     ============================= */
  CREATE TABLE IF NOT EXISTS vf_staging_year (
    code_dept       VARCHAR(5) NOT NULL,
    code_postal     VARCHAR(5) NOT NULL,
    commune         VARCHAR(100) NOT NULL,
    type_local      VARCHAR(50) NOT NULL,
    date_mutation   DATE NOT NULL,
    prix_moyen      DECIMAL(15,2) NOT NULL,
    surface_moyenne DECIMAL(15,2) NOT NULL,
    KEY idx_staging_year_group (code_dept, code_postal, commune, type_local),
    KEY idx_staging_year_date  (date_mutation)
  ) ENGINE=InnoDB;

  CREATE TABLE IF NOT EXISTS vf_staging_year_prev (
    code_dept       VARCHAR(5) NOT NULL,
    code_postal     VARCHAR(5) NOT NULL,
    commune         VARCHAR(100) NOT NULL,
    type_local      VARCHAR(50) NOT NULL,
    date_mutation   DATE NOT NULL,
    prix_moyen      DECIMAL(15,2) NOT NULL,
    surface_moyenne DECIMAL(15,2) NOT NULL,
    KEY idx_staging_prev_group (code_dept, code_postal, commune, type_local),
    KEY idx_staging_prev_date  (date_mutation)
  ) ENGINE=InnoDB;

  -- Données année N (p_year)
  TRUNCATE TABLE vf_staging_year;
  INSERT INTO vf_staging_year (
    code_dept, code_postal, commune, type_local, date_mutation, prix_moyen, surface_moyenne
  )
  SELECT
    code_dept,
    code_postal,
    commune,
    type_local,
    date_mutation,
    prix_moyen,
    surface_moyenne
  FROM vf_all_ventes
  WHERE date_mutation >= CONCAT(p_year,   '-01-01')
    AND date_mutation <  CONCAT(p_year+1, '-01-01')
    AND type_local IN ('Appartement','Maison')
    AND prix_moyen > 0
    AND surface_moyenne > 0;

  -- Données année N-1 (p_year-1)
  TRUNCATE TABLE vf_staging_year_prev;
  INSERT INTO vf_staging_year_prev (
    code_dept, code_postal, commune, type_local, date_mutation, prix_moyen, surface_moyenne
  )
  SELECT
    code_dept,
    code_postal,
    commune,
    type_local,
    date_mutation,
    prix_moyen,
    surface_moyenne
  FROM vf_all_ventes
  WHERE date_mutation >= CONCAT(p_year-1, '-01-01')
    AND date_mutation <  CONCAT(p_year,   '-01-01')
    AND type_local IN ('Appartement','Maison')
    AND prix_moyen > 0
    AND surface_moyenne > 0;
	
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
    src.code_dept,
    src.code_postal,
    src.commune,
    src.annee,
    src.type_local,
    src.nb_ventes,
    src.prix_moyen,
    src.surface_moyenne,
    src.prix_moyen_m2,
    src.prix_m2_q1,
    src.prix_m2_mediane,
    src.prix_m2_q3,
    src.surface_mediane,
    src.prix_med_s1,
    src.surf_med_s1,
    src.prix_m2_w_s1,
    src.prix_med_s2,
    src.surf_med_s2,
    src.prix_m2_w_s2,
    src.prix_med_s3,
    src.surf_med_s3,
    src.prix_m2_w_s3,
    src.prix_med_s4,
    src.surf_med_s4,
    src.prix_m2_w_s4,
    src.prix_med_s5,
    src.surf_med_s5,
    src.prix_m2_w_s5,
    src.nb_ventes_var_pct,
    src.prix_moyen_var_pct,
    src.surface_moyenne_var_pct,
    src.prix_moyen_m2_var_pct,
    src.prix_m2_mediane_var_pct,
    src.surface_mediane_var_pct,
    src.last_refreshed
  FROM (
    SELECT
      g.code_dept,
      g.code_postal,
      g.commune,
      p_year AS annee,
      g.type_local,

      /* agrégats globaux année N */
      g.nb_ventes,
      g.prix_moyen,
      g.surface_moyenne,
      g.prix_moyen_m2,

      /* quartiles p2m + médiane surface année N */
      q.prix_m2_q1,
      q.prix_m2_mediane,
      q.prix_m2_q3,
      ms.surface_mediane,

      /* tranches surface (médianes + p2m pondéré) année N */
      pvt.prix_med_s1,
      pvt.surf_med_s1,
      pvt.prix_m2_w_s1,
      pvt.prix_med_s2,
      pvt.surf_med_s2,
      pvt.prix_m2_w_s2,
      pvt.prix_med_s3,
      pvt.surf_med_s3,
      pvt.prix_m2_w_s3,
      pvt.prix_med_s4,
      pvt.surf_med_s4,
      pvt.prix_m2_w_s4,
      pvt.prix_med_s5,
      pvt.surf_med_s5,
      pvt.prix_m2_w_s5,

      /* variations N vs N-1 (en %) – sécurisées */

      CASE
        WHEN p.nb_ventes_prev IS NULL
             OR p.nb_ventes_prev = 0
             OR ABS((g.nb_ventes - p.nb_ventes_prev) / p.nb_ventes_prev * 100.0) > 1000
        THEN NULL
        ELSE ROUND((g.nb_ventes - p.nb_ventes_prev) / p.nb_ventes_prev * 100.0, 2)
      END AS nb_ventes_var_pct,

      CASE
        WHEN p.prix_moyen_prev IS NULL
             OR p.prix_moyen_prev = 0
             OR ABS((g.prix_moyen - p.prix_moyen_prev) / p.prix_moyen_prev * 100.0) > 1000
        THEN NULL
        ELSE ROUND((g.prix_moyen - p.prix_moyen_prev) / p.prix_moyen_prev * 100.0, 2)
      END AS prix_moyen_var_pct,

      CASE
        WHEN p.surface_moyenne_prev IS NULL
             OR p.surface_moyenne_prev = 0
             OR ABS((g.surface_moyenne - p.surface_moyenne_prev) / p.surface_moyenne_prev * 100.0) > 1000
        THEN NULL
        ELSE ROUND((g.surface_moyenne - p.surface_moyenne_prev) / p.surface_moyenne_prev * 100.0, 2)
      END AS surface_moyenne_var_pct,

      CASE
        WHEN p.prix_moyen_m2_prev IS NULL
             OR p.prix_moyen_m2_prev = 0
             OR ABS((g.prix_moyen_m2 - p.prix_moyen_m2_prev) / p.prix_moyen_m2_prev * 100.0) > 1000
        THEN NULL
        ELSE ROUND((g.prix_moyen_m2 - p.prix_moyen_m2_prev) / p.prix_moyen_m2_prev * 100.0, 2)
      END AS prix_moyen_m2_var_pct,

      CASE
        WHEN p.prix_m2_mediane_prev IS NULL
             OR p.prix_m2_mediane_prev = 0
             OR ABS((q.prix_m2_mediane - p.prix_m2_mediane_prev) / p.prix_m2_mediane_prev * 100.0) > 1000
        THEN NULL
        ELSE ROUND((q.prix_m2_mediane - p.prix_m2_mediane_prev) / p.prix_m2_mediane_prev * 100.0, 2)
      END AS prix_m2_mediane_var_pct,

      CASE
        WHEN p.surface_mediane_prev IS NULL
             OR p.surface_mediane_prev = 0
             OR ABS((ms.surface_mediane - p.surface_mediane_prev) / p.surface_mediane_prev * 100.0) > 1000
        THEN NULL
        ELSE ROUND((ms.surface_mediane - p.surface_mediane_prev) / p.surface_mediane_prev * 100.0, 2)
      END AS surface_mediane_var_pct,

      CURRENT_TIMESTAMP AS last_refreshed

    FROM
    /* ======== Agrégats globaux (N) ======== */
    (
      SELECT
        code_dept,
        code_postal,
        commune,
        type_local,
        COUNT(*)                               AS nb_ventes,
        ROUND(AVG(prix_moyen),  2)            AS prix_moyen,
        ROUND(AVG(surface_moyenne),  2)       AS surface_moyenne,
        ROUND(SUM(prix_moyen)/NULLIF(SUM(surface_moyenne),0), 2) AS prix_moyen_m2
      FROM vf_staging_year
      GROUP BY code_dept, code_postal, commune, type_local
    ) AS g

    /* ======== Quartiles p2m (N) via PERCENTILE_CONT ======== */
    LEFT JOIN
    (
      SELECT
        code_dept,
        code_postal,
        commune,
        type_local,
        ROUND(prix_m2_q1, 2)      AS prix_m2_q1,
        ROUND(prix_m2_mediane, 2) AS prix_m2_mediane,
        ROUND(prix_m2_q3, 2)      AS prix_m2_q3
      FROM (
        SELECT
          code_dept,
          code_postal,
          commune,
          type_local,
          PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY p2m)
            OVER (PARTITION BY code_dept, code_postal, commune, type_local) AS prix_m2_q1,
          PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY p2m)
            OVER (PARTITION BY code_dept, code_postal, commune, type_local) AS prix_m2_mediane,
          PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY p2m)
            OVER (PARTITION BY code_dept, code_postal, commune, type_local) AS prix_m2_q3,
          ROW_NUMBER() OVER (
            PARTITION BY code_dept, code_postal, commune, type_local
            ORDER BY code_dept
          ) AS rn
        FROM (
          SELECT
            code_dept,
            code_postal,
            commune,
            type_local,
            (prix_moyen / NULLIF(surface_moyenne, 0)) AS p2m
          FROM vf_staging_year
        ) AS base_p2m
      ) AS z
      WHERE rn = 1
    ) AS q
      ON q.code_dept   = g.code_dept
     AND q.code_postal = g.code_postal
     AND q.commune     = g.commune
     AND q.type_local  = g.type_local

    /* ======== Médiane de surface (N) via PERCENTILE_CONT ======== */
    LEFT JOIN
    (
      SELECT
        code_dept,
        code_postal,
        commune,
        type_local,
        ROUND(surface_mediane, 2) AS surface_mediane
      FROM (
        SELECT
          code_dept,
          code_postal,
          commune,
          type_local,
          PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY surface_moyenne)
            OVER (PARTITION BY code_dept, code_postal, commune, type_local) AS surface_mediane,
          ROW_NUMBER() OVER (
            PARTITION BY code_dept, code_postal, commune, type_local
            ORDER BY code_dept
          ) AS rn
        FROM vf_staging_year
      ) AS y
      WHERE rn = 1
    ) AS ms
      ON ms.code_dept   = g.code_dept
     AND ms.code_postal = g.code_postal
     AND ms.commune     = g.commune
     AND ms.type_local  = g.type_local
    
	/* ======== Tranches surface (N) via PERCENTILE_CONT + p2m pondéré ======== */
    LEFT JOIN
    (
      SELECT
        code_dept,
        code_postal,
        commune,
        type_local,
        MAX(CASE WHEN bucket = 'S1' THEN prix_med END)  AS prix_med_s1,
        MAX(CASE WHEN bucket = 'S1' THEN surf_med END)  AS surf_med_s1,
        MAX(CASE WHEN bucket = 'S1' THEN p2m_w   END)   AS prix_m2_w_s1,
        MAX(CASE WHEN bucket = 'S2' THEN prix_med END)  AS prix_med_s2,
        MAX(CASE WHEN bucket = 'S2' THEN surf_med END)  AS surf_med_s2,
        MAX(CASE WHEN bucket = 'S2' THEN p2m_w   END)   AS prix_m2_w_s2,
        MAX(CASE WHEN bucket = 'S3' THEN prix_med END)  AS prix_med_s3,
        MAX(CASE WHEN bucket = 'S3' THEN surf_med END)  AS surf_med_s3,
        MAX(CASE WHEN bucket = 'S3' THEN p2m_w   END)   AS prix_m2_w_s3,
        MAX(CASE WHEN bucket = 'S4' THEN prix_med END)  AS prix_med_s4,
        MAX(CASE WHEN bucket = 'S4' THEN surf_med END)  AS surf_med_s4,
        MAX(CASE WHEN bucket = 'S4' THEN p2m_w   END)   AS prix_m2_w_s4,
        MAX(CASE WHEN bucket = 'S5' THEN prix_med END)  AS prix_med_s5,
        MAX(CASE WHEN bucket = 'S5' THEN surf_med END)  AS surf_med_s5,
        MAX(CASE WHEN bucket = 'S5' THEN p2m_w   END)   AS prix_m2_w_s5
      FROM (
        SELECT
          code_dept,
          code_postal,
          commune,
          type_local,
          bucket,
          /* médianes par bucket */
          ROUND(
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY prix)
              OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket),
            2
          ) AS prix_med,
          ROUND(
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY surf)
              OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket),
            2
          ) AS surf_med,
          /* p2m pondéré surface par bucket */
          ROUND(
            SUM(prix) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket)
            / NULLIF(
                SUM(surf) OVER (PARTITION BY code_dept, code_postal, commune, type_local, bucket),
                0
              ),
            2
          ) AS p2m_w,
          ROW_NUMBER() OVER (
            PARTITION BY code_dept, code_postal, commune, type_local, bucket
            ORDER BY code_dept
          ) AS rn
        FROM (
          SELECT
            v.code_dept,
            v.code_postal,
            v.commune,
            v.type_local,
            v.prix_moyen      AS prix,
            v.surface_moyenne AS surf,
            CASE
              WHEN v.surface_moyenne BETWEEN  0 AND 25 THEN 'S1'
              WHEN v.surface_moyenne BETWEEN 26 AND 35 THEN 'S2'
              WHEN v.surface_moyenne BETWEEN 36 AND 55 THEN 'S3'
              WHEN v.surface_moyenne BETWEEN 56 AND 85 THEN 'S4'
              WHEN v.surface_moyenne >= 86             THEN 'S5'
              ELSE NULL
            END AS bucket
          FROM vf_staging_year v
        ) AS b
        WHERE bucket IS NOT NULL
      ) AS t
      WHERE rn = 1
      GROUP BY
        code_dept,
        code_postal,
        commune,
        type_local
    ) AS pvt
      ON pvt.code_dept   = g.code_dept
     AND pvt.code_postal = g.code_postal
     AND pvt.commune     = g.commune
     AND pvt.type_local  = g.type_local

    /* ======== Agrégats année N-1 pour variations ======== */
    LEFT JOIN
    (
      SELECT
        a.code_dept,
        a.code_postal,
        a.commune,
        a.type_local,
        a.nb_ventes_prev,
        a.prix_moyen_prev,
        a.surface_moyenne_prev,
        a.prix_moyen_m2_prev,
        mpr.prix_m2_mediane_prev,
        msr.surface_mediane_prev
      FROM
      (
        SELECT
          b.code_dept,
          b.code_postal,
          b.commune,
          b.type_local,
          COUNT(*)                               AS nb_ventes_prev,
          ROUND(AVG(b.prix),  2)                 AS prix_moyen_prev,
          ROUND(AVG(b.surf),  2)                 AS surface_moyenne_prev,
          ROUND(SUM(b.prix)/NULLIF(SUM(b.surf),0), 2) AS prix_moyen_m2_prev
        FROM (
          SELECT
            v.code_dept,
            v.code_postal,
            v.commune,
            v.type_local,
            v.prix_moyen AS prix,
            v.surface_moyenne AS surf
          FROM vf_staging_year_prev v
        ) AS b
        GROUP BY b.code_dept, b.code_postal, b.commune, b.type_local
      ) AS a
      /* médiane p2m N-1 */
      LEFT JOIN (
        SELECT
          z.code_dept,
          z.code_postal,
          z.commune,
          z.type_local,
          ROUND((1-z.f)*p2m_lo + z.f*p2m_hi, 2) AS prix_m2_mediane_prev
        FROM (
          SELECT
            x.code_dept,
            x.code_postal,
            x.commune,
            x.type_local,
            x.n,
            (x.n-1)*0.50+1 AS pos,
            ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
            MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_lo,
            MAX(CASE WHEN x.rn = CEIL ((x.n-1)*0.50+1) THEN x.p2m END) AS p2m_hi
          FROM (
            SELECT
              b2.code_dept,
              b2.code_postal,
              b2.commune,
              b2.type_local,
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
                v.code_dept,
                v.code_postal,
                v.commune,
                v.type_local,
                v.prix_moyen AS prix,
                v.surface_moyenne AS surf
              FROM vf_staging_year_prev v
            ) AS b2
          ) AS x
          GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
        ) AS z
      ) AS mpr
        ON mpr.code_dept = a.code_dept
       AND mpr.code_postal = a.code_postal
       AND mpr.commune = a.commune
       AND mpr.type_local = a.type_local
      /* médiane surface N-1 */
      LEFT JOIN (
        SELECT
          y.code_dept,
          y.code_postal,
          y.commune,
          y.type_local,
          ROUND((1-y.f)*surf_lo + y.f*surf_hi, 2) AS surface_mediane_prev
        FROM (
          SELECT
            x.code_dept,
            x.code_postal,
            x.commune,
            x.type_local,
            x.n,
            (x.n-1)*0.50+1 AS pos,
            ((x.n-1)*0.50+1 - FLOOR((x.n-1)*0.50+1)) AS f,
            MAX(CASE WHEN x.rn = FLOOR((x.n-1)*0.50+1) THEN x.surf END) AS surf_lo,
            MAX(CASE WHEN x.rn = CEIL ((x.n-1)*0.50+1) THEN x.surf END) AS surf_hi
          FROM (
            SELECT
              b3.code_dept,
              b3.code_postal,
              b3.commune,
              b3.type_local,
              b3.surf,
              ROW_NUMBER() OVER (
                PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local
                ORDER BY b3.surf
              ) AS rn,
              COUNT(*) OVER (
                PARTITION BY b3.code_dept, b3.code_postal, b3.commune, b3.type_local
              ) AS n
            FROM (
              SELECT
                v.code_dept,
                v.code_postal,
                v.commune,
                v.type_local,
                v.surface_moyenne AS surf
              FROM vf_staging_year_prev v
            ) AS b3
          ) AS x
          GROUP BY x.code_dept, x.code_postal, x.commune, x.type_local, x.n
        ) AS y
      ) AS msr
        ON msr.code_dept = a.code_dept
       AND msr.code_postal = a.code_postal
       AND msr.commune = a.commune
       AND msr.type_local = a.type_local
    ) AS p
      ON p.code_dept = g.code_dept
     AND p.code_postal = g.code_postal
     AND p.commune = g.commune
     AND p.type_local = g.type_local
  ) AS src
  ON DUPLICATE KEY UPDATE
    nb_ventes        = src.nb_ventes,
    prix_moyen       = src.prix_moyen,
    surface_moyenne  = src.surface_moyenne,
    prix_moyen_m2    = src.prix_moyen_m2,
    prix_m2_q1       = src.prix_m2_q1,
    prix_m2_mediane  = src.prix_m2_mediane,
    prix_m2_q3       = src.prix_m2_q3,
    surface_mediane  = src.surface_mediane,
    prix_med_s1      = src.prix_med_s1,
    surf_med_s1      = src.surf_med_s1,
    prix_m2_w_s1     = src.prix_m2_w_s1,
    prix_med_s2      = src.prix_med_s2,
    surf_med_s2      = src.surf_med_s2,
    prix_m2_w_s2     = src.prix_m2_w_s2,
    prix_med_s3      = src.prix_med_s3,
    surf_med_s3      = src.surf_med_s3,
    prix_m2_w_s3     = src.prix_m2_w_s3,
    prix_med_s4      = src.prix_med_s4,
    surf_med_s4      = src.surf_med_s4,
    prix_m2_w_s4     = src.prix_m2_w_s4,
    prix_med_s5      = src.prix_med_s5,
    surf_med_s5      = src.surf_med_s5,
    prix_m2_w_s5     = src.prix_m2_w_s5,
    nb_ventes_var_pct       = src.nb_ventes_var_pct,
    prix_moyen_var_pct      = src.prix_moyen_var_pct,
    surface_moyenne_var_pct = src.surface_moyenne_var_pct,
    prix_moyen_m2_var_pct   = src.prix_moyen_m2_var_pct,
    prix_m2_mediane_var_pct = src.prix_m2_mediane_var_pct,
    surface_mediane_var_pct = src.surface_mediane_var_pct,
    last_refreshed          = src.last_refreshed;

END$$

DELIMITER ;