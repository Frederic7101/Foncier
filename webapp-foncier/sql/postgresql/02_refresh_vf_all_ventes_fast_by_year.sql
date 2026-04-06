-- Étape 2 (version rapide, par année/batch) : refresh vf_all_ventes via procédure stockée.
-- Staging sans index, INSERT par année (partition pruning) et optionnellement par batch de N mois.
-- Modification : 06/04/2026 ajout des autres types de locaux pour récupérer toutes les ventes de vf_all_ventes et les visualiser dans le FO + conversion sous PostgreSQL 

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

DROP FUNCTION IF EXISTS sp_refresh_vf_all_ventes_year;

CREATE  OR REPLACE FUNCTION sp_refresh_vf_all_ventes_year(
  IN p_annee              	integer,
  IN p_nb_mois_par_batch   	smallint DEFAULT 12
) RETURNS void AS
$$
DECLARE 
  v_mois      smallint := 1;
  v_date_deb  DATE;
  v_date_fin  DATE;

BEGIN
  -- Normalisation du paramètre de batch
  IF p_nb_mois_par_batch IS NULL OR p_nb_mois_par_batch = 0 THEN
    p_nb_mois_par_batch := 12;
  END IF;
  IF p_nb_mois_par_batch > 12 THEN
    p_nb_mois_par_batch := 12;
  END IF;

  WHILE v_mois <= 12 LOOP
	-- 1er jour du mois en cours
    v_date_deb := MAKE_DATE(p_annee,v_mois,1);
    v_date_fin := v_date_deb + (p_nb_mois_par_batch || ' month')::interval;
	
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
      EXTRACT(YEAR FROM v.date_mutation)::int,
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
      AND v.type_local IN ('Appartement', 'Maison', 'Dépendance', 'Local industriel. commercial ou assimilé')
      AND v.date_mutation >= v_date_deb
      AND v.date_mutation < v_date_fin
    GROUP BY
      v.code_departement,
      v.code_postal,
      v.commune,
      v.type_local,
      v.date_mutation,
      CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune);

	-- Passage au mois suivant
    v_mois := v_mois + p_nb_mois_par_batch;
  END LOOP;
END$$
LANGUAGE plpgsql;

-- Refresh complet : staging, remplissage par années (et par batch de mois), index, swap.
-- 1️⃣ Suppression de l’ancienne fonction (si elle existe)
DROP FUNCTION IF EXISTS sp_refresh_vf_all_ventes_fast(
    p_annee_min integer,
    p_annee_max integer,
    p_nb_mois_par_batch smallint
);

-- 2️⃣ Création de la fonction qui remplace la procédure MySQL
CREATE OR REPLACE FUNCTION sp_refresh_vf_all_ventes_fast(
    p_annee_min           integer,
    p_annee_max           integer,
    p_nb_mois_par_batch   smallint DEFAULT 12   -- valeur par défaut si NULL ou 0
) RETURNS void AS
$$
DECLARE
    v_annee     integer;
    v_tbl_count integer;
BEGIN
    --------------------------------------------------------------------
    -- Normalisation du paramètre de batch
    --------------------------------------------------------------------
    IF p_nb_mois_par_batch IS NULL OR p_nb_mois_par_batch = 0 THEN
        p_nb_mois_par_batch := 12;
    END IF;
    IF p_nb_mois_par_batch > 12 THEN
        p_nb_mois_par_batch := 12;
    END IF;

    --------------------------------------------------------------------
    -- Validation des bornes d’année
    --------------------------------------------------------------------
    IF p_annee_min > p_annee_max THEN
        RAISE EXCEPTION 'p_annee_min doit être <= p_annee_max';
    END IF;

    --------------------------------------------------------------------
    -- 1) Recréer la table de staging (sans index)
    --------------------------------------------------------------------
    DROP TABLE IF EXISTS vf_all_ventes_new;

    CREATE TABLE vf_all_ventes_new (
        code_dept              VARCHAR(5)   NOT NULL,
        code_postal            VARCHAR(10)  NOT NULL,
        commune                VARCHAR(100) NOT NULL,
        type_local             VARCHAR(50)  NOT NULL,
        annee                  INTEGER      NOT NULL,
        date_mutation          DATE         NOT NULL,
        adresse_norm           VARCHAR(512),
        voie                   VARCHAR(255),
        no_voie                VARCHAR(20),
        nb_lignes              INTEGER      NOT NULL,
        valeur_fonciere        NUMERIC(15,2),
        surface_reelle_bati    NUMERIC(15,2),
        prix_m2                NUMERIC(15,2),
        nombre_pieces_principales INTEGER,
        latitude               DOUBLE PRECISION,
        longitude              DOUBLE PRECISION,
        last_refresh           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    --------------------------------------------------------------------
    -- 2) Boucle sur chaque année demandée
    --------------------------------------------------------------------
    v_annee := p_annee_min;
    WHILE v_annee <= p_annee_max LOOP
        PERFORM refresh_one_year(v_annee, p_nb_mois_par_batch);
        v_annee := v_annee + 1;
    END LOOP;
END;
$$
LANGUAGE plpgsql;
