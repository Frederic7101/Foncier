-- À exécuter une seule fois quand vf_all_ventes a encore l’ancienne structure
-- (colonnes nb_mutations, prix_moyen, surface_moyenne, prix_moyen_m2).
-- À ignorer si la table n’existe pas ou a déjà la nouvelle structure.

-- 1) Supprimer l’ancienne clé unique
ALTER TABLE vf_all_ventes DROP INDEX ux_vf_agg;

-- 2) Ajouter les nouvelles colonnes
ALTER TABLE vf_all_ventes
  ADD COLUMN adresse_norm           VARCHAR(512) DEFAULT NULL COMMENT 'Adresse normalisée (lieu de la transaction)' AFTER date_mutation,
  ADD COLUMN nb_lignes              INT          NULL COMMENT 'Nombre de lignes agrégées' AFTER no_voie,
  ADD COLUMN valeur_fonciere        DECIMAL(15,2) DEFAULT NULL COMMENT 'Moyenne valeur_fonciere' AFTER nb_lignes,
  ADD COLUMN surface_reelle_bati    DECIMAL(15,2) DEFAULT NULL COMMENT 'Moyenne surface_reelle_bati' AFTER valeur_fonciere,
  ADD COLUMN prix_m2                DECIMAL(15,2) DEFAULT NULL COMMENT 'Prix/m² pondéré' AFTER surface_reelle_bati,
  ADD COLUMN nombre_pieces_principales INT       DEFAULT NULL AFTER prix_m2,
  ADD COLUMN latitude               DOUBLE       DEFAULT NULL AFTER nombre_pieces_principales,
  ADD COLUMN longitude              DOUBLE       DEFAULT NULL AFTER latitude;

-- 3) Supprimer les anciennes colonnes
ALTER TABLE vf_all_ventes
  DROP COLUMN nb_mutations,
  DROP COLUMN prix_moyen,
  DROP COLUMN surface_moyenne,
  DROP COLUMN prix_moyen_m2;

-- 4) Nouvelle clé unique et index
ALTER TABLE vf_all_ventes
  ADD UNIQUE KEY ux_vf_agg (annee, type_local, code_dept, code_postal, commune, date_mutation, adresse_norm(255)),
  ADD KEY idx_vf_all_date_type_commune (date_mutation, type_local, code_dept, code_postal, commune),
  ADD KEY idx_vf_all_annee_type_commune (annee, type_local, code_dept, code_postal, commune);

-- 5) Rendre nb_lignes NOT NULL (après remplissage des données, ou laisser NULL jusqu’au premier refresh)
-- ALTER TABLE vf_all_ventes MODIFY COLUMN nb_lignes INT NOT NULL;
