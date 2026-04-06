-- Rentabilités par type de local (DVF) : parkings/dépendances, locaux pro, terrains, immeubles.
-- À exécuter une fois sur la base PostgreSQL du projet.

ALTER TABLE foncier.indicateurs_communes
  ADD COLUMN IF NOT EXISTS renta_brute_parking NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_parking NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_local_indus NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_local_indus NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_terrain NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_terrain NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_immeuble NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_immeuble NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS nb_locaux_parking INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_local_indus INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_terrain INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_immeuble INTEGER;

ALTER TABLE foncier.indicateurs_depts
  ADD COLUMN IF NOT EXISTS renta_brute_parking NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_parking NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_local_indus NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_local_indus NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_terrain NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_terrain NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_immeuble NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_immeuble NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS nb_locaux_parking INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_local_indus INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_terrain INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_immeuble INTEGER;

ALTER TABLE foncier.indicateurs_regions
  ADD COLUMN IF NOT EXISTS renta_brute_parking NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_parking NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_local_indus NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_local_indus NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_terrain NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_terrain NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_brute_immeuble NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS renta_nette_immeuble NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS nb_locaux_parking INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_local_indus INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_terrain INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_immeuble INTEGER;

-- Patterns pour regrouper foncier.vf_communes.type_local (libellés DVF) + libellé d'affichage
UPDATE foncier.ref_type_logts SET libelle = 'Dépendances', type_local_pattern = '%Dépendance%' WHERE code = 'PARKING';
UPDATE foncier.ref_type_logts SET type_local_pattern = '%Local%industriel%' WHERE code = 'LOCAL_INDUS';
UPDATE foncier.ref_type_logts SET type_local_pattern = '%Terrain%' WHERE code = 'TERRAIN';
UPDATE foncier.ref_type_logts SET type_local_pattern = '%Immeuble%' WHERE code = 'IMMEUBLE';

COMMENT ON COLUMN foncier.indicateurs_communes.renta_nette_parking IS 'Rentabilité nette estimée pour les Dépendances (type_local DVF) — loyer proxy = appartements ANIL';
