-- Ajoute nb_locaux : effectif (parc DVF ou nb ventes) sur lequel repose la renta agrégée Maisons/Appart.
-- À exécuter sur une base existante (les CREATE TABLE initiaux peuvent être mis à jour séparément).

ALTER TABLE foncier.indicateurs_communes ADD COLUMN IF NOT EXISTS nb_locaux INTEGER;
COMMENT ON COLUMN foncier.indicateurs_communes.nb_locaux IS 'Nb de logements (parc ou ventes DVF) pour la ligne renta agrégée Maisons/Appart.';

ALTER TABLE foncier.indicateurs_depts ADD COLUMN IF NOT EXISTS nb_locaux BIGINT;
COMMENT ON COLUMN foncier.indicateurs_depts.nb_locaux IS 'Somme des nb_locaux des communes du département (indicateurs valides).';

ALTER TABLE foncier.indicateurs_regions ADD COLUMN IF NOT EXISTS nb_locaux BIGINT;
COMMENT ON COLUMN foncier.indicateurs_regions.nb_locaux IS 'Somme des nb_locaux des communes de la région (indicateurs valides).';
