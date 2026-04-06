-- Phase 1 : ajout colonnes nb_locaux_maisons et nb_locaux_appts sur les 3 tables indicateurs.
-- Ces colonnes permettent d'afficher le nb de locaux vendus exact quand l'utilisateur
-- choisit le niveau détaillé "type de local" (Maisons ou Appartements) dans comparaison_scores.
-- Additive : la colonne nb_locaux globale est conservée ; les nouvelles colonnes seront NULL
-- jusqu'au prochain refresh des indicateurs.

ALTER TABLE foncier.indicateurs_communes
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons INTEGER,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts   INTEGER;

ALTER TABLE foncier.indicateurs_depts
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts   BIGINT;

ALTER TABLE foncier.indicateurs_regions
  ADD COLUMN IF NOT EXISTS nb_locaux_maisons BIGINT,
  ADD COLUMN IF NOT EXISTS nb_locaux_appts   BIGINT;
