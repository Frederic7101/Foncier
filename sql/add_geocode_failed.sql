-- Migration : marquer les adresses pour lesquelles BAN ne renvoie rien,
-- afin que geocode_ban.py ne les retraite pas à chaque exécution.
-- À exécuter une fois sur une base PostgreSQL existante (schéma ventes_notaire).
--
-- Exemple :
--   psql -U postgres -d foncier -f sql/add_geocode_failed.sql
-- (à la racine du projet, ou avec chemin absolu vers le fichier)

SET search_path TO ventes_notaire;

ALTER TABLE adresses_geocodees
  ADD COLUMN IF NOT EXISTS geocode_failed SMALLINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN adresses_geocodees.geocode_failed IS '1 = BAN interrogé sans résultat (ne plus retraiter)';
