-- Fenêtres DVF 1–5 ans : snapshots JSON par période + nb ventes agrégées (migration).
-- Schéma : foncier.

ALTER TABLE foncier.indicateurs_communes ADD COLUMN IF NOT EXISTS nb_ventes_dvf INTEGER;
ALTER TABLE foncier.indicateurs_communes ADD COLUMN IF NOT EXISTS indicateurs_par_periode JSONB;

ALTER TABLE foncier.indicateurs_depts ADD COLUMN IF NOT EXISTS nb_ventes_dvf BIGINT;
ALTER TABLE foncier.indicateurs_depts ADD COLUMN IF NOT EXISTS indicateurs_par_periode JSONB;

ALTER TABLE foncier.indicateurs_regions ADD COLUMN IF NOT EXISTS nb_ventes_dvf BIGINT;
ALTER TABLE foncier.indicateurs_regions ADD COLUMN IF NOT EXISTS indicateurs_par_periode JSONB;

COMMENT ON COLUMN foncier.indicateurs_communes.indicateurs_par_periode IS 'Par clé "1"|"2"|"3"|"5" : rentas, nb_locaux, nb_ventes_dvf (fenêtre DVF).';
COMMENT ON COLUMN foncier.indicateurs_depts.indicateurs_par_periode IS 'Agrégat départemental des snapshots commune (même structure).';
COMMENT ON COLUMN foncier.indicateurs_regions.indicateurs_par_periode IS 'Agrégat régional des snapshots commune (même structure).';
