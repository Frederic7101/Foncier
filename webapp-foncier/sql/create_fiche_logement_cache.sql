-- Cache des réponses complètes de l'API fiche-logement (une entrée par commune, clé = code_insee).
-- Permet d'éviter de recalculer parc, ventes, locations, fiscalité et rentabilités à chaque requête.
-- Schéma : foncier (convention projet).
-- À exécuter une fois ; les données sont alimentées par l'API (écriture à la volée) ou par un script de refresh.
DROP TABLE IF EXISTS foncier.fiche_logement_cache;
CREATE TABLE IF NOT EXISTS foncier.fiche_logement_cache (
  code_insee   TEXT PRIMARY KEY,
  payload      JSONB NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE foncier.fiche_logement_cache IS 'Cache des réponses GET /api/fiche-logement par code_insee. Rempli à la volée ou par script de refresh.';
COMMENT ON COLUMN foncier.fiche_logement_cache.payload IS 'Réponse JSON complète (parc, ventes, locations, fiscalite, rentabilite_mediane, rentabilite_moyenne, + loypredm2 et prix_m2_moyenne pour la comparaison).';
