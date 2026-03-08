-- Vue vw_vf_communes non utilisée : on consulte vf_communes (table pré-calculée par
-- sp_refresh_vf_communes_agg), plus riche et plus rapide. Ce script supprime la vue
-- si elle existe (éviter qu'elle soit utilisée par erreur).

DROP VIEW IF EXISTS vw_vf_communes;
