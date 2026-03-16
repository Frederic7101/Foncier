-- Recalcule uniquement les 6 indicateurs de variation N-1 (_var_pct) dans vf_communes.
-- Utilise les lignes déjà présentes : pour chaque (code_dept, code_postal, commune, type_local, annee),
-- on joint la ligne d'année N-1 et on met à jour les colonnes *_var_pct.
-- À exécuter après un refresh partiel ou si seule la formule des variations a changé.
--
-- Optionnel : restreindre par plage d'années en décommentant la clause AND c.annee BETWEEN ... dans le WHERE.

UPDATE vf_communes c
INNER JOIN vf_communes p
  ON p.code_dept = c.code_dept
  AND p.code_postal = c.code_postal
  AND p.commune = c.commune
  AND p.type_local = c.type_local
  AND p.annee = c.annee - 1
SET
  c.nb_ventes_var_pct       = LEAST(GREATEST(ROUND((c.nb_ventes       - IFNULL(p.nb_ventes,0))        / NULLIF(p.nb_ventes,0)        * 100, 2), -9999999.99), 9999999.99),
  c.prix_moyen_var_pct      = LEAST(GREATEST(ROUND((c.prix_moyen      - IFNULL(p.prix_moyen,0))       / NULLIF(p.prix_moyen,0)       * 100, 2), -9999999.99), 9999999.99),
  c.surface_moyenne_var_pct = LEAST(GREATEST(ROUND((c.surface_moyenne - IFNULL(p.surface_moyenne,0))  / NULLIF(p.surface_moyenne,0)  * 100, 2), -9999999.99), 9999999.99),
  c.prix_moyen_m2_var_pct   = LEAST(GREATEST(ROUND((c.prix_m2_moyenne - IFNULL(p.prix_m2_moyenne,0))  / NULLIF(p.prix_m2_moyenne,0)  * 100, 2), -9999999.99), 9999999.99),
  c.prix_m2_mediane_var_pct = LEAST(GREATEST(ROUND((c.prix_m2_mediane - IFNULL(p.prix_m2_mediane,0))  / NULLIF(p.prix_m2_mediane,0)  * 100, 2), -9999999.99), 9999999.99),
  c.surface_mediane_var_pct = LEAST(GREATEST(ROUND((c.surface_mediane - IFNULL(p.surface_mediane,0))  / NULLIF(p.surface_mediane,0)  * 100, 2), -9999999.99), 9999999.99)
-- WHERE c.annee BETWEEN 2021 AND 2025;
