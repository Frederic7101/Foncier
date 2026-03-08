# Audit tables, vues, index, procédures (foncier / ventes)

État des lieux pour repérer l’inutile et les incohérences.

---

## 1. Tables

| Table | Rôle | Statut |
|-------|------|--------|
| **valeursfoncieres** | Données brutes DVF | ✅ Utilisée (source) |
| **adresses_geocodees** | Cache géocodage BAN | ✅ Utilisée (étape 1) |
| **vf_all_ventes** | Agrégat par transaction (lieu + date + type_local) | ✅ Utilisée (étape 2 → 3) |
| **vf_communes** | Agrégat commune × année × type_local (quartiles, S/T, variations) | ✅ Utilisée (étape 3) |
| **vf_staging_year** / **vf_staging_year_prev** | Staging (MySQL/foncier_schema, migrate) | ⚠️ Aucun script sql/ ne les crée ni ne les utilise. Probables reliquats. |
| **valeursfoncieres_agg** | Remplie par sp_refresh_valeursfoncieres_agg (dump) | ❌ Table absente du schéma actuel ; procédure obsolète (on utilise vf_all_ventes + vf_communes). |
| **vf_all_ventes_staging** | Temporaire dans 02_refresh_vf_all_ventes_fast.sql | ✅ OK (créée/détruite par le script). |
| **licitor_annonces** | schema_encheres.sql | Autre projet (enchères), hors flux foncier. |

**Incohérence de schéma** : le dump et MySQL/foncier_schema ont **vf_all_ventes** avec l’ancienne structure (nb_mutations, prix_moyen, surface_moyenne, prix_moyen_m2). La **structure cible** est dans **02_refresh_vf_all_ventes.sql** (nb_lignes, valeur_fonciere, surface_reelle_bati, adresse_norm, latitude, longitude, etc.). Après 02_alter + 02_refresh, la table en base a la nouvelle structure.

---

## 2. Vues

| Vue | Statut |
|-----|--------|
| **vw_vf_communes** | ❌ Non utilisée → supprimée (04_recreate_vw_vf_communes.sql = DROP). |
| **vw_valeursfoncieres_agg_YYYY** | ❌ Non utilisées → supprimées (05_drop_vw_valeursfoncieres_agg.sql). |

**PostgreSQL** : `sql/postgresql/02_recreate_views.sql` recrée encore ces deux vues. Pour rester cohérent avec la décision MySQL, il faudrait ne plus les recréer (ou ne faire que les DROP). Voir section 6 ci‑dessous.

---

## 3. Procédures stockées

| Procédure | Fichier | Statut |
|-----------|---------|--------|
| **sp_refresh_vf_communes_agg** (+ all, by_dept, dept_years, postal_years) | 03_sp_refresh_vf_communes_agg.sql | ✅ Utilisées (remplissent vf_communes complète). |
| **sp_refresh_vf_communes** (sans _agg) | 03_sp_refresh_vf_communes.sql | ⚠️ **Redondante** : ne remplit que 5 colonnes (nb_ventes, prix_moyen, surface_moyenne, prix_moyen_m2, last_refreshed). La table vf_communes a bien plus de colonnes (quartiles, S/T, variations) ; elles resteraient NULL. À considérer dépréciée ou à supprimer. |
| **sp_refresh_valeursfoncieres_agg** | dump-foncier (pas dans sql/) | ❌ **Obsolète** : insère dans valeursfoncieres_agg (table absente du schéma). Remplacée par vf_all_ventes + vf_communes. |
| **sp_refresh_vf_all_ventes_year** / **sp_refresh_vf_all_ventes_fast** | 02_refresh_vf_all_ventes_fast_by_year.sql | ✅ Alternatives pour remplir vf_all_ventes (par année ou “fast”). |
| **sp_init_adresses_geocodees_by_year** | sp_init_adresses_geocodees_by_year.sql | ✅ Utilisée (plan étape 1.1). |

---

## 4. Index

- **valeursfoncieres** : idx sur type_local, code_departement, code_postal, commune, date_mutation, (latitude, longitude). Cohérents avec les filtres des refresh et du géocodage.
- **vf_all_ventes** : ux_vf_agg (ou équivalent avec adresse_norm dans 02_refresh), idx sur date_mutation + type + lieu, annee + type + lieu. OK.
- **vf_communes** : ux_communes_agg (annee, code_dept, code_postal, commune, type_local). OK.
- **adresses_geocodees** : PK, ux sur adresse_norm. OK.

Rien d’inutile repéré côté index.

---

## 5. Scripts / plan

- **03_refresh_vf_communes.sql** : appelle `sp_refresh_vf_communes_all`. Exemple en commentaire `CALL sp_refresh_vf_communes_agg(2024)` à mettre à jour en `CALL sp_refresh_vf_communes_agg(2024, NULL, NULL);`.
- **PLAN_FINALISATION_BDD.md** (étape 3) : indique “INSERT … SELECT avec ON DUPLICATE KEY UPDATE” alors que le script fait TRUNCATE + CALL. À aligner sur “TRUNCATE puis CALL sp_refresh_vf_communes_all (procédures dans 03_sp_refresh_vf_communes_agg.sql)”.

---

## 6. Actions recommandées

1. **Procédure sp_refresh_vf_communes** : documenter comme dépréciée (ou supprimer 03_sp_refresh_vf_communes.sql) et n’utiliser que sp_refresh_vf_communes_agg.
2. **PostgreSQL 02_recreate_views.sql** : ne plus recréer vw_vf_communes ni vw_valeursfoncieres_agg_2020 ; au choix : uniquement DROP VIEW, ou commenter les CREATE et noter qu’on ne les utilise pas (comme en MySQL).
3. **03_refresh_vf_communes.sql** : corriger l’exemple d’appel (année, NULL, NULL).
4. **PLAN_FINALISATION_BDD.md** : corriger la description de l’étape 3.
5. **Optionnel** : **`sql/06_drop_obsolete_objects.sql`** — DROP des objets obsolètes en base si présents (sp_refresh_valeursfoncieres_agg, sp_refresh_vf_communes, vf_staging_*, valeursfoncieres_agg). À exécuter à la main si besoin de nettoyer une base existante.

---

*Rapport généré pour cohérence du schéma et des scripts.*
