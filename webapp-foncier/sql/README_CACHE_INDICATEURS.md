# Cache fiche logement et indicateurs communes

## Tables

- **foncier.fiche_logement_cache** : cache des réponses complètes de l’API `GET /api/fiche-logement` (une entrée par `code_insee`). Réduit le temps de réponse des fiches commune.
- **foncier.indicateurs_communes** : indicateurs précalculés par commune (rentabilité, fiscalité) pour la page comparaison des scores. Permet d’afficher la comparaison sans recalculer pour chaque commune.

## Création des tables

Exécuter dans l’ordre (PostgreSQL, schéma `foncier`) :

```bash
psql -f create_fiche_logement_cache.sql
psql -f create_indicateurs_communes.sql
```

Ou depuis un client SQL :

```sql
\i create_fiche_logement_cache.sql
\i create_indicateurs_communes.sql
```

## Remplissage

- **Automatique (à la volée)** : la première requête vers une commune remplit le cache fiche et, en mode comparaison, la table indicateurs. Les requêtes suivantes lisent le cache.
- **Rafraîchissement en masse** : appeler l’API `POST /api/refresh-indicateurs` pour préremplir les caches.
  - Sans paramètre : traite jusqu’à 500 communes (une par `code_insee` dans `ref_communes`).
  - Avec `code_insee_list` : traite uniquement les code_insee fournis.
  - Avec `limit` : max communes (ex. 500, max 50000). Absent ou 0 = toutes.
  - Avec `workers` : nombre de workers parallèles pour le calcul des fiches (défaut 4, max 16). Accélère le remplissage du cache fiche.

Exemple :

```cmd
curl -X POST "http://localhost:8000/api/refresh-indicateurs"
curl -X POST "http://localhost:8000/api/refresh-indicateurs?limit=500&workers=8"
```

## Prérequis

- `ref_communes` doit contenir au moins : `code_insee`, `dep_code` (ou `code_dept` selon schéma), `code_postal`, `nom_standard` / `nom_standard_majuscule`, et si possible `population`.
- Les tables `ref_departements` et `ref_regions` sont utilisées pour les libellés et la liste des communes par département/région.
