## Exemple de plan de tests – Webapp DVF

### 1. Objet et périmètre

- **Application** : webapp de statistiques foncières (DVF).  
- **Objectifs** :
  - vérifier la bonne exposition des endpoints API principaux ;  
  - valider le comportement de l’écran Stats (vue simple, comparaison, superposition) ;  
  - vérifier le retour des ventes autour d’un point.  

### 2. Types de tests

- **Unitaires** :
  - fonctions d’agrégation `_agg_rows` dans `webapp-foncier/backend/main.py` (à terme) ;  
  - endpoints simples ne nécessitant pas la BDD (ex. `/health`).  
- **Intégration** :
  - appels aux endpoints `/api/period`, `/api/geo`, `/api/communes`, `/api/stats`, `/api/ventes` avec une base de test.  
- **Fonctionnels** :
  - parcours utilisateur sur l’écran Stats (sélection de commune, affichage des graphiques).  

### 3. Cas de tests (extraits)

| ID | Type | Titre | Résultat attendu | Priorité |
|----|------|-------|------------------|----------|
| CT-API-001 | Intégration | Vérifier `/health` | Statut HTTP 200 et corps `{\"status\": \"ok\"}` | Must |
| CT-API-002 | Intégration | Vérifier `/api/period` | Réponse 200 avec `annee_min` ≤ `annee_max` | Should |
| CT-IHM-001 | Fonctionnel | Affichage Stats commune unique | Graphiques et indicateurs cohérents pour la commune sélectionnée | Must |

Les cas détaillés peuvent être suivis dans les fichiers de campagne de tests (voir `suivi-campagnes/`).  

