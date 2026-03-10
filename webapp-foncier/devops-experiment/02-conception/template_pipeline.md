## Template de conception de pipeline de données

### 1. Vue globale

- **Source(s) de données** :  
- **Objectif du pipeline** (agrégation, nettoyage, géocodage, etc.) :  
- **Fréquence d’exécution** (batch, temps réel, événementiel) :  

### 2. Étapes du pipeline

1. **Ingestion**  
   - format source (CSV, API, base distante) ;  
   - contrôles de cohérence.  

2. **Nettoyage / normalisation**  
   - règles d’enrichissement et de correction ;  
   - déduplication éventuelle.  

3. **Stockage intermédiaire**  
   - tables de staging / buffers.  

4. **Transformations / agrégations**  
   - tables cibles ;  
   - indicateurs calculés (somme, moyenne, médiane, quantiles, etc.).  

5. **Exposition**  
   - API, vues, exports.  

### 3. Exemple simplifié – DVF

- **Source** : fichiers DVF ;  
- **Étapes** :
  - chargement dans `valeursfoncieres` ;  
  - géocodage via `adresses_geocodees` (scripts Python BAN) ;  
  - agrégation dans `vf_all_ventes` et `vf_communes` ;  
  - exposition via l’API FastAPI (voir `webapp-foncier/backend/main.py`) et l’IHM Stats.  

### 4. Opérations techniques

- Scripts SQL (création de tables, vues, procédures, triggers) ;  
- Scripts Python / shell (ETL, appels externes, géocodage) ;  
- Planificateur (cron, ordonnanceur, outil ETL).  

