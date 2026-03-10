## Suivi des campagnes de tests

Ce dossier contient un **modèle de définition de campagne**, des fichiers de **suivi d'exécution** et un exemple de **visualisation** simple en HTML/JS.

### 1. Définition d’une campagne

- `modele_campagne.yaml` ou `modele_campagne.json` :  
  - nom de la campagne ;  
  - date de début / fin ;  
  - périmètre (liste de cas de tests ou lots) ;  
  - références (plan de tests, version de l’application).  

### 2. Suivi d’exécution

- `modele_suivi_exec.csv` : chaque ligne correspond à l’exécution d’un cas de test.  
- Champs minimaux : ID cas, statut (PASS/FAIL/BLOCKED), exécutant, date, commentaire.  

### 3. Visualisation HTML/JS

- `dashboard_suivi.html` : lit un fichier JSON de résultats et affiche des indicateurs simples :
  - nombre total de cas ;  
  - répartition PASS/FAIL/BLOCKED ;  
  - tableau détaillé.  

Il est possible de générer le JSON de résultats à partir du CSV via un script Python ou un outil externe, puis d’ouvrir la page HTML localement.

