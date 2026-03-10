Conception – IHM, modèle de données et pipeline d’alimentation
==============================================================

1. Objet du document
--------------------

Ce document décrit la **conception technique** de l’application de statistiques foncières :

- architecture générale (frontend, backend, base de données) ;
- modèle conceptuel des données (tables principales, clés, relations) ;
- procédures, triggers et scripts d’agrégation ;
- pipeline d’alimentation depuis la source DVF jusqu’à l’IHM.

2. Architecture générale
------------------------

2.1 Composants

- **Base de données** : PostgreSQL 18, base `foncier`, schéma `ventes_notaire`.  
  - Tables sources et agrégées : `valeursfoncieres`, `adresses_geocodees`, `vf_all_ventes`, `vf_communes`, tables de référence optionnelles (`ref_regions`, `ref_departements`, `ref_communes`).  

- **Backend API** : FastAPI (`webapp-foncier/backend/main.py`).  
  - Couche d’accès PostgreSQL (`psycopg2`, `RealDictCursor`).  
  - Endpoints :
    - `/api/period` : bornes années disponibles.  
    - `/api/geo` : régions + départements présents dans `vf_communes`.  
    - `/api/communes` : liste des communes (filtrées ou globales).  
    - `/api/stats` : agrégats `vf_communes` (global + série annuelle).  
    - `/api/ventes` : ventes détaillées autour d’un point géographique (table `valeursfoncieres`).  

- **Frontend** :  
  - Fichiers : `webapp-foncier/frontend/stats.html`, `stats.js`, `stats.css`.  
  - Rôles :
    - gestion du formulaire (choix de lieu, comparaison, type, période) ;  
    - appels aux endpoints backend ;  
    - construction dynamique des listes (`<select>`) et des graphiques (Chart.js) ;  
    - logique de comparaison et superposition des métriques.

2.2 Flux de données

1. L’utilisateur sélectionne des critères dans l’IHM.  
2. Le frontend appelle l’API (`/api/geo`, `/api/communes`, `/api/period`, `/api/stats`).  
3. L’API interroge les tables agrégées (`vf_communes`) via des requêtes SQL paramétrées.  
4. Les réponses JSON sont utilisées par le frontend pour :
   - remplir listes et titres ;  
   - alimenter les graphiques (courbes par année).  

3. Modèle de données (MCD simplifié)
------------------------------------

3.1 Entités principales

### 3.1.1 `valeursfoncieres`

- **Rôle** : table de **détail DVF**, chaque ligne correspond à une mutation (vente) ou à une fraction de mutation.  
- **Champs principaux (simplifiés)** :
  - `id` (PK, entier) ;  
  - `code_departement` (varchar) ;  
  - `code_postal` (varchar) ;  
  - `commune` (varchar) ;  
  - `date_mutation` (date) ;  
  - `nature_mutation` (varchar, ex. "Vente") ;  
  - `type_local` (varchar, ex. "Appartement", "Maison") ;  
  - `valeur_fonciere` (numeric) ;  
  - `surface_reelle_bati` (numeric, nullable) ;  
  - `surface_terrain` (numeric, nullable) ;  
  - `nombre_pieces_principales` (int, nullable) ;  
  - `voie`, `type_de_voie`, `no_voie` (info adresse) ;  
  - `adresse_norm` (adresse normalisée) ;  
  - `latitude`, `longitude` (float, nullable) ;  
  - `dedup_key_hash_calc` (texte, clé de déduplication calculée par trigger).  

- **Clé primaire** : `id`.  

### 3.1.2 `adresses_geocodees`

- **Rôle** : buffer pour le géocodage BAN.  
- **Champs principaux** (schéma exact dans scripts SQL) :
  - identifiants d’adresse (adresse brute, code postal, commune, etc.) ;  
  - `latitude`, `longitude` ;  
  - éventuels statuts de géocodage (succès/erreur, date de traitement).  
- **Clés** :  
  - PK technique (ex. `id`) ;  
  - combinaisons d’attributs d’adresse pour éviter les doublons.

### 3.1.3 `vf_all_ventes`

- **Rôle** : agrégation transactionnelle de `valeursfoncieres`.  
- Chaque ligne représente une **vente agrégée** par combinaison :  
  - `(code_departement, code_postal, commune, adresse_norm, date_mutation, type_local)`.  
- **Champs principaux** :
  - `nb_lignes` : nombre de lignes sources DVF agrégées ;  
  - `valeur_fonciere` : moyenne de la valeur foncière sur ces lignes ;  
  - `surface_reelle_bati` : moyenne ;  
  - `prix_m2` : prix moyen au m² ;  
  - `nombre_pieces_principales` : moyenne ;  
  - `latitude`, `longitude` : coordonnées moyennes (ou représentatives) ;  
  - `adresse_norm`.  

### 3.1.4 `vf_communes`

- **Rôle** : table d’agrégats pré-calculés pour l’IHM.  
- **Clé logique** :
  - `annee` (int) ;  
  - `code_dept` (varchar) ;  
  - `code_postal` (varchar) ;  
  - `commune` (varchar) ;  
  - `type_local` (varchar).  
- **Champs principaux** :
  - `nb_ventes` (int) ;  
  - `prix_moyen`, `prix_q1`, `prix_median`, `prix_q3` ;  
  - `surface_moyenne`, `surface_mediane` ;  
  - `prix_m2_moyenne`, `prix_m2_q1`, `prix_m2_mediane`, `prix_m2_q3`.  
- **Champs par catégories de surface** (S1..S5) :
  - `prix_med_s1`, `surf_med_s1`, `prix_m2_w_s1`, …, `prix_med_s5`, `surf_med_s5`, `prix_m2_w_s5`.  
- **Champs par catégories de pièces** (T1..T5) :
  - `prix_med_T1`, `surf_med_T1`, `prix_m2_w_T1`, …, `prix_med_T5`, `surf_med_T5`, `prix_m2_w_T5`.  

### 3.1.5 Tables de référence

- **`ref_regions`**  
  - PK : `code_region`.  
  - Champs : `nom_region`, etc.  

- **`ref_departements`**  
  - PK : `code_dept`.  
  - FK : `code_region` → `ref_regions.code_region`.  

- **`ref_communes`** (optionnelle)  
  - Champs : `code_dept`, `code_postal`, `commune` ;  
  - utilisée en priorité par `/api/communes` si disponible.

3.2 Relations principales

- `ref_regions` (1) — (N) `ref_departements` par `code_region`.  
- `ref_departements` (1) — (N) `vf_communes` par `code_dept`.  
- `vf_all_ventes` (agrégé à partir de) `valeursfoncieres` (via scripts SQL).  
- `vf_communes` (agrégé à partir de) `vf_all_ventes`.  
- `adresses_geocodees` (source lat/lon) → mise à jour de `valeursfoncieres.latitude`/`longitude`.

4. Procédures, triggers et scripts
----------------------------------

4.1 Trigger de déduplication sur `valeursfoncieres`

- **Trigger** : `tr_vf_set_dedup_key`  
  - Appelé `BEFORE INSERT OR UPDATE ON ventes_notaire.valeursfoncieres`.  
- **Fonction** : `vf_set_dedup_key()`  
  - Concatène plusieurs champs (code département, code postal, nature_mutation, date, type_local, valeur, adresse…) ;  
  - Calcule un hash SHA-256 du texte concaténé et le stocke dans `dedup_key_hash_calc`.  
- **But** : disposer d’une clé stable pour repérer des doublons potentiels de mutation.

4.2 Procédures d’agrégation communes

- **`sp_refresh_vf_communes_all`** (dans `03_sp_refresh_vf_communes_agg.sql`) :  
  - vide (`TRUNCATE`) ou nettoie `vf_communes` ;  
  - agrège les données à partir de `vf_all_ventes` par (année, code_dept, commune, code_postal, type_local) ;  
  - calcule les indicateurs globaux (prix moyens, médianes, quartiles, surfaces, prix/m²) et par catégories S/T.

4.3 Scripts SQL

- `02_refresh_vf_all_ventes.sql`  
  - Recrée la table `vf_all_ventes` et l’alimente depuis `valeursfoncieres`.  

- `03_refresh_vf_communes.sql`  
  - Exécute `TRUNCATE vf_communes;` + `CALL sp_refresh_vf_communes_all();`.  

- Scripts de migration MySQL → PostgreSQL (plan `PLAN_MIGRATION_POSTGRESQL.md`)  
  - Fichier `pgloader/mysql_to_ventes_notaire.load` pour pgLoader :  
    - copie la base `foncier` de MySQL vers PostgreSQL ;  
    - renomme le schéma `foncier` en `ventes_notaire` ;  
    - recrée ensuite les vues et triggers nécessaires.

5. Pipeline d’alimentation – description textuelle
--------------------------------------------------

5.1 Étape 0 – Import DVF dans `valeursfoncieres`

- Source : fichiers DVF (CSV ou autres) → base MySQL `foncier` → migration vers PostgreSQL `ventes_notaire`.  
- Table cible de détail : `ventes_notaire.valeursfoncieres` (structure enrichie).

5.2 Étape 1 – Géocodage des adresses (BAN)

1. **Initialisation des adresses à géocoder**  
   - Script : `init_adresses_geocodees.sql` ou `sp_init_adresses_geocodees_by_year`.  
   - Remplit `adresses_geocodees` à partir de `valeursfoncieres` (sélection des adresses pertinentes).

2. **Géocodage BAN**  
   - Script Python : `webapp-foncier/scripts/geocode_ban.py` ou `geocode_ban_postgres.py`.  
   - Pour chaque adresse non géocodée :
     - envoie une requête à l’API BAN ;  
     - récupère lat/lon ;  
     - met à jour `adresses_geocodees`.

3. **Recopie lat/lon vers `valeursfoncieres`**  
   - Soit avec `geocode_ban.py --update-only` (mode batch par année) ;  
   - soit avec `01_update_valeursfoncieres_latlon.sql`.  
   - À chaque exécution, les lat/lon trouvés dans `adresses_geocodees` sont recopiés dans `valeursfoncieres.latitude/longitude`.

5.3 Étape 2 – Agrégation transactionnelle (`vf_all_ventes`)

- Script : `02_refresh_vf_all_ventes.sql`.  
- Logique :
  - filtre `valeursfoncieres` (ex. `nature_mutation = 'Vente'`) ;  
  - groupe par `(code_departement, code_postal, commune, adresse_norm, date_mutation, type_local)` ;  
  - calcule des moyennes et agrégats (prix, surfaces, prix/m², nb lignes, pièces, lat/lon moyens) ;  
  - insère les résultats dans `vf_all_ventes`.

5.4 Étape 3 – Agrégation commune/année (`vf_communes`)

- Script : `03_refresh_vf_communes.sql` + procédures `sp_refresh_vf_communes_*`.  
- Logique :
  - lit `vf_all_ventes` ;  
  - regroupe par `(annee, code_dept, code_postal, commune, type_local)` ;  
  - calcule :
    - `nb_ventes` ;  
    - prix moyen, Q1, médian, Q3 ;  
    - surfaces moyennes/médianes ;  
    - prix/m² moyens, Q1, médian, Q3 ;  
    - indicateurs par tranches de surface (S1..S5) et de pièces (T1..T5).  
  - stocke les résultats dans `vf_communes`.

5.5 Étape 4 – Exposition via l’API FastAPI

- `GET /api/period` :  
  - requête SQL : `SELECT MIN(annee), MAX(annee) FROM vf_communes`.  
  - renvoie les bornes pour initialiser le formulaire IHM.

- `GET /api/geo` :  
  - récupère la liste des `code_dept` réellement présents dans `vf_communes` ;  
  - tente d’utiliser `ref_regions`/`ref_departements` pour reconstituer l’arborescence régions → départements ;  
  - fallback sur `REGIONS_METRO` si les tables de ref manquent.

- `GET /api/communes[?code_dept=XX]` :  
  - si `code_dept` fourni :  
    - `SELECT DISTINCT code_dept, code_postal, commune FROM vf_communes WHERE code_dept = :code_dept`.  
  - sinon :  
    - essaie `SELECT code_dept, code_postal, commune FROM ref_communes`;  
    - fallback sur `vf_communes`.  
  - utilisé pour alimenter les listes de communes dans l’IHM.

- `GET /api/stats` :  
  - paramètres : `niveau`, `region_id`, `code_dept`, `code_postal`, `commune`, `type_local`, `annee_min`, `annee_max`, `surface_cat`, `pieces_cat`.  
  - filtre :
    - géo : selon niveau (region/department/commune) → détermine `code_dept` list ;  
    - type : `type_local` ou liste par défaut (`Appartement`, `Maison`) ;  
    - période : `annee BETWEEN annee_min AND annee_max`.  
  - produit :
    - `global` = agrégat global (`_agg_rows`) sur toutes les lignes vf_communes correspondant aux critères ;  
    - `series` = agrégat par année (groupement et appel `_agg_rows` par année), pour tracer les courbes.

- `GET /api/ventes` :  
  - recherche de ventes détaillées dans `valeursfoncieres` autour d’un point (lat/lon) ;  
  - préfiltre bounding box puis filtre par distance géographique (formule de la sphère) et autres critères éventuels (type, surfaces, dates).

5.6 Étape 5 – Consommation par le frontend

1. Au chargement :  
   - `loadGeo()` → `/api/geo` pour remplir régions / départements ;  
   - `loadPeriod()` → `/api/period` pour initialiser la période par défaut ;  
   - `loadCommunesInBackground()` pour précharger éventuellement toutes les communes.

2. À chaque modification de critère (formulaire) :  
   - l’IHM peut rappeler `submitStats()` si des résultats sont déjà visibles (`refreshStatsIfResultsVisible()`), afin de mettre à jour en continu les statistiques.

3. À l’affichage des résultats :  
   - les réponses `global` et `series` de `/api/stats` sont utilisées pour :
     - remplir les listes de statistiques (moyenne, médiane, Q1, Q3) ;  
     - construire les datasets Chart.js (prix, surface, prix/m²) ;  
     - gérer les comparaisons (plusieurs lieux), la superposition, et la synchronisation des axes Y (logique de min/max global).

6. Conclusion
------------

La conception repose sur :

- une **séparation claire** entre données détaillées (`valeursfoncieres`), agrégats transactionnels (`vf_all_ventes`) et agrégats par commune/année/type (`vf_communes`) ;
- une **API simple** (quelques endpoints paramétrés) exposant des agrégats prêts à être consommés par l’IHM ;
- un **frontend riche** gérant la complexité d’affichage (comparaison, superposition, axes Y partagés), sans logique métier forte côté client.

Cette architecture facilite :

- l’extension fonctionnelle (ajout de nouveaux agrégats ou de nouvelles vues) ;  
- le recalcul périodique des agrégats (scripts SQL/ETL) ;  
- l’évolution de la présentation IHM sans remettre en cause le modèle de données sous-jacent.

