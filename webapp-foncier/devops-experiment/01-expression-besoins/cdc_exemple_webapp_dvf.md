## Exemple de mini cahier des charges – Webapp DVF

Ce document illustre l'utilisation du **modèle de cahier des charges** pour l'application de statistiques foncières (webapp DVF).

### 1. Contexte et objectifs

- **Contexte** : exploiter les données DVF agrégées dans PostgreSQL (schéma `ventes_notaire`) pour fournir des statistiques foncières accessibles à des profils non techniques.
- **Objectifs** :
  - permettre la **visualisation de l’évolution** des prix, surfaces et prix/m² par commune, département ou région ;
  - offrir une **comparaison** de plusieurs lieux sur une même période ;
  - fournir un point d’entrée aux **ventes détaillées** autour d’un point géographique.

### 2. Périmètre

- **Inclus** :
  - écran « Stats » (vue simple, comparaison, superposition) ;
  - API de consultation (`/api/period`, `/api/geo`, `/api/communes`, `/api/stats`, `/api/ventes`) ;
  - scripts de géocodage BAN pour l’alimentation de la base.
- **Exclus** :
  - authentification utilisateurs (phase ultérieure) ;
  - gestion des droits avancée.

### 3. Référence détaillée

Les spécifications fonctionnelles complètes de l’IHM Stats sont décrites dans :  
- `specs_ihm_stats.md`

La conception technique (architecture, modèle de données, pipeline) est décrite dans :  
- `conception_ihm_bdd_pipeline.md`

Ces deux documents servent de **référence** pour la webapp DVF et d’exemple concret pour ce cadre d’expérimentation.

