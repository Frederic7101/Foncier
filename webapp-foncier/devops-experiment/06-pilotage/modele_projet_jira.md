## Modèle de configuration projet JIRA

### 1. Types de tickets

- **Story** : user stories, exigences fonctionnelles.  
- **Task** : tâches techniques ou génériques.  
- **Bug** : anomalies détectées en test ou en production.  
- **Epic** (optionnel) : regroupement de stories pour un gros sujet (ex. “Stats foncières DVF”).  

### 2. Champs recommandés

- **Résumé** (obligatoire).  
- **Description** : peut inclure un lien vers :
  - le cahier des charges ;  
  - la user story source (dossier `01-expression-besoins/`) ;  
  - le plan de tests associé.  
- **Priorité** : Bloquant / Critique / Majeur / Mineur.  
- **Composant** : ex. Backend, Frontend, Data, DevOps.  
- **Version corrigée dans** (pour les bugs).  

### 3. Workflow simplifié

- `To Do` → `In Progress` → `In Review` → `Done`  

Un workflow plus complet peut inclure des états de validation métier ou de recette.  

