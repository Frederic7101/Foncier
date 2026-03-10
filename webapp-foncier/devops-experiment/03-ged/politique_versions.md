## Politique de gestion de versions documentaire

### 1. Principes

- Chaque document “officiel” (spec, conception, plan, rapport) possède un **numéro de version**.  
- Les modifications sont tracées via **Git** et/ou via un historique dans le document.  

### 2. Cycle de vie d’un document

1. **Brouillon** (`v0.x`)  
   - Document en cours de rédaction.  
2. **Publié** (`v1.0`, `v1.1`, etc.)  
   - Version validée par les parties prenantes.  
3. **Obsolète / archivé**  
   - Remplacé par une version plus récente ou par un autre document.  

### 3. Règles de mise à jour

- Incrémenter la version **mineure** (`v1.1`) pour les corrections sans impact sur le périmètre.  
- Incrémenter la version **majeure** (`v2.0`) pour des changements importants (périmètre, architecture, règles de gestion).  
- Enregistrer dans le document un court **changelog** : date, auteur, nature des modifications.  

### 4. Localisation des versions “publiées”

- Option 1 : dossier `publie/` contenant uniquement les versions validées.  
- Option 2 : tags Git (ex. `doc-spec-ihm-stats-v1.0`) pour figer un état de la documentation dans le dépôt.  

La webapp DVF peut utiliser ce dossier `03-ged/` comme référence pour la liste des documents considérés comme **officiels**.  

