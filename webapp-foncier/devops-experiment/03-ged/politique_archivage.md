## Politique d’archivage documentaire

### 1. Objectifs

- Assurer la **traçabilité** des versions successives.  
- Éviter l’encombrement du dossier de travail courant.  

### 2. Règles d’archivage

- Archiver un document lorsqu’une **nouvelle version publiée** est disponible.  
- Conserver au minimum :
  - la dernière version publiée ;  
  - les versions associées à des jalons importants (MVP, mise en production, audits).  

### 3. Emplacements proposés

- Dossier `archive/` au sein du projet (ou dépôt distinct dédié aux archives).  
- Éventuellement un espace documentaire (Confluence, GED d’entreprise) référencé par lien.  

### 4. Script d’archivage (exemple)

Un script Python simple peut copier une version donnée vers un sous-dossier d’archive avec horodatage.  
Voir `archiver_document.py` dans ce même dossier pour un exemple minimal.

