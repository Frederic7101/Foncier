## Intégration JIRA et Confluence

Ce dossier contient des **modèles** et **scripts d’exemple** pour intégrer le projet avec JIRA et Confluence.

### 1. Pré-requis

- Accès à une instance JIRA / Confluence (Cloud ou Server).  
- Création d’un **token API** ou configuration OAuth selon la politique de sécurité.  
- Variables d’environnement à définir sur le poste ou dans un fichier `.env` non versionné :
  - `JIRA_URL` (ex. `https://votre-instance.atlassian.net`)  
  - `JIRA_EMAIL` ou `JIRA_USER`  
  - `JIRA_TOKEN`  
  - `CONFLUENCE_URL`  
  - `CONFLUENCE_EMAIL` ou `CONFLUENCE_USER`  
  - `CONFLUENCE_TOKEN`  

Un fichier `config.example.env` fournit la liste de ces variables **sans valeurs**.  

### 2. Contenu

- `modele_projet_jira.md` : suggestion de types de tickets, champs et conventions.  
- `modele_espace_confluence.md` : structure type d’un espace documentaire.  
- `create_jira_issue_example.py` : exemple de création d’issue JIRA à partir d’un titre et d’une description.  
- `update_confluence_page_example.py` : exemple de création/mise à jour d’une page Confluence à partir d’un contenu Markdown.  

Les scripts utilisent la bibliothèque `requests` (à installer dans un environnement Python séparé si besoin).  

