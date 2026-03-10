# Schéma de la base MySQL (Foncier)

Ce répertoire contient la **description de la base de données** (structure uniquement, sans données).

## Fichiers

- `foncier_schema.sql` : export du schéma (tables, vues, procédures) généré par `mysqldump --no-data`.

## Régénérer le schéma

Depuis la racine du projet (parent de `MySQL`) :

```bash
python MySQL/export_schema.py
```

Le script utilise les variables d'environnement définies dans `webapp-foncier/backend/.env` (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME).  
**Prérequis** : `mysqldump` doit être disponible dans le PATH (client MySQL installé).
