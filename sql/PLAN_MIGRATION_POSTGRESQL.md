# Migration MySQL → PostgreSQL (schéma foncier → ventes_notaire)

Objectif : migrer la base MySQL `foncier` vers PostgreSQL avec le schéma nommé **ventes_notaire**, de la façon la plus rapide et simple possible (outil **pgLoader** + renommage de schéma en une commande).

**Environnement cible : PostgreSQL 18** (port 5432 par défaut).

---

## Prérequis

1. **PostgreSQL 18** installé et démarré (port 5432).
2. **MySQL** avec la base `foncier` (déjà le cas).
3. **Docker Desktop** pour Windows (pour exécuter pgLoader sans installation manuelle).
   - Téléchargement : https://www.docker.com/products/docker-desktop/
   - Alternative sans Docker : binaire Windows pgLoader (voir section « Sans Docker » plus bas).

---

## Vue d’ensemble (5 étapes)

| Étape | Action | Durée estimée |
|-------|--------|----------------|
| 1 | Installer PostgreSQL, créer la base cible | 2 min |
| 2 | Adapter le fichier de config pgLoader (mots de passe) | 1 min |
| 3 | Lancer pgLoader (Docker) | Selon volume de données |
| 4 | Renommer le schéma `foncier` → `ventes_notaire` dans PostgreSQL | < 1 s |
| 5 | Recréer les vues et optionnellement la colonne générée | 2 min |

---

## Étape 1 : PostgreSQL – créer la base cible

PostgreSQL doit avoir une base qui recevra les données (pgLoader créera le schéma `foncier` dedans ; on le renommera ensuite).

**Cmd (avec psql dans le PATH) :**

```cmd
psql -U postgres -c "CREATE DATABASE foncier ENCODING 'UTF8' LC_COLLATE='fr_FR.UTF-8' LC_CTYPE='fr_FR.UTF-8' TEMPLATE template0;"
```

Si `fr_FR.UTF-8` n’existe pas sur ta machine, utilise `C` ou `en_US.UTF-8` :

```cmd
psql -U postgres -c "CREATE DATABASE foncier ENCODING 'UTF8';"
```

---

## Étape 2 : Fichier de config pgLoader

Le fichier **`pgloader/mysql_to_ventes_notaire.load`** contient les connexions MySQL et PostgreSQL.

À adapter dans le fichier :

- **MySQL** : `mysql://root:TON_MOT_DE_PASSE_MYSQL@host.docker.internal/foncier`
- **PostgreSQL** : `postgresql://postgres:TON_MOT_DE_PASSE_PG@host.docker.internal/foncier`

`host.docker.internal` permet au conteneur Docker d’atteindre MySQL et PostgreSQL sur ta machine Windows.

---

## Étape 3 : Lancer la migration avec pgLoader (Docker)

Ouvre un terminal (Cmd ou PowerShell) dans le dossier du projet (parent de `sql`).

**PowerShell :**

```powershell
cd c:\Users\frede\OneDrive\Documents\Cursor
docker run --rm -v "${PWD}/sql/pgloader:/mnt" dimitri/pgloader:latest pgloader /mnt/mysql_to_ventes_notaire.load
```

**Cmd :**

```cmd
cd c:\Users\frede\OneDrive\Documents\Cursor
docker run --rm -v "%CD%\sql\pgloader:/mnt" dimitri/pgloader:latest pgloader /mnt/mysql_to_ventes_notaire.load
```

pgLoader va :

- Se connecter à MySQL (base `foncier`) et à PostgreSQL (base `foncier`).
- Créer le schéma `foncier` dans PostgreSQL et y recréer les tables (sans partitions MySQL ; une table par ancienne table).
- Copier toutes les données.
- Créer les index et réinitialiser les séquences.

Les **vues** et **procédures stockées** MySQL ne sont pas migrées par pgLoader ; on les recrée à l’étape 5.

---

## Étape 4 : Renommer le schéma en ventes_notaire

Une fois pgLoader terminé sans erreur :

```cmd
psql -U postgres -d foncier -c "ALTER SCHEMA foncier RENAME TO ventes_notaire;"
```

C’est instantané (pas de recopie de données).

---

## Étape 5 : Recréer les vues (et optionnellement colonne générée)

Exécuter le script SQL qui recrée les vues dans le schéma `ventes_notaire` :

```cmd
psql -U postgres -d foncier -f sql/postgresql/02_recreate_views.sql
```

La table **valeursfoncieres** a en MySQL une colonne **générée** (`dedup_key_hash`). pgLoader l’aura importée comme colonne normale (remplie ou non selon la version). Si tu veux la même sémantique qu’en MySQL (colonne calculée), on peut ajouter une colonne générée ou un trigger après coup (voir section « Colonne dedup_key_hash » en bas de ce plan).

---

## Vérifications rapides

```cmd
psql -U postgres -d foncier -c "\dn"
psql -U postgres -d foncier -c "\dt ventes_notaire.*"
psql -U postgres -d foncier -c "SELECT COUNT(*) FROM ventes_notaire.valeursfoncieres;"
psql -U postgres -d foncier -c "SELECT COUNT(*) FROM ventes_notaire.adresses_geocodees;"
```

---

## Schéma cible sous PostgreSQL

- **Base** : `foncier`
- **Schéma des données migrées** : `ventes_notaire` (ex-MySQL `foncier`)
- Plus tard, tu pourras ajouter le schéma **encheres** dans la même base pour Licitor :

```sql
CREATE SCHEMA IF NOT EXISTS encheres;
-- puis créer la table licitor_annonces dans encheres
```

---

## Sans Docker (pgLoader sous Windows)

Si tu ne veux pas utiliser Docker :

1. Télécharger un binaire pgLoader pour Windows (par ex. https://github.com/zenghui-li/pgloader-for-windows/releases) ou compiler depuis les sources.
2. Dans le fichier `.load`, remplacer `host.docker.internal` par `localhost`.
3. Exécuter :  
   `pgloader sql\pgloader\mysql_to_ventes_notaire.load`

---

## Colonne dedup_key_hash (optionnel)

En MySQL, `dedup_key_hash` est une colonne générée (SHA2). En PostgreSQL, tu peux soit garder la colonne telle qu’importée par pgLoader, soit recréer une colonne générée. Exemple (à exécuter après migration, si besoin) :

```sql
-- Option : ajouter une colonne générée (PostgreSQL 12+)
ALTER TABLE ventes_notaire.valeursfoncieres
  ADD COLUMN dedup_key_hash_calc text GENERATED ALWAYS AS (
    encode(sha256(
      concat_ws('|',
        coalesce(code_departement,''),
        coalesce(code_postal,''),
        coalesce(nature_mutation,''),
        coalesce(date_mutation::text,''),
        coalesce(type_local,''),
        coalesce(valeur_fonciere::text,''),
        coalesce(trim(voie),''),
        coalesce(trim(type_de_voie),''),
        coalesce(trim(no_voie),''),
        coalesce(surface_reelle_bati::text,'')
      )::bytea
    ), 'hex')
  ) STORED;
```

Tu pourras ensuite supprimer l’ancienne colonne importée et renommer `dedup_key_hash_calc` en `dedup_key_hash` si tu veux garder le même nom.

---

## Référence PostgreSQL 18 (Windows)

- Données et config : `C:\Program Files\PostgreSQL\18\data\`
- Fichier d’authentification : `C:\Program Files\PostgreSQL\18\data\pg_hba.conf`
- Service Windows : « PostgreSQL 18 » dans `services.msc`

---

## En cas d’erreur pgLoader

- Vérifier que MySQL et PostgreSQL 18 sont démarrés et accessibles.
- Avec Docker : vérifier que `host.docker.internal` résout bien (Docker Desktop le fournit sous Windows).
- Consulter le message d’erreur pgLoader (connexion, mot de passe, encodage, type non supporté).
- Pour une table problématique (ex. colonne générée complexe), tu peux exclure cette table dans le fichier `.load` puis la recréer à la main dans PostgreSQL.
