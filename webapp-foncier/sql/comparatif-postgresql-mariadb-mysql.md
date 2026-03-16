# Comparatif PostgreSQL, MariaDB et MySQL

Comparaison des trois moteurs de base de données relationnelles pour aider au choix selon le projet.

---

## Classement par critère (vitesse, stockage, stabilité, flexibilité)

| Critère | Le plus performant | Commentaire |
|--------|--------------------|-------------|
| **Vitesse** (lecture/écriture brute, cas simples) | **MySQL / MariaDB** | Très bons sur requêtes simples, peu de jointures, réplication lecture. PostgreSQL peut être plus lent par défaut sur charges très simples. |
| **Vitesse** (requêtes complexes, agrégations, fenêtres) | **PostgreSQL** | Optimiseur avancé, parallélisation des requêtes, index GIN/GiST ; souvent meilleur sur gros volumes et requêtes analytiques. |
| **Stockage** (compression, occupation disque) | **PostgreSQL** | TOAST, possibilité de compression ; réplication logique sans doublon. MariaDB ColumnStore pour l’analytique. |
| **Stabilité** | **PostgreSQL** puis **MariaDB** | PG : concurrence MVCC éprouvée, peu de blocages. MySQL/MariaDB : très stables aussi, historique de production massif. |
| **Flexibilité** | **PostgreSQL** | Types riches (JSON/JSONB, array, géo), schémas multiples, extensions (PostGIS, etc.), SQL avancé (CTE, fenêtres). MySQL/MariaDB plus limités. |

**En résumé** : pour **vitesse brute sur cas simples** → MySQL/MariaDB. Pour **vitesse sur requêtes complexes**, **stockage**, **stabilité** et **flexibilité** → **PostgreSQL**.

---

## Vue d’ensemble

| Critère | PostgreSQL | MariaDB | MySQL |
|--------|------------|---------|--------|
| **Type** | Projet open source indépendant | Fork de MySQL, communautaire | Oracle (propriétaire + version open source) |
| **Licence** | PostgreSQL (BSD-like) | GPL v2 | GPL (Community) / commerciale (Oracle) |
| **Maturité** | Très ancien, très stable | Issu de MySQL, évolue en parallèle | Référence historique, très répandu |

---

## Avantages et inconvénients

### PostgreSQL

**Avantages**
- **Fonctionnalités avancées** : types riches (tableaux, JSON/JSONB, UUID, géométrie PostGIS), fenêtres (window functions), CTE récursives, full-text search intégré.
- **Conformité SQL** : très bon respect du standard SQL, requêtes complexes bien supportées.
- **Renommage simple** : `ALTER DATABASE ... RENAME TO` et `ALTER SCHEMA ... RENAME TO` (pas de dump/import pour renommer).
- **Schémas** : plusieurs schémas par base (organisation claire : `ventes_notaire`, `encheres`, etc.) sans multiplier les bases.
- **Concurrence** : modèle MVCC (Multi-Version Concurrency Control), peu de blocages en lecture/écriture.
- **Extensions** : PostGIS (géoloc), pg_cron (tâches), nombreuses extensions officielles ou communautaires.
- **Licence permissive** : pas de contrainte commerciale, utilisation libre.

**Inconvénients**
- **Ressources** : souvent un peu plus gourmand en RAM et réglages que MySQL/MariaDB pour des petits déploiements.
- **Écosystème hérité** : beaucoup d’hébergeurs et d’outils historiques sont encore “MySQL first” (même si PostgreSQL est très répandu).
- **Courbe d’apprentissage** : syntaxe et concepts (schémas, extensions) peuvent demander un temps d’adaptation si on vient de MySQL.

**Idéal pour** : applications exigeantes en SQL, données complexes (JSON, géo), besoin de schémas multiples, nouveaux projets où la flexibilité prime.

---

### MariaDB

**Avantages**
- **Compatibilité MySQL** : fork de MySQL, protocole et syntaxe très proches ; migration MySQL → MariaDB souvent transparente.
- **Innovations** : moteurs de stockage (ColumnStore, Spider), meilleures perfs sur certains workloads, correctifs et évolutions communautaires.
- **Communauté** : projet ouvert, réactif, sans dépendance à Oracle.
- **Gratuit** : 100 % open source (GPL).

**Inconvénients**
- **Pas de renommage de base** : comme MySQL, pas d’équivalent à `ALTER DATABASE ... RENAME TO` ; dump/import ou RENAME TABLE.
- **Divergence avec MySQL** : au fil du temps, différences de comportement et de fonctionnalités ; certains outils ou drivers ciblent MySQL en priorité.
- **Double référence** : choix entre “suivre MySQL” ou “suivre MariaDB” pour la doc et les bonnes pratiques.

**Idéal pour** : remplacer MySQL sans tout réécrire, projets déjà en MySQL qui veulent rester dans le même écosystème avec une alternative communautaire.

---

### MySQL

**Avantages**
- **Très répandu** : hébergement, tutoriels, drivers, ORM — support partout.
- **Simple à démarrer** : installation et premières requêtes rapides.
- **Performant** : sur des cas simples (lecture/écriture basiques, petits schémas), souvent très bon.
- **Écosystème** : outils d’admin, connecteurs, documentation abondante.

**Inconvénients**
- **Contrôle Oracle** : la version “reference” est sous licence Oracle ; la version open source (MySQL Community) reste la plus utilisée.
- **Pas de renommage de base** : comme MariaDB, migration foncier → ventes_notaire par dump/import ou RENAME TABLE.
- **SQL limité** : pas de schémas multiples dans le sens PostgreSQL, moins de types avancés, certaines fonctionnalités (fenêtres, JSON) arrivées plus tard.
- **Licence** : attention aux usages commerciaux et aux produits dérivés (selon version et distribution).

**Idéal pour** : projets simples, hébergement mutualisé, équipes déjà à l’aise avec MySQL, besoin de compatibilité maximale avec l’existant “MySQL”.

---

## Synthèse par besoin

| Besoin | Choix le plus adapté |
|--------|-----------------------|
| Renommer une base ou un schéma facilement | **PostgreSQL** |
| Plusieurs “schémas” (ex. ventes_notaire, encheres) dans une même base | **PostgreSQL** |
| Données géographiques (PostGIS) | **PostgreSQL** |
| Rester proche de MySQL (migration, compétences) | **MariaDB** ou **MySQL** |
| Maximum de documentation / hébergeurs “MySQL” | **MySQL** |
| Nouveau projet, SQL avancé, types riches | **PostgreSQL** |
| Petit projet, hébergement type mutualisé | **MySQL** ou **MariaDB** |

---

## Pour ton contexte (foncier, enchères)

- **Schémas distincts** (ventes_notaire, encheres) : PostgreSQL gère ça nativement et proprement ; avec MySQL/MariaDB tu as une base = un “schéma”, donc plusieurs bases.
- **Renommage foncier → ventes_notaire** : une commande avec PostgreSQL ; avec MySQL/MariaDB, procédure dump/import ou RENAME TABLE comme aujourd’hui.
- **Géocodage / coordonnées** : PostgreSQL + PostGIS est très adapté ; MySQL/MariaDB ont des types spatiaux mais moins riches que PostGIS.

Si tu envisages une migration vers PostgreSQL, on peut détailler les étapes (export MySQL → import PostgreSQL, adaptation des types et des requêtes).
