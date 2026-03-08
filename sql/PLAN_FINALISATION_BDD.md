# Finalisation BDD avant migration PostgreSQL

À faire dans l’ordre avant de lancer la migration (pgLoader, schéma ventes_notaire).

---

## En parallèle du géocodage BAN

Le géocodage BAN (étape 1.1) peut être **très long**. Tu peux avancer sur le reste sans attendre qu’il soit terminé.

| À faire | Quand | Pourquoi |
|--------|--------|----------|
| **Étapes 2 et 3** (vf_all_ventes, vf_communes) | Dès que tu veux, en parallèle du géocodage | Elles ne dépendent pas des lat/lon : elles s’appuient sur `valeursfoncieres` (et `vf_all_ventes`) qui existent déjà. Les champs latitude/longitude dans vf_all_ventes seront NULL ou partiels ; tu pourras **rejouer 2 puis 3** après la fin du géocodage pour les remplir. |
| **Étape 1.2** (recopier lat/lon → valeursfoncieres) | Périodiquement pendant le géocodage (ex. une fois par jour ou par semaine) | Tu peux lancer **`geocode_ban.py --update-only`** ou **`01_update_valeursfoncieres_latlon.sql`** autant de fois que tu veux : à chaque fois, les adresses déjà géocodées dans `adresses_geocodees` sont recopiées dans `valeursfoncieres`. Inutile d’attendre la fin du géocodage. |

**En résumé** : lance le géocodage (1.1). Pendant ce temps, tu peux exécuter **02** et **03** une première fois, et lancer **1.2** (ou `--update-only`) de temps en temps. Optionnel : **04** supprime la vue vw_vf_communes (non utilisée, on consulte vf_communes). Quand le géocodage est terminé, refais **1.2** une dernière fois, puis **02** et **03** une dernière fois pour figer les lat/lon dans vf_all_ventes et vf_communes.

---

## 1. Finir adresses_geocodees puis valeursfoncieres

### 1.1 Adresses à géocoder

- Lancer le pré-remplissage si besoin : **`init_adresses_geocodees.sql`** ou **`init_adresses_geocodees_by_year.sql`** (ou procédure **`sp_init_adresses_geocodees_by_year`**).
- Lancer le géocodage BAN : **`python webapp/scripts/geocode_ban.py`** (jusqu’à ce qu’il ne reste plus d’adresses à traiter, ou selon ton besoin).

### 1.2 Recopier lat/lon vers valeursfoncieres

Une fois `adresses_geocodees` à jour :

- Soit : **`python webapp/scripts/geocode_ban.py --update-only`** (met à jour valeursfoncieres par année 2020–2025).
- Soit : exécuter **`sql/01_update_valeursfoncieres_latlon.sql`** (équivalent SQL, une requête par année).

---

## 2. Ajuster vf_all_ventes

Cette table agrège les **lignes de détail** de `valeursfoncieres` (nature_mutation = 'Vente') par **transaction** identifiée par : lieu (département, commune, code_postal, adresse normalisée), type_local, date_mutation.

- **Nouveaux champs** : **nb_lignes** (remplace nb_mutations), **valeur_fonciere** (moyenne, remplace prix_moyen), **surface_reelle_bati** (moyenne, remplace surface_moyenne), **prix_m2** (remplace prix_moyen_m2), **nombre_pieces_principales**, **latitude**, **longitude**, **adresse_norm**.
- **Script** : **`sql/02_refresh_vf_all_ventes.sql`** (DROP/CREATE table + INSERT depuis valeursfoncieres).

---

## 3. Mettre à jour vf_communes depuis vf_all_ventes

Agrégation par **(année, département, commune, code_postal, type_local)** pour renseigner nb_ventes, prix_moyen, surface_moyenne, prix_moyen_m2.

- **Script** : **`sql/03_refresh_vf_communes.sql`** (TRUNCATE puis CALL sp_refresh_vf_communes_all). Les procédures sont dans **`sql/03_sp_refresh_vf_communes_agg.sql`**. Pour un refresh complet, exécuter le TRUNCATE + CALL du script.

---

## 4. (Optionnel) Supprimer la vue vw_vf_communes

La vue **vw_vf_communes** n’est pas utilisée : on consulte **vf_communes** (table pré-calculée, plus riche et plus rapide). Pour éviter qu’elle soit utilisée par erreur, tu peux la supprimer.

- **Script** : **`sql/04_recreate_vw_vf_communes.sql`** (DROP VIEW IF EXISTS vw_vf_communes).

---

Une fois les étapes 1 à 3 exécutées (et optionnellement 4), la BDD est prête pour la migration PostgreSQL.
