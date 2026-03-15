# Résumé session webapp-foncier – 14 au 15 mars 2026

## Objectif de la session : Panorama Logement (formules, libellés, formatage)

Cette session a porté sur les **formules de rentabilité**, l’**estimation des charges** (TEOM pour les maisons), le **coefficient valeur locative cadastrale** (4 → 3), le **formatage des pourcentages et loyers €/m²**, et plusieurs **libellés / notes** de la fiche commune (Panorama Logement et Fiscalité).

---

## 1. Formules backend (main.py)

### 1.1 Estimation Charges
- **Appartements** : inchangé, 10 % × loyer_m2 × surface × 12.
- **Maisons** : remplacement par **taux TEOM (dernière année) × surface × 3 × loypredm2** (surface_mediane pour le tableau médianes, surface_moyenne pour le tableau moyennes). Coefficient 3 = valeur locative cadastrale.
- **Maisons/Appart.** : moyenne pondérée par nb_maisons et nb_apparts des charges Maisons et Appartements.

### 1.2 Taxe foncière et valeur locative cadastrale
- Coefficient multiplicateur **4 remplacé par 3** partout (valeur locative cadastrale) :
  - Taxe foncière simulée : `surface_moy × 3 × loypredm2 × (taux_tfb/100)`.
  - TF utilisée dans renta_hc et renta_nette : idem.
  - Charges Maisons (TEOM) : `(taux_teom/100) × surface × 3 × loyer_ref`.

### 1.3 Rentabilités HC et nette
- **Rentabilité HC** = (loyer_m2 × surface_mediane × 12 − Taxe foncière) / **(prix_median × 1,10)** × 100 (dénominateur = prix total × 1 + frais de mutation 10 %).
- **Rentabilité nette** = (loyer_m2 × surface_mediane × 12 × 75 % − 25 % × Charges − TF) / (prix_median × 1,10) × 100.

### 1.4 Colonne Taxe foncière retirée des tableaux de rentabilités
- Les deux tableaux (médianes et moyennes) n’affichent plus la colonne « Taxe foncière » ; les lignes API ne renvoient plus `taxe_fonciere`.

### 1.5 Fiscalité
- **taxe_fonciere_simulee** ajoutée par année dans la liste `fiscalite` (pour la colonne « Taxe Foncière (simulée) »).
- Taux **taux_teom** récupéré (dernière année) pour le calcul des charges Maisons.

---

## 2. Frontend (fiche_commune.html)

### 2.1 Libellés et titres
- Titre du bloc loyers : **« Loyers observés par l'ANIL »** (remplace « Loyers »).
- Tableau Fiscalité : colonne **« Taxe Foncière Non Bâti »** remplacée par **« Taxe Foncière (simulée) »** (montant en €) ; colonne taux : **« Taux TF (Taxe Foncière) »** (remplace « Taxe Foncière (taux TFB) »).

### 2.2 Notes de bas de tableau
- **Taxe Foncière (simulée)** : « Taxe Foncière (simulée) = surface_moy × coeff_valeur_locative_cadas × loyer_ref_m2 × taux_TFB. »
- **Rentabilité HC (note 2)** : « Rentabilité hors charges (HC) = (Loyers annuels nets de charges récupérables / (Prix d'acquisition × (1 + taux_frais_mutation))) × 100, avec pour la simulation taux_frais_mutation = 10 %. »

### 2.3 Formatage
- **Pourcentages** : format **xx.x %** partout (1 décimale fixe) via `formatPct()` (rentabilités, ratio propriétaires, TFB, TEOM).
- **Loyers €/m²** : **1 chiffre après la virgule** via `formatEuroM2()` (minimumFractionDigits: 1, maximumFractionDigits: 1), unité affichée « €/m² ».

---

## 3. Documentation

- **Note-Panorama-Logement.md** : mise à jour des formules (charges Maisons TEOM, coefficient 3, taxe simulée, colonnes rentabilités sans TF, libellés).

---

## 4. Fichiers modifiés / créés (session 14–15/03)

- `webapp-foncier/backend/main.py` : formules rentabilités, charges TEOM, coefficient 3, taxe_fonciere_simulee, retrait colonne TF des rentas.
- `webapp-foncier/frontend/fiche_commune.html` : libellés, notes, formatPct, formatEuroM2, titres et colonnes.
- `Note-Panorama-Logement.md` : formules et réalisation.
- `Résumé session webapp-foncier 20260314-15.md` : présente note.

---

## 5. Reste à faire / vérifications

- **Parc Logement** : les données de la sous-section Parc ne sont « visiblement pas bonnes » (à vérifier en priorité : source, requête, champs).
- **Calculs** : vérification globale des formules (rentabilités, charges, TF simulée) sur cas réels.
- **Tests** : reprise manuelle ou scénarios de test sur une ou deux communes pour valider les ordres de grandeur.

---

*Référence : backend `main.py` (endpoint `/api/fiche-logement`), frontend `fiche_commune.html` (Panorama logement, fiscalité, rentabilités), `Note-Panorama-Logement.md`.*
