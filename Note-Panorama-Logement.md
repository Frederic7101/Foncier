# Panorama Logement — Demande et réalisation

Note de conception et de mise en œuvre de la section **Panorama Logement** de la fiche commune (webapp-foncier). Document rédigé pour conserver la trace de la demande utilisateur et de ce qui a été compris et réalisé.

---

## 1. Demande utilisateur (résumé)

### 1.1 Section Parc logements
- **Source des données** : table `agreg_communes` (implémentation : `agreg_communes_dvf`).
- **Année** affichée dans le titre « Parc logements » : année la plus récente pour laquelle `insee_com` = code INSEE de la commune (code INSEE issu de `ref_communes`).
- **Affichage** : insérer une ligne « Code INSEE : &lt;code INSEE&gt; » dans la **3ᵉ colonne** du panorama global (sous la ligne nom de commune et codes postaux).
- **Données à afficher** (pour cette commune et cette année) :  
  - nombre de maisons (`nb_maisons`),  
  - nombre d’appartements (`nb_apparts`),  
  - ratio propriétaires : `(prop_maison + prop_appart) / (nb_maisons + nb_apparts)` — interprété en réalisation comme moyenne pondérée : `(prop_maison×nb_maisons + prop_appart×nb_apparts) / (nb_maisons + nb_apparts)`.

### 1.2 Section Ventes
- **Source** : table `vf_communes`.
- **Année** : année la plus récente pour le couple (commune = nom standard de la commune dans `ref_communes`, même niveau géo).
- **Données** : par type de bien (Maisons+Appartements, Maisons, Appartements) : nb de ventes, prix médian, prix médian/m², surface médiane, prix moyen, prix moyen/m², surface moyenne, en **pondérant** les indicateurs par le nb de ventes de chaque type.

### 1.3 Section Locations
- **Source** : table `loyers_communes`.
- **Année** : année la plus récente pour `insee_c` = code INSEE de la commune.
- **Données** : par type (Maisons+Appartements, Maisons, Appartements) : nb de loyers (`nbobs_com`), loyer médian/m² (`loypredm2`), loyer Q1 (`lwr_ipm2`), loyer Q3 (`upr_ipm2`), pondérés par le nb d’observations.
- **Présentation** : remplacer les libellés des colonnes par **Loyer médian/m²**, **Loyer Q1/m²**, **Loyer Q3/m²** ; ajouter **2 lignes** : « Apparts 1/2 pièces » (`segment_surface = '1-2_pieces'`) et « Apparts. 3p+ » (`segment_surface = '3_plus_pieces'`).

### 1.4 Tableaux de rentabilité
- **Colonnes** : ajouter une colonne **Type de bien** (Maisons/Apparts, Maisons, Apparts) ; les 4 colonnes de largeur identiques (puis étendu à 6 avec Taxe foncière et Estimation Charges).
- **Formules** :
  - **Rentabilité brute** = `(loyer_médian/m² × 12) / (prix_médian/m²) × 100`, pondérée par le nb de logements (agreg_communes).
  - **Taxe foncière moyenne** = surface_moyenne × 3 × loypredm2 × taux_global_tfb (en %), soit `surface_moy × 3 × loyer_ref × (taux_tfb / 100)` (valeur locative cadastrale ; fiscalite_locale, jointure `code_insee`).
  - **Rentabilité HC** = `(loyer_médian/m²*surface_mediane × 12 − Taxe foncière médiane) / (prix_médian × 1,10) × 100`, pondérée par le nb de logements.
  - **Estimation Charges** : pour **Appartements** = 10 % × loyer_médian_m2 × surface × 12 ; pour **Maisons** = taux TEOM (dernière année) × surface × 4 × loypredm2 (surface = surface_mediane pour le tableau médianes, surface_moyenne pour le tableau moyennes) ; pour **Maisons/Appart.** = moyenne pondérée par le nb de logements (maisons vs appartements).
  - **Rentabilité nette** (incluant 25 % vacance/impayés/travaux) = `(loyer_médian/m² *surface_mediane× 12 × 75 % − 25 % × Charges_médiane − Taxe_foncière_médiane) / (prix_médian × 1,10) × 100`, pondérée par le nb de logements.
- **Deux tableaux** : un basé sur les **médianes**, un sur les **moyennes** (mêmes colonnes, indicateurs en moyen au lieu de médian).

### 1.5 Fiscalité locale et colonnes rentabilité (demande complémentaire)
- Brancher la **fiscalité locale** à partir de la table `fiscalite_locale` (affichage par année : TFNB, TFB, TEOM).
- Ajouter dans les **deux** tableaux de rentabilité les colonnes : **Taxe foncière médiane** (resp. **moyenne** pour le 2ᵉ tableau) et **Estimation Charges**.

---

## 2. Ce qui a été compris et réalisé

### 2.1 Backend (`webapp-foncier/backend/main.py`)

- **Endpoint `GET /api/fiche-logement`** (paramètres : `code_dept`, `code_postal`, `commune`) :
  - Résolution du **code INSEE** via `ref_communes` (comparaison sur le nom en forme canonique).
  - **Parc** : lecture dans **`agreg_communes_dvf`** (table existante dans le schéma) ; année = MAX(annee) pour `insee_com` ; champs : `nb_maisons`, `nb_apparts`, `ratio_proprietaires` (moyenne pondérée des `prop_maison` / `prop_appart` par effectifs), `surface_moy`.
  - **Ventes** : lecture dans **`vf_communes`** ; année = MAX(annee) pour la commune (code_dept, code_postal, commune canonique) ; agrégation par type (Maisons/Appart., Maisons, Appartements) avec moyennes pondérées par `nb_ventes` (prix médian, prix_m2_mediane, surface_mediane, prix_moyen, prix_m2_moyenne, surface_moyenne).
  - **Locations** : lecture dans **`loyers_communes`** ; année = MAX(annee) pour `insee_c` ; lignes par type (Maisons/Appart., Maisons, Appartements) et par segment (Apparts 1/2 pièces, Apparts. 3p+) ; indicateurs : `nbobs_com`, `loypredm2`, `lwr_ipm2`, `upr_ipm2` (pondérés par `nbobs_com`).
  - **Fiscalité** : requête sur **`fiscalite_locale`** par `code_insee` ; retour **`fiscalite`** : `{ annee, taux_tfnb, taux_tfb, taux_teom, taxe_fonciere_simulee }` (tri année décroissante). **taxe_fonciere_simulee** = surface_moy × 3 × loypredm2 × (taux_tfb/100) par année (valeur locative cadastrale). Taux TEOM de la dernière année utilisé pour l’estimation des charges Maisons.
  - **Rentabilités** : deux jeux de lignes (médianes et moyennes), **sans colonne Taxe foncière** : renta_brute, charges_mediane, renta_hc, renta_nette. **Charges** : Maisons = (taux_teom/100) × surface × 3 × loyer_ref (surface_mediane ou surface_moyenne selon tableau) ; Appartements = 10 % × loyer_m2 × surface × 12 ; Maisons/Appart. = pondération par nb_maisons et nb_apparts.

- **Endpoint `/api/stats`** (niveau commune) : la réponse inclut désormais **`code_insee`** pour affichage dans la 3ᵉ colonne du panorama global.

### 2.2 Frontend (`webapp-foncier/frontend/fiche_commune.html`)

- **Panorama global, 3ᵉ colonne** : ligne **« Code INSEE : &lt;code_insee&gt; »** sous le nom de la commune et les codes postaux, alimentée par `data.code_insee` de `/api/stats`.
- **Parc logements** : titre avec année issue de `fl.parc.annee` ; remplissage Nb maisons, Nb d’appartements, Ratio propriétaires. La colonne « Ratio logements vacants » reste non alimentée (aucune source indiquée).
- **Ventes** : année dans le titre ; tableau 3 lignes (Maisons/Appart., Maisons, Appartements) avec nb ventes, prix médian, prix médian/m², surface médiane, prix moyen, prix moyen/m², surface moyenne.
- **Locations** : année dans le titre ; tableau avec en-têtes **Loyer médian/m²**, **Loyer Q1/m²**, **Loyer Q3/m²** ; 5 lignes : Maisons/Appart., Maisons, Appartements, Apparts 1/2 pièces, Apparts. 3p+.
- **Fiscalité locale** : tableau (Année, Taxe Foncière Non Bâti, Taxe Foncière, TEOM) rempli dynamiquement depuis `fl.fiscalite` ; si aucune donnée, affichage « — ».
- **Rentabilités** :
  - Premier tableau (médianes) : 6 colonnes — Type de bien | Rentabilité brute | **Taxe foncière médiane** | **Estimation Charges** | Rentabilité HC | Rentabilité nette.
  - Second tableau (moyennes) : 6 colonnes — Type de bien | Rentabilité brute | **Taxe foncière moyenne** | **Estimation Charges** | Rentabilité HC | Rentabilité nette.
  - Les cellules Taxe foncière et Estimation Charges sont formatées en euros (valeur annuelle).

### 2.3 Choix et conventions

- **Table parc** : le schéma utilise **`agreg_communes_dvf`** ; si une table ou vue **`agreg_communes`** existe avec la même structure, le backend peut être adapté pour la cibler.
- **Ratio propriétaires** : implémenté comme moyenne pondérée des parts (prop_maison, prop_appart) par les effectifs (nb_maisons, nb_apparts), donnant un pourcentage cohérent avec des champs « part » 0–100.
- **Taxe foncière** : simulation par année dans le bloc Fiscalité (colonne « Taxe Foncière (simulée) » en €) ; colonne Taxe foncière retirée des deux tableaux de rentabilités.

---

*Référence : backend `main.py` (endpoint `/api/fiche-logement`, `/api/stats`), frontend `fiche_commune.html` (Panorama logement, fiscalité, rentabilités).*
