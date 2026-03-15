# Note — Préparation du squelette de la page comparaison_scores.html

**Objet :** Structure et conception de la page « Comparaison des communes » (scores d’investissement).  
**Fichier :** `frontend/comparaison_scores.html`

---

## 1. Objectif de la page

Permettre de **comparer des communes** selon des scores (rentabilité brute/nette simulée, taux TF, TEOM, etc.) en choisissant un périmètre géographique (région, département) ou une liste de communes ciblées, puis en affichant un tableau classé (rang, région, code dépt, code postal, commune, score principal + colonnes optionnelles pour les scores secondaires).

---

## 2. Structure générale du squelette

### 2.1 Conteneur principal
- **`#app-comparaison`** : bloc principal (max-width 1200px, fond #f5f5f7), aligné avec les autres pages (fiche_commune, stats_ventes).

### 2.2 En-tête
- **Titre** : « Comparaison des communes » (h1, style aligné fiche_commune).
- **Sous-titre** : affiché en bandeau au survol du titre (« Classement des communes selon les scores… »).
- **Navigation** : boutons Fiche commune, Stats des ventes, Ventes (même logique que les autres pages, via `nav_links.js`).

### 2.3 Zone de contrôles (`section.comparaison-controls`)

**Périmètre géographique**
- **Région** : `#comparaison-region` (select, rempli via `/api/geo`).
- **Département** : `#comparaison-dept` (select, dépend de la région).
- **Commune** : `#comparaison-commune` (select, liste des communes du département via `/api/communes?code_dept=…` ; option « Toutes les communes du département » pour tout sélectionner).
- **Recherche rapide** : `#comparaison-search` + `#comparaison-suggestions` (autocomplétion commune mutualisée, `communes_autocomplete.js`).

**Scores**
- **Score principal** : `#comparaison-score-principal` (renta_brute, renta_nette).
- **Nombre max de communes** : `#comparaison-n-max` (1–500, défaut 20).
- **Scores secondaires** (affichage dans le tableau) : cases à cocher (renta_brute, renta_nette, taux_tfb, taux_teom).
- **Boutons** : « Comparer » (`#comparaison-btn`), « Réinitialiser » (`#comparaison-reset-btn`).

**Communes sélectionnées**
- Bloc `#comparaison-selected-block` (masqué si vide) : liste `#comparaison-selected-ul` des communes ajoutées (via recherche rapide ou select), avec bouton « Vider la liste » (`#comparaison-vider-liste`).

### 2.4 Zone résultats
- **Message d’accueil** : `#comparaison-empty` (affiché par défaut).
- **Chargement** : `#comparaison-loading` (masqué par défaut, classe `.visible` pour afficher).
- **Erreur** : `#comparaison-error` (role="alert", hidden par défaut).
- **Tableau** : `#comparaison-table` (aria-hidden par défaut), thead `#comparaison-thead`, tbody `#comparaison-tbody`. Colonnes : Rang, Région, Code dépt, Code postal, Commune, Score (principal + colonnes secondaires selon les cases cochées).

---

## 3. Données et APIs

- **Géo** : `GET /api/geo` → régions, départements ; remplissage des selects région/département au chargement.
- **Communes** : `GET /api/communes?code_dept=…` pour la liste du département ; `GET /api/communes?q=…` pour l’autocomplétion (via `communes_autocomplete.js`).
- **Comparaison** : appel API (paramètres région_id, code_dept, score principal, n_max, scores secondaires, liste de communes sélectionnées) pour récupérer le classement et remplir le tableau.

État local script : `geo`, `communesList`, `selectedCommunes` (liste d’objets { code_dept, code_postal, commune }).

---

## 4. Comportements intégrés au squelette

- **Init depuis l’URL** : si `?code_dept=…&code_postal=…&commune=…`, pré-remplissage région/département, ajout de la commune dans « Communes sélectionnées », mise à jour des liens de navigation (`initFromUrlParams()` après `loadGeo()`).
- **Réinitialiser** : vide la liste sélectionnée, remet URL et sélecteurs à zéro, décoche les scores secondaires, remet score principal et n-max aux valeurs initiales, réaffiche le message d’accueil et met à jour la nav (`updateNavLinksFromCommune()`).
- **Navigation** : même apparence et même gestion que sur les 3 autres pages (boutons, nouvel onglet, `nav_links.js`).

---

## 5. Fichiers et styles

- **CSS** : `style.css` global ; le reste des styles est en bloc `<style>` dans la page (préfixes `#app-comparaison`, `.comparaison-*`) pour garder la page autonome.
- **Scripts** : `communes_autocomplete.js`, `nav_links.js` ; le reste du script (géo, filtres, appel comparaison, rendu tableau) est inline dans la page.

---

## 6. Conventions d’identifiants

Préfixe **`comparaison-`** pour les éléments propres à la page (ex. `comparaison-region`, `comparaison-dept`, `comparaison-commune`, `comparaison-search`, `comparaison-btn`, `comparaison-reset-btn`, `comparaison-empty`, `comparaison-loading`, `comparaison-error`, `comparaison-table`, etc.), afin d’éviter les conflits avec les autres pages.

---

*Cette note décrit le squelette et la préparation de la page comparaison_scores.html telle qu’utilisée dans la webapp foncier.*
