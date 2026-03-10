## Référence de maquettage

Ce document décrit comment produire et stocker les maquettes pour un projet.

### 1. Outils possibles

- **Outils en ligne** : Figma, Balsamiq, Whimsical, etc.
- **Maquettes statiques** : pages HTML/CSS simples dans un dossier `maquettes/`.
- **Documents bureautiques** : LibreOffice / PowerPoint avec captures et annotations.

### 2. Bonnes pratiques

- Travailler d'abord sur des **wireframes basse fidélité** (structure, zones principales).
- Limiter le nombre de résolutions cibles (desktop, tablette, mobile) selon le besoin.
- Lier chaque écran à des **user stories** ou exigences identifiées.

### 3. Emplacement des maquettes

- Dossier recommandé dans ce projet : `webapp-foncier/devops-experiment/01-expression-besoins/maquettes/`.
- Ou bien un espace documentaire (Confluence, partage réseau) référencé ici par URL.

### 4. Exemple – écran « Stats » DVF

Pour la webapp DVF, les spécifications fonctionnelles sont décrites dans `specs_ihm_stats.md`.  
Les maquettes peuvent illustrer :

- le formulaire de critères (sélection région/département/commune, période, type de local) ;
- la zone de messages / état (chargement, erreurs) ;
- la zone de résultats en **vue simple**, **comparaison** et **superposition**.

Les maquettes HTML/CSS simples pourront être ajoutées dans `maquettes/` si nécessaire.

