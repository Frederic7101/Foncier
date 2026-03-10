## Template de conception IHM

### 1. Écrans

Lister les principaux écrans (ex. Accueil, Stats, Recherche, Administration) avec pour chacun :

- **Nom** :  
- **Objectif** :  
- **Acteurs concernés** :  

### 2. Découpage d’un écran type

Exemple pour un écran « Stats » :

- **Zone 1 – Formulaire de critères** : sélecteurs, filtres, boutons d’action.
- **Zone 2 – Messages / état** : messages par défaut, erreurs, chargement.
- **Zone 3 – Résultats** : tableaux, graphiques, indicateurs clés.

### 3. Composants et comportements

Pour chaque composant important :

- **ID / nom** :  
- **Rôle** :  
- **Événements** (clic, changement de valeur, saisie) :  
- **Comportement attendu** (ex. validation, activation/désactivation) :  

### 4. Navigation et parcours utilisateur

- **Parcours typiques** (scénarios) :  
- **Redirections / écrans d’erreur** :  
- **Raccourcis / ergonomie** (clavier, focus, accessibilité) :  

### 5. Règles de présentation

- Principes de mise en page (grille, flex, marges) ;  
- Couleurs / typographie (lien vers charte si disponible) ;  
- Gestion des petits écrans (responsive).  

### 6. Références webapp DVF

S’inspirer notamment de :

- `specs_ihm_stats.md` (description fonctionnelle de l’écran Stats).  
- `webapp-foncier/frontend/stats.html`, `stats.js`, `stats.css`.  

