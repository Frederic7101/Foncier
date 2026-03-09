# Explication des différences d’affichage des graphiques (3 modes)

## 1. Mode unique — conteneur dépasse et nécessite un scroller

**Cause principale :** La règle globale `.stats-tabs` impose **`min-height: 60vh`** (ligne ~468). Elle s’applique aussi au mode lieu unique. Du coup, le bloc onglets (titre + liste + graphique) a une hauteur minimale de 60 % de la fenêtre, à quoi s’ajoutent le titre de la région et le « Nombre de ventes ». La somme dépasse souvent la hauteur de `#stats-results`, d’où l’apparition du scroll.

En **superposition**, `.stats-overlay-results ... .stats-tabs` redéfinit `min-height: 0`, donc pas de minimum imposé et pas de dépassement.

**En résumé :** En mode unique, le `min-height: 60vh` sur `.stats-tabs` force une hauteur trop grande et déclenche le scroller.

---

## 2. Mode comparaison en grille — graphiques non affichés

**Cause :** La **chaîne de hauteur en flex** n’est pas complète dans la grille.

- **`.stats-result-half`** (chaque colonne Lieu 1, Lieu 2, …) n’est pas en `display: flex`. C’est un bloc classique. Les enfants (titre, nb ventes, `.stats-tabs`) s’empilent en flux normal et ne se partagent pas la hauteur de la cellule grille.
- Les propriétés **`flex: 1 1 auto`** et **`min-height: 0`** sur `.stats-tabs` et `.stats-tab-panel .chart-wrap` ne servent à rien si le parent n’est pas un conteneur flex. La hauteur « disponible » pour le graphique n’est pas définie.
- En pratique, au moment où Chart.js crée les graphiques, le conteneur du canvas peut avoir une hauteur calculée à **0** (ou très faible), donc les graphiques ne s’affichent pas ou sont invisibles.

En **superposition**, tout le bloc est en flex (`#stats-overlay-results` → `.stats-tabs` → `.stats-tab-panels` → `.stats-tab-panel` → `.chart-wrap`) avec `min-height: 0` partout, donc la hauteur se propage jusqu’au canvas et les graphiques ont une taille correcte.

**En résumé :** En grille, le parent direct des onglets (`.stats-result-half`) n’est pas en flex et il manque une chaîne flex avec `min-height: 0` dans les onglets de la grille, donc le conteneur du graphique reste sans hauteur et les graphiques ne s’affichent pas.

---

## 3. Mode superposition — graphique bien affiché

Ici, toute la zone est pensée en flex :

- `#stats-overlay-results` : `display: flex`, `flex: 1 1 auto`, `min-height: 0`
- `.stats-tabs` : `flex: 1 1 auto`, **`min-height: 0`** (pas de 60vh)
- `.stats-tab-panels` : `flex: 1 1 auto`, `min-height: 0`
- `.stats-tab-panel.active` : `flex: 1 1 auto`, `min-height: 0`
- `.chart-wrap` : `flex: 1 1 auto`, `min-height: 0`
- `.chart-canvas-wrap` : `aspect-ratio: 1`, `max-height: 100%`, etc.

La hauteur de `#stats-results` se transmet jusqu’au canvas, le graphique a une taille définie et s’affiche correctement.

---

## Où sont les différences (en plus du CSS des `.chart-wrap`) ?

| Élément | Mode unique | Mode grille | Mode superposition |
|--------|-------------|-------------|---------------------|
| Conteneur principal | `#stats-content` | `#stats-compare-results` | `#stats-overlay-results` |
| `display` conteneur | flex (règle commune) | **grid** | flex |
| `.stats-tabs` | **min-height: 60vh** (global) | idem | **min-height: 0** (overlay) |
| Parent des onglets | `#stats-content` (flex) | **`.stats-result-half` (non flex)** | `#stats-overlay-results` (flex) |
| Tab-panels / tab-panel | flex + min-height: 0 | **pas de flex sur .stats-tabs-compare .stats-tab-panels** | flex + min-height: 0 |
| Chaîne de hauteur | cassée par 60vh | cassée par bloc non flex | cohérente |

Les différences viennent donc surtout :

1. Du **`min-height: 60vh`** sur `.stats-tabs` qui s’applique au mode unique (et à la grille) mais pas à la superposition.
2. De l’absence de **flex** sur `.stats-result-half` et sur la hiérarchie des onglets en mode grille (`.stats-tabs-compare`), alors que tout est en flex en superposition.

Les règles spécifiques des `.chart-wrap` / `.chart-canvas-wrap` (max-height, aspect-ratio, etc.) jouent aussi, mais la raison principale des comportements différents est cette **chaîne de hauteur** (flex + min-height) qui est cohérente uniquement en superposition.
