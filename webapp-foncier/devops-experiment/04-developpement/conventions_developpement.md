## Conventions de développement

### 1. Branches Git

- **Branches de fonctionnalité** : `feature/<resume-court>`  
  - ex. `feature/ihm-stats-comparaison`  
- **Branches de correction** : `fix/<resume-court>`  
  - ex. `fix/api-stats-erreur-500`  
- **Branches techniques** : `chore/<resume-court>`  

### 2. Messages de commit

Format recommandé :

`<type>: <resume court>`  

Où `<type>` ∈ {`feat`, `fix`, `chore`, `docs`, `refactor`, `test`}  

Exemples :

- `feat: ajout comparaison multi-communes`  
- `fix: corrige erreur 500 sur /api/stats sans periode`  

### 3. Lien avec la webapp DVF

- **Backend** : `webapp-foncier/backend/main.py` (API FastAPI, endpoints principaux).  
- **Frontend** : `webapp-foncier/frontend/stats.html`, `stats.js`, `stats.css`.  
- **Scripts** : `webapp-foncier/scripts/` (géocodage BAN, etc.).  

### 4. Tests

- Les tests Python sont regroupés dans `webapp/tests/` (ou un sous-dossier `backend/tests/`).  
- Utiliser `pytest` + `httpx` (ou le `TestClient` FastAPI) pour tester les endpoints.  

