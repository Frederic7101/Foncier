## Checklist sécurité applicative

### 1. Authentification & autorisation

- Mécanisme d’authentification défini (SSO, login/mot de passe, autre) ?  
- Gestion des rôles / permissions (RBAC) documentée ?  
- Pages / endpoints sensibles protégés ?  

### 2. Données sensibles

- Inventaire des données sensibles (perso, financières, secrets techniques) réalisé ?  
- Chiffrement **en transit** (HTTPS obligatoire) ?  
- Chiffrement **au repos** (disques, champs BDD le cas échéant) ?  

### 3. Exposition des API

- Filtrage des origines (CORS) paramétré selon le contexte ?  
- Routes d’admin / debug désactivées ou protégées en production ?  
- Limitation des volumes / rate limiting (si pertinent) ?  

### 4. Journalisation et audit

- Journalisation des événements de sécurité (connexion, échec, changement critique) ?  
- Conservation et protection des logs (accès restreint) ?  

### 5. Configuration & secrets

- Secrets (mots de passe, tokens) sortis du code source (variables d’environnement, coffre-fort) ?  
- Fichiers d’exemple (`config.example.*`) fournis sans données sensibles ?  

### 6. Tests de sécurité

- Revue de code orientée sécurité effectuée ?  
- Outils d’analyse statique / scans de vulnérabilités prévus ?  

