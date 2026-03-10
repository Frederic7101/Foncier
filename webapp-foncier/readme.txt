Pour tester cette première version, tu peux suivre ces étapes simples.

1. Préparer le backend FastAPI
Ouvrir un terminal dans c:\Users\frede\OneDrive\Documents\Cursor\webapp-foncier.
Installer les dépendances Python (une fois) :
pip install -r requirements.txt
Créer (si tu veux) un fichier .env dans webapp-foncier/backend avec tes paramètres MySQL :
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=ton_mot_de_passe
DB_NAME=foncier
Lancer l’API :
cd backend
uvicorn main:app --reload
L’API sera accessible sur http://localhost:8000.
Tu peux vérifier avec http://localhost:8000/health dans le navigateur (doit répondre {"status": "ok"}).
Important : pour que la recherche par rayon fonctionne, les colonnes latitude et longitude doivent exister dans valeursfoncieres et être remplies.

2. Tester le frontend (page web)
Ouvrir dans le navigateur : http://localhost:8000/ ou http://localhost:8000/stats.html (ne pas ouvrir les .html en double-clic, sinon "failed to fetch"). Anciennement : ouvrir le fichier c:\Users\frede\OneDrive\Documents\Cursor\webapp-foncier\frontend\index.html dans ton navigateur (double‑clic ou “Ouvrir avec…” un navigateur moderne).
Tu dois voir :
À gauche : les filtres (adresse BAN, type de local, surfaces, rayon, période, bouton Rechercher).
À droite : la carte + la zone “Ventes trouvées”.
3. Scénario de test
Dans le champ Adresse (BAN), tape une adresse réelle (ex. “10 rue de …, 91350 Grigny”), puis clique sur une suggestion.
La carte doit se centrer, avec un cercle correspondant au rayon (2 km par défaut).
Choisis :
Un type de local (ou laisse “Tous”),
Éventuellement une fourchette de surface,
Un rayon (ex. 1–3 km),
Une période (date min/max) si tu veux restreindre.
Clique sur Rechercher.
Le frontend appelle GET http://localhost:8000/api/ventes?....
Si des ventes correspondent, tu verras :
Un résumé “X vente(s) trouvée(s)”,
Une liste déroulante de ventes,
En sélectionnant une ligne, les détails s’affichent dessous (date, type, surface, prix, adresse, distance) et la carte se recentre sur la vente.
Si tu rencontres un message d’erreur (dans un popup ou dans la console du navigateur), dis-moi exactement ce que tu vois, et je t’indiquerai comment le corriger