# Plan – Migration & enchères (syntaxe terminal Cursor / Cmd)

## Contexte

- **Cursor** : le terminal intégré est en **PowerShell** (sauf si vous avez choisi Cmd).
- **Erreur 1046** : elle apparaît si on importe `ventes_notaire_dump.sql` sans avoir **créé la base `ventes_notaire`** avant. Le dump contient `USE ventes_notaire;` puis des `CREATE TABLE` ; si la base n’existe pas, `USE` échoue et il n’y a plus de base sélectionnée → erreur à la ligne du premier `CREATE TABLE`.

---

## 1. Migration foncier → ventes_notaire

### Terminal Cursor (PowerShell)

```powershell
cd c:\Users\frede\OneDrive\Documents\Cursor\sql

# 1) Dump de la base actuelle (mot de passe demandé)
& "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe" -u root -p foncier > foncier_dump.sql

# 2) Remplacer foncier par ventes_notaire (génère ventes_notaire_dump.sql)
#    Le script ajoute en tête du dump SET FOREIGN_KEY_CHECKS=0; SET UNIQUE_CHECKS=0;
#    pour accélérer l'import, et réactive les contraintes en fin de fichier.
.\remplace_foncier_ventes_notaire.ps1

# 3) Créer la base ventes_notaire (obligatoire avant import, évite 1046)
& "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p < create_ventes_notaire.sql

# 4) Importer le dump dans ventes_notaire
Get-Content .\ventes_notaire_dump.sql -Raw | & "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p ventes_notaire
```

Si le dump est très gros, éviter `Get-Content -Raw` (mémoire). Utiliser **Cmd** pour l’import (étape 4) :

```powershell
cmd /c "`"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe`" -u root -p ventes_notaire < ventes_notaire_dump.sql"
```

### Cmd (invite de commandes Windows)

```cmd
cd /d c:\Users\frede\OneDrive\Documents\Cursor\sql

REM 1) Dump
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe" -u root -p foncier > foncier_dump.sql

REM 2) Remplacement (à faire dans PowerShell : .\remplace_foncier_ventes_notaire.ps1)
REM    Ou en Cmd on ne peut pas faire le remplacement facilement ; lancer le .ps1 depuis PowerShell.

REM 3) Créer la base (évite ERROR 1046)
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p < create_ventes_notaire.sql

REM 4) Import dans ventes_notaire
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p ventes_notaire < ventes_notaire_dump.sql
```

**Important** : l’étape 3 doit être exécutée **avant** l’étape 4. Sans création de la base, vous aurez « ERROR 1046 at line 22: no database selected ».

---

## 2. Schéma enchères (Licitor)

### Terminal Cursor (PowerShell)

```powershell
cd c:\Users\frede\OneDrive\Documents\Cursor\sql

& "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p < schema_encheres.sql
```

### Cmd

```cmd
cd /d c:\Users\frede\OneDrive\Documents\Cursor\sql

"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p < schema_encheres.sql
```

---

## 3. Scraping Licitor

### Terminal Cursor (PowerShell)

```powershell
cd c:\Users\frede\OneDrive\Documents\Cursor\encheres
pip install -r requirements.txt
$env:MYSQL_PASSWORD = "votre_mot_de_passe"   # optionnel si déjà configuré
python scrap_licitor.py
```

---

## 4. Récap correction ERROR 1046

| Ordre | Action |
|-------|--------|
| 1 | Créer la base : `mysql -u root -p < sql\create_ventes_notaire.sql` |
| 2 | Importer le dump **en ciblant la base** : `mysql -u root -p ventes_notaire < ventes_notaire_dump.sql` |

Sans l’étape 1, `USE ventes_notaire;` dans le dump échoue (base inexistante), donc « no database selected » au premier `CREATE TABLE`.
