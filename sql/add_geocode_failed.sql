-- Migration : marquer les adresses pour lesquelles BAN ne renvoie rien,
-- afin que geocode_ban.py ne les retraite pas à chaque exécution.
-- À exécuter une fois sur une base existante.
--
-- Sous Windows (PowerShell), < ne fonctionne pas. Utiliser :
--   Get-Content .\sql\add_geocode_failed.sql -Raw | mysql -u root -p foncier
-- Ou avec CMD (redirection possible) :
--   cmd /c "mysql -u root -p foncier < sql\add_geocode_failed.sql"
-- (en étant à la racine du projet, et mysql dans le PATH)

-- Si la colonne existe déjà, ignorer l'erreur "Duplicate column name".
ALTER TABLE adresses_geocodees
  ADD COLUMN geocode_failed TINYINT(1) NOT NULL DEFAULT 0
  COMMENT '1 = BAN interrogé sans résultat (ne plus retraiter)';
