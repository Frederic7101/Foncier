quelle est la source des communes dans geojson_cartes ?



Exécuté 22 commandes, fichiers trouvés
Exécuté 22 commandes, fichiers trouvés

J'ai tous les éléments pour répondre précisément.

Source du GeoJSON des communes
Fichier : frontend/data/carto/communes-version-simplifiee.geojson
Source : dépôt GitHub gregoiredavid/france-geojson

C'est la source la plus utilisée pour ce type de fichier en France, et tous les indices concordent :

Indice	Valeur observée	Cohérent avec
Nom du fichier	communes-version-simplifiee.geojson	Nommage exact du dépôt
Propriétés	{"code": "01073", "nom": "Ceyzérieu"}	Format exact (code INSEE + nom)
Taille	19,3 Mo	Cohérent avec la version simplifiée France entière
Paris	Code 75056 (commune unique, pas d'arrondissements)	COG INSEE standard
Château-Thierry	Code 02168 présent ✓	COG INSEE standard
Le code de chaque feature est le code INSEE COG (Code Officiel Géographique), identique au code_insee de ref_communes.

Implications pour le diagnostic
Le matching dans l'application se fait par props.code = code_insee → matching direct par code INSEE, pas par nom
Paris (75056), Lyon (69123) et Marseille (13055) sont des communes uniques dans le GeoJSON — les arrondissements n'y sont pas. C'est cohérent avec le traitement dédupliqué côté API
Un dans_geojson_cartes = 'non' signifie que le code_insee de ref_communes est absent ou mal formaté (ex. 2168 au lieu de 02168), pas un problème de nom
Vérifier la complétude
-- Communes de ref_communes absentes du GeoJSON (code_insee non trouvé)
SELECT code_insee, dep_code, nom_standard, population
FROM foncier.ref_communes
WHERE code_insee IS NOT NULL AND code_insee <> ''
  AND NOT EXISTS (
      SELECT 1 FROM foncier.geojson_communes g
      WHERE g.code_insee_geo = code_insee
  )
ORDER BY dep_code, nom_standard;

est-ce que ces données sont enregistrées localement ou bien est-ce qu'on interroge par API ?
comment faire pour ajouter une commune manquante dans ce référentiel geojson ?



Données GeoJSON : locales, pas d'API
Le fichier est 100% local : frontend/data/carto/communes-version-simplifiee.geojson (19,3 Mo, chargé via fetch("data/carto/communes-version-simplifiee.geojson") côté client, aucune requête externe).

Ajouter une commune manquante
Option 1 — Récupérer la géométrie depuis l'API officielle (recommandé)
L'API geo.api.gouv.fr fournit les contours GeoJSON en temps réel :

https://geo.api.gouv.fr/communes/02168?fields=nom,code,contour&format=geojson&geometry=contour

Remplacer 02168 par le code INSEE voulu. La réponse est directement un Feature GeoJSON avec :

{
  "type": "Feature",
  "geometry": { "type": "Polygon", "coordinates": [...] },
  "properties": { "code": "02168", "nom": "Château-Thierry" }
}

Option 2 — Récupérer depuis le dépôt source
Le fichier vient de gregoiredavid/france-geojson. La version complète (non simplifiée) contient toutes les communes ; si une commune est absente de la version simplifiée, elle peut y figurer.

Procédure d'ajout manuel dans le fichier local
Récupérer la feature via l'API ci-dessus
Ouvrir frontend/data/carto/communes-version-simplifiee.geojson
Insérer la feature dans le tableau "features": [...] — n'importe où dans la liste
S'assurer que les propriétés sont exactement "code" et "nom" (pas "code_insee", pas "name") :
{
  "type": "Feature",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[3.397, 49.051], [3.412, 49.058], ...]]
  },
  "properties": {
    "code": "02168",
    "nom": "Château-Thierry"
  }
}

⚠️ La géométrie du fichier simplifié est volontairement allégée (< 5 Ko par commune). La géométrie retournée par geo.api.gouv.fr avec geometry=contour peut être plus lourde — pour la simplifier, utiliser le paramètre &precision=0.001 dans l'URL API (précision en degrés).

Script PowerShell pour automatiser l'injection
Si plusieurs communes manquent dans le GeoJSON (détecté via dans_geojson_cartes = 'non' dans la vue diagnostic), voici comment les injecter en batch :

# Récupérer les codes manquants (exemple : depuis la vue diagnostic)
$codesManquants = @("02168", "75056")  # remplacer par vos codes

$geojsonPath = "frontend\data\carto\communes-version-simplifiee.geojson"
$geojson = Get-Content $geojsonPath -Raw | ConvertFrom-Json

foreach ($code in $codesManquants) {
    $url = "https://geo.api.gouv.fr/communes/${code}?fields=nom,code,contour&format=geojson&geometry=contour&precision=0.001"
    try {
        $feature = Invoke-RestMethod -Uri $url -Method Get
        # Normaliser les propriétés au format attendu
        $feature.properties = [PSCustomObject]@{ code = $code; nom = $feature.properties.nom }
        $geojson.features += $feature
        Write-Host "Ajouté : $code ($($feature.properties.nom))"
    } catch {
        Write-Warning "Erreur pour $code : $_"
    }
}

$geojson | ConvertTo-Json -Depth 20 -Compress | Set-Content $geojsonPath -Encoding UTF8
Write-Host "GeoJSON mis à jour."

Note : ConvertTo-Json -Depth 20 est nécessaire car les coordonnées polygonales sont profondément imbriquées. Sans -Depth suffisant, les coordonnées sont tronquées avec "...".-- =============================================================================
-- DIAGNOSTIC DE COMPLÉTUDE 
-- Fichier : diagnostic_completude_communes.sql
-- Objectif : pour chaque commune de ref_communes, indiquer sa présence ou
--            son absence dans chaque source de données de l'application.
--
-- Colonnes produites :
--   code_dept, nom_dept, nom_region, commune, code_insee, code_postal, population
--   dans_vf_communes            (données DVF brutes, match strict dep+CP+nom)
--   dans_vf_communes_partiel    (données DVF brutes, match dep+CP seulement → nom mismatch probable)
--   dans_indicateurs_communes   (indicateurs précalculés, match par code_insee)
--   dans_loyers_communes        (loyers de marché, match par code_insee)
--   dans_aides_logement_part    (% ménages bénéficiaires aides logement, match par code_insee)
--   dans_fiscalite_locale       (TFB / TEOM, match par code_insee)
--   dans_rpls_logements         (parc locatif social, match par dep_code + nom canonique)
--   dans_selecteur              (= dans_vf_communes : commune visible dans le sélecteur UI)
--   autocomplete_code_insee_ok  (code_insee non-vide → autocomplétion retourne un résultat exploitable)
--   dans_geojson_cartes         (code_insee présent dans le GeoJSON des communes, voir étape 1)
--
-- =============================================================================
-- ÉTAPE 1 : charger les codes INSEE du GeoJSON dans une table permanente
-- =============================================================================
-- Le fichier GeoJSON : frontend/data/carto/communes-version-simplifiee.geojson
-- Chaque feature a : {"code": "01073", "nom": "Ceyzérieu"}
-- La table est permanente (pas TEMP) pour pouvoir l'utiliser depuis n'importe
-- quel client SQL sans session psql.

CREATE TABLE IF NOT EXISTS foncier.geojson_communes (
    code_insee_geo varchar(6) PRIMARY KEY
);

-- ── Option A : COPY côté serveur (à utiliser dans DBeaver / pgAdmin / DataGrip) ──
-- Nécessite que le serveur PostgreSQL soit local (chemin accessible par le process postgres).
-- \COPY ne fonctionne QUE dans psql CLI ; utiliser COPY (sans backslash) dans les clients SQL.
--
-- 1. Générer le fichier CSV via PowerShell :
--    $geo = [System.IO.File]::ReadAllText('C:\Users\frede\OneDrive\Documents\Cursor\webapp-foncier\frontend\data\carto\communes-version-simplifiee.geojson') | ConvertFrom-Json
--    $geo.features.properties.code | Set-Content 'C:\Temp\geojson_codes_communes.csv'
--
-- 2. Charger depuis le client SQL (chemin vu par le SERVEUR PostgreSQL) :
--    TRUNCATE foncier.geojson_communes;
--    COPY foncier.geojson_communes (code_insee_geo)
--    FROM 'C:/Temp/geojson_codes_communes.csv'
--    WITH (FORMAT TEXT);

-- ── Option B : INSERT généré par PowerShell (fonctionne toujours) ─────────────
-- Produit un fichier .sql avec INSERT … VALUES prêt à coller dans le client SQL.
-- Exécuter dans PowerShell depuis le répertoire racine du projet :
--
--    $geo  = [System.IO.File]::ReadAllText('frontend\data\carto\communes-version-simplifiee.geojson') | ConvertFrom-Json
--    $vals = ($geo.features.properties.code | ForEach-Object { "('$_')" }) -join ","
--    "TRUNCATE foncier.geojson_communes; INSERT INTO foncier.geojson_communes VALUES $vals;" | Set-Content 'sql\insert_geojson_communes.sql'
--
-- Puis exécuter sql\insert_geojson_communes.sql dans le client SQL.

-- =============================================================================
-- ÉTAPE 2 : requête de diagnostic principale
-- =============================================================================

WITH

-- ── ref_communes : noms canoniques (unaccent + lettres A-Z seulement) ──────────
ref_norm AS (
    SELECT
        c.code_insee,
        c.dep_code,
        c.code_postal,
        c.nom_standard,
        c.nom_standard_majuscule,
        COALESCE(c.population::int, 0)                          AS population,
        d.nom_dept,
        r.nom_region,
        -- Même normalisation que _sql_norm_name_canonical() dans main.py :
        -- 1) TRIM 2) supprimer parenthèses finales 3) variantes apostrophe
        -- 4) unaccent 5) ne garder que A-Z 6) UPPER
        UPPER(REGEXP_REPLACE(unaccent(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REGEXP_REPLACE(TRIM(c.nom_standard_majuscule),
                    '\s*\([^)]*\)\s*$', ''),
                U&'\2019',''), U&'\02bc',''), U&'\02b9',''), U&'\2032',''), '''','')
        ), '[^a-zA-Z]', '', 'g'))                               AS nom_canonical
    FROM foncier.ref_communes c
    LEFT JOIN foncier.ref_departements d ON d.code_dept = c.dep_code
    LEFT JOIN foncier.ref_regions      r ON r.code_region = d.code_region
),

-- ── vf_communes : noms canoniques (+ suppression suffixe arrondissement) ──────
-- Même normalisation que _sql_norm_name_canonical_commune_vf() dans main.py
vf_norm AS (
    SELECT DISTINCT
        code_dept,
        LEFT(LPAD(code_postal::text, 5, '0'), 3)                AS cp3,
        UPPER(REGEXP_REPLACE(unaccent(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REGEXP_REPLACE(
                    -- Étape supplémentaire : retirer "PARIS 01", "LYON 2EME", etc.
                    REGEXP_REPLACE(TRIM(commune),
                        '\s+[0-9]{1,2}\s*(ER|EME|E)?\s*$', '', 'i'),
                '\s*\([^)]*\)\s*$', ''),
                U&'\2019',''), U&'\02bc',''), U&'\02b9',''), U&'\2032',''), '''','')
        ), '[^a-zA-Z]', '', 'g'))                               AS nom_canonical
    FROM foncier.vf_communes
),

-- ── vf_communes : présence souple (dep + 3 premiers chiffres CP, sans nom) ───
-- Utile pour détecter "données DVF présentes mais nom mismatch"
vf_loose AS (
    SELECT DISTINCT
        code_dept,
        LEFT(LPAD(code_postal::text, 5, '0'), 3)                AS cp3
    FROM foncier.vf_communes
),

-- ── rpls_logements : noms canoniques + normalisation du code département ──────
-- rpls.dep_code peut être stocké sur 2 ou 3 caractères avec ou sans zéro initial
rpls_norm AS (
    SELECT DISTINCT
        -- Normaliser dep_code : strip zéros initiaux, puis recoller à 2 chiffres
        -- pour les départements numériques (gère "02" → "02", "2" → "02", "2A" → "2A")
        CASE
            WHEN dep_code ~ '^[0-9]+$'
                THEN LPAD(REGEXP_REPLACE(dep_code, '^0+', ''), 2, '0')
            ELSE dep_code
        END                                                      AS dep_code_norm,
        UPPER(REGEXP_REPLACE(unaccent(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REGEXP_REPLACE(TRIM(commune),
                    '\s*\([^)]*\)\s*$', ''),
                U&'\2019',''), U&'\02bc',''), U&'\02b9',''), U&'\2032',''), '''','')
        ), '[^a-zA-Z]', '', 'g'))                               AS nom_canonical
    FROM foncier.rpls_logements
    WHERE commune IS NOT NULL AND commune <> ''
),

-- ── Déduplication des tables de référence (évite les doublons dans les LEFT JOIN) ──
indic_codes AS (SELECT DISTINCT code_insee FROM foncier.indicateurs_communes  WHERE code_insee IS NOT NULL),
loyers_codes AS (SELECT DISTINCT insee_c   FROM foncier.loyers_communes        WHERE insee_c   IS NOT NULL),
aides_codes  AS (SELECT DISTINCT codegeo   FROM foncier.aides_logement_part    WHERE codegeo   IS NOT NULL),
fisc_codes   AS (SELECT DISTINCT code_insee FROM foncier.fiscalite_locale      WHERE code_insee IS NOT NULL)

-- ── Requête principale ────────────────────────────────────────────────────────
SELECT
    rn.dep_code                                                  AS code_dept,
    rn.nom_dept,
    rn.nom_region,
    rn.nom_standard                                              AS commune,
    rn.code_insee,
    rn.code_postal,
    rn.population,

    -- ── Données DVF brutes ───────────────────────────────────────────────────
    -- Match strict : dep_code + 3 premiers chiffres CP + nom canonique
    -- = condition exacte utilisée dans le sélecteur de communes de l'UI
    CASE WHEN vf_strict.nom_canonical IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_vf_communes,

    -- Match partiel : dep_code + 3 premiers chiffres CP (sans le nom)
    -- "oui" avec dans_vf_communes="non" → données présentes mais NOM MISMATCH
    CASE WHEN vf_part.cp3 IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_vf_communes_partiel,

    -- ── Indicateurs précalculés ──────────────────────────────────────────────
    CASE WHEN ic.code_insee IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_indicateurs_communes,

    -- ── Loyers de marché ────────────────────────────────────────────────────
    CASE WHEN lc.insee_c IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_loyers_communes,

    -- ── Aides au logement ───────────────────────────────────────────────────
    CASE WHEN ac.codegeo IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_aides_logement_part,

    -- ── Fiscalité locale (TFB, TEOM, TH) ────────────────────────────────────
    CASE WHEN fc.code_insee IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_fiscalite_locale,

    -- ── Parc locatif social (RPLS) ───────────────────────────────────────────
    CASE WHEN rp.dep_code_norm IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_rpls_logements,

    -- ── Sélecteur de communes dans l'UI ─────────────────────────────────────
    -- Identique à dans_vf_communes : une commune apparaît dans le sélecteur
    -- si et seulement si le JOIN strict vf_communes → ref_communes réussit
    CASE WHEN vf_strict.nom_canonical IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_selecteur,

    -- ── Autocomplétion ───────────────────────────────────────────────────────
    -- L'autocomplétion interroge ref_communes directement → toute commune de
    -- ref_communes est trouvable par nom. La colonne indique si le code_insee
    -- est renseigné (sinon, la commune est trouvée mais aucune donnée ne peut
    -- être chargée via le chemin S1)
    CASE WHEN rn.code_insee IS NOT NULL AND rn.code_insee <> ''
         THEN 'oui' ELSE 'non' END                               AS autocomplete_code_insee_ok,

    -- ── Cartes GeoJSON ───────────────────────────────────────────────────────
    -- Le GeoJSON communes-version-simplifiee.geojson utilise props.code = code_insee
    -- pour le rendu choroplèthe. Nécessite d'avoir chargé geojson_communes (étape 1).
    CASE WHEN gc.code_insee_geo IS NOT NULL
         THEN 'oui' ELSE 'non' END                               AS dans_geojson_cartes

FROM ref_norm rn

-- vf_communes match STRICT (dep + cp3 + nom)
LEFT JOIN vf_norm vf_strict
    ON  vf_strict.code_dept     = rn.dep_code
    AND vf_strict.cp3           = LEFT(rn.code_postal, 5)
    AND vf_strict.nom_canonical = rn.nom_canonical

-- vf_communes match PARTIEL (dep + cp3 sans nom)
LEFT JOIN vf_loose vf_part
    ON  vf_part.code_dept = rn.dep_code
    AND vf_part.cp3       = LEFT(rn.code_postal, 5)

-- indicateurs_communes
LEFT JOIN indic_codes ic  ON ic.code_insee  = rn.code_insee

-- loyers_communes
LEFT JOIN loyers_codes lc ON lc.insee_c    = rn.code_insee

-- aides_logement_part
LEFT JOIN aides_codes  ac ON ac.codegeo    = rn.code_insee

-- fiscalite_locale
LEFT JOIN fisc_codes   fc ON fc.code_insee = rn.code_insee

-- rpls_logements (dep_code normalisé + nom canonique)
LEFT JOIN rpls_norm rp
    ON  rp.dep_code_norm  = rn.dep_code
    AND rp.nom_canonical  = rn.nom_canonical

-- GeoJSON cartes communes (table temporaire chargée à l'étape 1)
LEFT JOIN foncier.geojson_communes gc ON gc.code_insee_geo = rn.code_insee

ORDER BY rn.dep_code, rn.nom_standard;


-- =============================================================================
-- ÉTAPE 3 : exporter en CSV
-- =============================================================================
-- ⚠️  \COPY est une méta-commande psql uniquement.
--     Dans DBeaver / pgAdmin / DataGrip → exécuter la requête de l'étape 2
--     puis utiliser Export > CSV depuis l'interface (séparateur ";" , UTF-8).
--
-- Depuis psql CLI : remplacer \COPY ci-dessous par COPY et adapter le chemin.
-- Depuis un client SQL : utiliser la variante VIEW (voir fin de fichier).
--
-- ── Variante VIEW (à créer une seule fois, puis interroger librement) ─────────
-- Voir section "ÉTAPE 3b" en bas de fichier.
--
-- ── Variante COPY serveur (PostgreSQL local, chemin vu par le serveur) ────────
-- COPY (SELECT * FROM foncier.v_diagnostic_completude_communes)
-- TO 'C:/Users/frede/OneDrive/Documents/Cursor/webapp-foncier/diagnostic_completude_communes.csv'
-- WITH (FORMAT CSV, HEADER true, DELIMITER ';', ENCODING 'UTF8');

-- [Bloc \COPY conservé pour usage psql CLI uniquement]
\COPY (
    WITH
    ref_norm AS (
        SELECT
            c.code_insee,
            c.dep_code,
            c.code_postal,
            c.nom_standard,
            c.nom_standard_majuscule,
            COALESCE(c.population::int, 0)                          AS population,
            d.nom_dept,
            r.nom_region,
            UPPER(REGEXP_REPLACE(unaccent(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    REGEXP_REPLACE(TRIM(c.nom_standard_majuscule),
                        '\s*\([^)]*\)\s*$', ''),
                    U&'\2019',''), U&'\02bc',''), U&'\02b9',''), U&'\2032',''), '''','')
            ), '[^a-zA-Z]', '', 'g'))                               AS nom_canonical
        FROM foncier.ref_communes c
        LEFT JOIN foncier.ref_departements d ON d.code_dept = c.dep_code
        LEFT JOIN foncier.ref_regions      r ON r.code_region = d.code_region
    ),
    vf_norm AS (
        SELECT DISTINCT
            code_dept,
            LEFT(LPAD(code_postal::text, 5, '0'), 3)                AS cp3,
            UPPER(REGEXP_REPLACE(unaccent(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(commune),
                            '\s+[0-9]{1,2}\s*(ER|EME|E)?\s*$', '', 'i'),
                    '\s*\([^)]*\)\s*$', ''),
                    U&'\2019',''), U&'\02bc',''), U&'\02b9',''), U&'\2032',''), '''','')
            ), '[^a-zA-Z]', '', 'g'))                               AS nom_canonical
        FROM foncier.vf_communes
    ),
    vf_loose AS (
        SELECT DISTINCT
            code_dept,
            LEFT(LPAD(code_postal::text, 5, '0'), 3)                AS cp3
        FROM foncier.vf_communes
    ),
    rpls_norm AS (
        SELECT DISTINCT
            CASE
                WHEN dep_code ~ '^[0-9]+$'
                    THEN LPAD(REGEXP_REPLACE(dep_code, '^0+', ''), 2, '0')
                ELSE dep_code
            END                                                      AS dep_code_norm,
            UPPER(REGEXP_REPLACE(unaccent(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    REGEXP_REPLACE(TRIM(commune),
                        '\s*\([^)]*\)\s*$', ''),
                    U&'\2019',''), U&'\02bc',''), U&'\02b9',''), U&'\2032',''), '''','')
            ), '[^a-zA-Z]', '', 'g'))                               AS nom_canonical
        FROM foncier.rpls_logements
        WHERE commune IS NOT NULL AND commune <> ''
    ),
    indic_codes  AS (SELECT DISTINCT code_insee FROM foncier.indicateurs_communes  WHERE code_insee IS NOT NULL),
    loyers_codes AS (SELECT DISTINCT insee_c    FROM foncier.loyers_communes        WHERE insee_c   IS NOT NULL),
    aides_codes  AS (SELECT DISTINCT codegeo    FROM foncier.aides_logement_part    WHERE codegeo   IS NOT NULL),
    fisc_codes   AS (SELECT DISTINCT code_insee FROM foncier.fiscalite_locale       WHERE code_insee IS NOT NULL)
    SELECT
        rn.dep_code          AS code_dept,
        rn.nom_dept,
        rn.nom_region,
        rn.nom_standard      AS commune,
        rn.code_insee,
        rn.code_postal,
        rn.population,
        CASE WHEN vf_strict.nom_canonical IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_vf_communes,
        CASE WHEN vf_part.cp3 IS NOT NULL             THEN 'oui' ELSE 'non' END AS dans_vf_communes_partiel,
        CASE WHEN ic.code_insee  IS NOT NULL           THEN 'oui' ELSE 'non' END AS dans_indicateurs_communes,
        CASE WHEN lc.insee_c     IS NOT NULL           THEN 'oui' ELSE 'non' END AS dans_loyers_communes,
        CASE WHEN ac.codegeo     IS NOT NULL           THEN 'oui' ELSE 'non' END AS dans_aides_logement_part,
        CASE WHEN fc.code_insee  IS NOT NULL           THEN 'oui' ELSE 'non' END AS dans_fiscalite_locale,
        CASE WHEN rp.dep_code_norm IS NOT NULL         THEN 'oui' ELSE 'non' END AS dans_rpls_logements,
        CASE WHEN vf_strict.nom_canonical IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_selecteur,
        CASE WHEN rn.code_insee IS NOT NULL AND rn.code_insee <> ''
             THEN 'oui' ELSE 'non' END                                           AS autocomplete_code_insee_ok,
        CASE WHEN gc.code_insee_geo IS NOT NULL        THEN 'oui' ELSE 'non' END AS dans_geojson_cartes
    FROM ref_norm rn
    LEFT JOIN vf_norm      vf_strict ON vf_strict.code_dept = rn.dep_code AND vf_strict.cp3 = LEFT(rn.code_postal, 3) AND vf_strict.nom_canonical = rn.nom_canonical
    LEFT JOIN vf_loose     vf_part   ON vf_part.code_dept   = rn.dep_code AND vf_part.cp3   = LEFT(rn.code_postal, 3)
    LEFT JOIN indic_codes  ic        ON ic.code_insee  = rn.code_insee
    LEFT JOIN loyers_codes lc        ON lc.insee_c     = rn.code_insee
    LEFT JOIN aides_codes  ac        ON ac.codegeo     = rn.code_insee
    LEFT JOIN fisc_codes   fc        ON fc.code_insee  = rn.code_insee
    LEFT JOIN rpls_norm    rp        ON rp.dep_code_norm = rn.dep_code AND rp.nom_canonical = rn.nom_canonical
    LEFT JOIN foncier.geojson_communes gc ON gc.code_insee_geo = rn.code_insee
    ORDER BY rn.dep_code, rn.nom_standard
) TO '/tmp/diagnostic_completude_communes.csv'  -- adapter le chemin (psql CLI)
WITH (FORMAT CSV, HEADER true, DELIMITER ';', ENCODING 'UTF8');


-- =============================================================================
-- ÉTAPE 3b : créer une VIEW permanente (alternative au \COPY)
-- =============================================================================
-- Une fois créée, l'export depuis n'importe quel client SQL se réduit à :
--   SELECT * FROM foncier.v_diagnostic_completude_communes;
-- Puis Export CSV depuis l'interface du client.
-- Ou depuis psql :
--   COPY (SELECT * FROM foncier.v_diagnostic_completude_communes)
--   TO 'C:/Users/frede/.../diagnostic_completude_communes.csv'
--   WITH (FORMAT CSV, HEADER true, DELIMITER ';', ENCODING 'UTF8');

CREATE OR REPLACE VIEW foncier.v_diagnostic_completude_communes AS
WITH
ref_norm AS (
    SELECT c.code_insee, c.dep_code, c.code_postal, c.nom_standard,
        COALESCE(c.population::int, 0) AS population,
        d.nom_dept, r.nom_region,
        UPPER(REGEXP_REPLACE(unaccent(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REGEXP_REPLACE(TRIM(c.nom_standard_majuscule),'\s*\([^)]*\)\s*$',''),
            U&'\2019',''),U&'\02bc',''),U&'\02b9',''),U&'\2032',''),'''','')
        ),'[^a-zA-Z]','','g')) AS nom_canonical
    FROM foncier.ref_communes c
    LEFT JOIN foncier.ref_departements d ON d.code_dept  = c.dep_code
    LEFT JOIN foncier.ref_regions      r ON r.code_region = d.code_region
),
vf_norm AS (
    SELECT DISTINCT code_dept,
        LEFT(LPAD(code_postal::text,5,'0'),3) AS cp3,
        UPPER(REGEXP_REPLACE(unaccent(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REGEXP_REPLACE(REGEXP_REPLACE(TRIM(commune),'\s+[0-9]{1,2}\s*(ER|EME|E)?\s*$','','i'),'\s*\([^)]*\)\s*$',''),
            U&'\2019',''),U&'\02bc',''),U&'\02b9',''),U&'\2032',''),'''','')
        ),'[^a-zA-Z]','','g')) AS nom_canonical
    FROM foncier.vf_communes
),
vf_loose AS (
    SELECT DISTINCT code_dept, LEFT(LPAD(code_postal::text,5,'0'),3) AS cp3
    FROM foncier.vf_communes
),
rpls_norm AS (
    SELECT DISTINCT
        CASE WHEN dep_code ~ '^[0-9]+$' THEN LPAD(REGEXP_REPLACE(dep_code,'^0+',''),2,'0')
             ELSE dep_code END AS dep_code_norm,
        UPPER(REGEXP_REPLACE(unaccent(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REGEXP_REPLACE(TRIM(commune),'\s*\([^)]*\)\s*$',''),
            U&'\2019',''),U&'\02bc',''),U&'\02b9',''),U&'\2032',''),'''','')
        ),'[^a-zA-Z]','','g')) AS nom_canonical
    FROM foncier.rpls_logements WHERE commune IS NOT NULL AND commune <> ''
),
ic AS (SELECT DISTINCT code_insee FROM foncier.indicateurs_communes WHERE code_insee IS NOT NULL),
lc AS (SELECT DISTINCT insee_c    FROM foncier.loyers_communes       WHERE insee_c   IS NOT NULL),
ac AS (SELECT DISTINCT codegeo    FROM foncier.aides_logement_part   WHERE codegeo   IS NOT NULL),
fc AS (SELECT DISTINCT code_insee FROM foncier.fiscalite_locale      WHERE code_insee IS NOT NULL)
SELECT
    rn.dep_code AS code_dept, rn.nom_dept, rn.nom_region,
    rn.nom_standard AS commune, rn.code_insee, rn.code_postal, rn.population,
    CASE WHEN vs.nom_canonical IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_vf_communes,
    CASE WHEN vp.cp3           IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_vf_communes_partiel,
    CASE WHEN ic.code_insee    IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_indicateurs_communes,
    CASE WHEN lc.insee_c       IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_loyers_communes,
    CASE WHEN ac.codegeo       IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_aides_logement_part,
    CASE WHEN fc.code_insee    IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_fiscalite_locale,
    CASE WHEN rp.dep_code_norm IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_rpls_logements,
    CASE WHEN vs.nom_canonical IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_selecteur,
    CASE WHEN rn.code_insee IS NOT NULL AND rn.code_insee <> '' THEN 'oui' ELSE 'non' END AS autocomplete_code_insee_ok,
    CASE WHEN gc.code_insee_geo IS NOT NULL THEN 'oui' ELSE 'non' END AS dans_geojson_cartes
FROM ref_norm rn
LEFT JOIN vf_norm      vs ON vs.code_dept=rn.dep_code AND vs.cp3=LEFT(rn.code_postal,3) AND vs.nom_canonical=rn.nom_canonical
LEFT JOIN vf_loose     vp ON vp.code_dept=rn.dep_code AND vp.cp3=LEFT(rn.code_postal,3)
LEFT JOIN ic              ON ic.code_insee  = rn.code_insee
LEFT JOIN lc              ON lc.insee_c     = rn.code_insee
LEFT JOIN ac              ON ac.codegeo     = rn.code_insee
LEFT JOIN fc              ON fc.code_insee  = rn.code_insee
LEFT JOIN rpls_norm    rp ON rp.dep_code_norm=rn.dep_code AND rp.nom_canonical=rn.nom_canonical
LEFT JOIN foncier.geojson_communes gc ON gc.code_insee_geo = rn.code_insee
ORDER BY rn.dep_code, rn.nom_standard;


-- =============================================================================
-- REQUÊTES DE SYNTHÈSE (optionnel : résumé par département)
-- =============================================================================

-- Nombre de communes sans données DVF par département
SELECT
    dep_code AS code_dept,
    nom_dept,
    COUNT(*)                                                         AS total_communes,
    SUM(CASE WHEN (SELECT COUNT(*) FROM foncier.vf_communes v2
                   WHERE v2.code_dept = c.dep_code
                     AND LEFT(LPAD(v2.code_postal::text,5,'0'),3) = LEFT(c.code_postal,3)
                  ) = 0 THEN 1 ELSE 0 END)                          AS sans_vf_communes,
    SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM foncier.indicateurs_communes i
                              WHERE i.code_insee = c.code_insee)
             THEN 1 ELSE 0 END)                                     AS sans_indicateurs,
    SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM foncier.loyers_communes l
                              WHERE l.insee_c = c.code_insee)
             THEN 1 ELSE 0 END)                                     AS sans_loyers,
    SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM foncier.aides_logement_part a
                              WHERE a.codegeo = c.code_insee)
             THEN 1 ELSE 0 END)                                     AS sans_aides_logement,
    SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM foncier.fiscalite_locale f
                              WHERE f.code_insee = c.code_insee)
             THEN 1 ELSE 0 END)                                     AS sans_fiscalite,
    SUM(CASE WHEN c.code_insee IS NULL OR c.code_insee = ''
             THEN 1 ELSE 0 END)                                     AS sans_code_insee
FROM foncier.ref_communes c
LEFT JOIN foncier.ref_departements d ON d.code_dept = c.dep_code
GROUP BY c.dep_code, d.nom_dept
ORDER BY c.dep_code;
