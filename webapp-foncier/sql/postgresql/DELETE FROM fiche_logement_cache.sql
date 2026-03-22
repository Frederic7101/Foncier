-- Supprime les lignes de fiche_logement_cache dont la commune (ref_communes)
-- a plus d'un code postal dans le champ codes_postaux. 
DELETE FROM foncier.fiche_logement_cache c
WHERE EXISTS (
  SELECT 1
  FROM foncier.ref_communes r
  WHERE r.code_insee = c.code_insee
  and length(r.codes_postaux) > 5
);

-- =============================================================================
-- Vérification avant DELETE (communes dans cache, avec ventes dans vf_communes,
-- et avec renta_brute/renta_nette NULL dans indicateurs_communes).
-- Exécuter d'abord le COUNT puis le SELECT pour lister ; ensuite lancer le DELETE.
-- =============================================================================

-- Nombre de communes (ref_communes) présentes dans fiche_logement_cache,
-- ayant au moins un enregistrement dans vf_communes et renta_brute/renta_nette NULL dans indicateurs_communes.
SELECT COUNT(DISTINCT r.code_insee) AS nb_communes_a_invalider
FROM foncier.ref_communes r
INNER JOIN foncier.fiche_logement_cache c ON c.code_insee = r.code_insee
INNER JOIN foncier.indicateurs_communes i ON i.code_insee = r.code_insee AND i.renta_brute IS NULL AND i.renta_nette IS NULL
WHERE EXISTS (
  SELECT 1
  FROM foncier.vf_communes v
  WHERE (v.code_dept = r.dep_code
         OR v.code_dept = TRIM(LEADING '0' FROM r.dep_code)
         OR r.dep_code = LPAD(v.code_dept::text, 2, '0'))
   AND UPPER(REGEXP_REPLACE(unaccent(
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
           REGEXP_REPLACE(TRIM(r.nom_standard_majuscule), E'\\s*\\([^)]*\\)\\s*$', ''),
         U&'\2019', ''), U&'\02bc', ''), U&'\02b9', ''), U&'\2032', ''), '''', '')
       ), '[^a-zA-Z]', '', 'g'))
     = UPPER(REGEXP_REPLACE(unaccent(
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
           REGEXP_REPLACE(REGEXP_REPLACE(TRIM(v.commune), E'\\s+[0-9]{1,2}\\s*(ER|EME|E)?\\s*$', '', 'i'), E'\\s*\\([^)]*\\)\\s*$', ''),
         U&'\2019', ''), U&'\02bc', ''), U&'\02b9', ''), U&'\2032', ''), '''', '')
       ), '[^a-zA-Z]', '', 'g'))
);

-- Liste des communes concernées (code_insee, département, nom).
SELECT DISTINCT r.code_insee, r.dep_code, r.nom_standard_majuscule AS nom_commune
FROM foncier.ref_communes r
INNER JOIN foncier.fiche_logement_cache c ON c.code_insee = r.code_insee
INNER JOIN foncier.indicateurs_communes i ON i.code_insee = r.code_insee AND i.renta_brute IS NULL AND i.renta_nette IS NULL
WHERE EXISTS (
  SELECT 1
  FROM foncier.vf_communes v
  WHERE (v.code_dept = r.dep_code
         OR v.code_dept = TRIM(LEADING '0' FROM r.dep_code)
         OR r.dep_code = LPAD(v.code_dept::text, 2, '0'))
   AND UPPER(REGEXP_REPLACE(unaccent(
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
           REGEXP_REPLACE(TRIM(r.nom_standard_majuscule), E'\\s*\\([^)]*\\)\\s*$', ''),
         U&'\2019', ''), U&'\02bc', ''), U&'\02b9', ''), U&'\2032', ''), '''', '')
       ), '[^a-zA-Z]', '', 'g'))
     = UPPER(REGEXP_REPLACE(unaccent(
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
           REGEXP_REPLACE(REGEXP_REPLACE(TRIM(v.commune), E'\\s+[0-9]{1,2}\\s*(ER|EME|E)?\\s*$', '', 'i'), E'\\s*\\([^)]*\\)\\s*$', ''),
         U&'\2019', ''), U&'\02bc', ''), U&'\02b9', ''), U&'\2032', ''), '''', '')
       ), '[^a-zA-Z]', '', 'g'))
)
ORDER BY r.dep_code, r.nom_standard_majuscule;

-- Supprime les lignes de fiche_logement_cache dont la commune (ref_communes)
-- a au moins un enregistrement dans vf_communes (jointure code_dept + commune canonique)
-- et a renta_brute/renta_nette NULL dans indicateurs_communes.
-- Utilise la même logique de normalisation que le backend (arrondissements retirés côté vf_communes).
-- À exécuter pour invalider le cache des communes concernées (recalcul avec nouvelle logique).
--- ============================== liste des communes concernées
"07239"	"07"	"SAINT-GENEST-LACHAMP"
"09256"	"09"	"SAINT-BAUZEIL"
"13055"	"13"	"MARSEILLE"
"64182"	"64"	"CASTILLON (CANTON DE LEMBEYE)"
"69123"	"69"	"LYON"
"75056" "75"	"PARIS"
"97101"	"971"	"LES ABYMES"
"97306"	"973"	"MANA"
--- ==============================
--- ============================== liste des communes concernées
"07239"	"07"	"SAINT-GENEST-LACHAMP"
"13055"	"13"	"MARSEILLE"
"69123"	"69"	"LYON"
"75056"	"75"	"PARIS"
"97101"	"971"	"LES ABYMES"
"97306"	"973"	"MANA"
--- ==============================


DELETE FROM foncier.fiche_logement_cache c
WHERE EXISTS (
  SELECT 1
  FROM foncier.ref_communes r
  INNER JOIN foncier.vf_communes v
    ON (v.code_dept = r.dep_code
        OR v.code_dept = TRIM(LEADING '0' FROM r.dep_code)
        OR r.dep_code = LPAD(v.code_dept::text, 2, '0'))
   AND UPPER(REGEXP_REPLACE(unaccent(
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
           REGEXP_REPLACE(TRIM(r.nom_standard_majuscule), E'\\s*\\([^)]*\\)\\s*$', ''),
         U&'\2019', ''), U&'\02bc', ''), U&'\02b9', ''), U&'\2032', ''), '''', '')
       ), '[^a-zA-Z]', '', 'g'))
     = UPPER(REGEXP_REPLACE(unaccent(
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
           REGEXP_REPLACE(REGEXP_REPLACE(TRIM(v.commune), E'\\s+[0-9]{1,2}\\s*(ER|EME|E)?\\s*$', '', 'i'), E'\\s*\\([^)]*\\)\\s*$', ''),
         U&'\2019', ''), U&'\02bc', ''), U&'\02b9', ''), U&'\2032', ''), '''', '')
       ), '[^a-zA-Z]', '', 'g'))
  WHERE r.code_insee = c.code_insee
    AND EXISTS (
      SELECT 1 FROM foncier.indicateurs_communes i
      WHERE i.code_insee = r.code_insee AND i.renta_brute IS NULL AND i.renta_nette IS NULL
    )
);


