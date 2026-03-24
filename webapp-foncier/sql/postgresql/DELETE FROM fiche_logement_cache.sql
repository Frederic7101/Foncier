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


-- nb de communes où nb_locaux = NULL mais renta_brute n'est pas NULL ou renta_nette n'est pas NULL
-- 3388 occurrences; est-ce un pb de rafraîchissement ou parce qu'il n'y a pas de données dans vf_communes pour ces communes ?
select count(*) from foncier.indicateurs_communes ic
where ic.nb_locaux is null and (ic.renta_brute is not null or ic.renta_nette is not null)

-- 0 communes qui ne seraient pas dans ref_communes, donc elles y sont toutes
select * from foncier.indicateurs_communes ic
where ic.nb_locaux is null and (ic.renta_brute is not null or ic.renta_nette is not null)
and ic.code_insee not in (
	select rc.code_insee from foncier.ref_communes rc
	)

-- 429 communes seulement qui n'ont pas de données dans vf_communes avec le code_postal
select * from foncier.indicateurs_communes ic
where ic.nb_locaux is null and (ic.renta_brute is not null or ic.renta_nette is not null)
and ic.code_postal not in (
	select vfc.code_postal from foncier.vf_communes vfc
	)

-- 0 communes qui n'ont pas de données dans vf_communes avec le nom_standard_majuscule de ref_communes
-- donc toutes les communes concernées ont bien des données dans vf_communes
-- en conclusion : les 3388 communes ont plus probablement un pb de rafraîchissement
select * from foncier.indicateurs_communes ic
where ic.nb_locaux is null and (ic.renta_brute is not null or ic.renta_nette is not null)
and ic.code_insee not in (
	select rc.code_insee from foncier.ref_communes rc
	where UPPER(REGEXP_REPLACE(unaccent(
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
           REGEXP_REPLACE(TRIM(rc.nom_standard_majuscule), E'\\s*\\([^)]*\\)\\s*$', ''),
         U&'\2019', ''), U&'\02bc', ''), U&'\02b9', ''), U&'\2032', ''), '''', '')
       ), '[^a-zA-Z]', '', 'g')) 
       in (
			select UPPER(REGEXP_REPLACE(unaccent(
	         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    	       REGEXP_REPLACE(REGEXP_REPLACE(TRIM(vfc.commune), E'\\s+[0-9]{1,2}\\s*(ER|EME|E)?\\s*$', '', 'i'), E'\\s*\\([^)]*\\)\\s*$', ''),
        	 U&'\2019', ''), U&'\02bc', ''), U&'\02b9', ''), U&'\2032', ''), '''', '')
	       ), '[^a-zA-Z]', '', 'g')) from foncier.vf_communes vfc	
	));


-- 3388 Nombre de communes (ref_communes) présentes dans fiche_logement_cache,
-- ayant au moins un enregistrement dans vf_communes et nb_locaux = NULL et (renta_brute ou renta_nette != NULL) dans indicateurs_communes.
SELECT COUNT(DISTINCT r.code_insee) AS nb_communes_a_invalider
FROM foncier.ref_communes r
INNER JOIN foncier.fiche_logement_cache c ON c.code_insee = r.code_insee
INNER JOIN foncier.indicateurs_communes i ON i.code_insee = r.code_insee and (i.renta_brute IS not NULL or i.renta_nette IS not null) and (i.nb_locaux is null)
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

DELETE FROM foncier.fiche_logement_cache c
where exists (
	select 1 from foncier.indicateurs_communes ic
	where ic.code_insee = c.code_insee and (ic.renta_brute is not null or ic.renta_nette is not null) and (ic.nb_locaux is null)
)

-- 34820 fiches dans le cache
select count(*) from foncier.fiche_logement_cache c;

-- 31183 indicateurs => il manque 3637 indicateurs, même après avoir supprimé les fiches des communes qui n'ont pas d'indicateurs et un refresh-indicateurs en masse; pourquoi alors qu'avec un refresh-indicateurs individuel cela les ajoute bien
-- la cause était qu'une erreur lors du refresh-indicateurs en masse provoquait le rollback de la transaction et donc la suppression des indicateurs communes; c'est corrigé maintenant
select count(*) from foncier.indicateurs_communes ic;

-- recherche des fiches sans indicateurs : 3741
select * from foncier.fiche_logement_cache flc 
where flc.code_insee not in (
	select ic.code_insee from foncier.indicateurs_communes ic 
)

-- supprime les fiches des communes qui n'ont pas d'indicateurs
delete from foncier.fiche_logement_cache c
where c.code_insee not in (
	select ic.code_insee from foncier.indicateurs_communes ic 
)

delete from foncier.fiche_logement_cache c
where c.code_insee in ('01443')