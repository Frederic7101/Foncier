-- Étape 2 : (re)remplir vf_all_ventes en agrégant valeursfoncieres (Vente) par transaction
-- (lieu + date + type_local). On conserve la table : création si absente, sinon TRUNCATE + INSERT.
-- Si la table a encore l’ancienne structure (nb_mutations, prix_moyen…), exécuter une fois 02_alter_vf_all_ventes.sql.
-- Modification : 06/04/2026 ajout des autres types de locaux pour récupérer toutes les ventes de vf_all_ventes et les visualiser dans le FO + conversion sous PostgreSQL 

-- Créer la table uniquement si elle n’existe pas (structure cible)
CREATE TABLE IF NOT EXISTS foncier.vf_all_ventes (
    code_dept               VARCHAR(5)   NOT NULL,
    code_postal             VARCHAR(10)  NOT NULL,
    commune                 VARCHAR(100) NOT NULL,
    type_local              VARCHAR(50)  NOT NULL,
    annee                   INT          NOT NULL,
    date_mutation           DATE         NOT NULL,
    adresse_norm            VARCHAR(512),
    voie                    VARCHAR(255),
    no_voie                 VARCHAR(20),
    nb_lignes               INT          NOT NULL,
    valeur_fonciere         DECIMAL(15,2),
    surface_reelle_bati     DECIMAL(15,2),
    prix_m2                 DECIMAL(15,2),
    nombre_pieces_principales INT,
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    last_refreshed          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Exemple de contrainte UNIQUE (ajustez les colonnes selon vos besoins)
    CONSTRAINT vf_all_ventes_uniq UNIQUE (code_dept, code_postal, adresse_norm, no_voie)
);

comment on column foncier.vf_all_ventes.adresse_norm is 'Adresse normalisée (lieu de la transaction)';
comment on column foncier.vf_all_ventes.nb_lignes is 'Nombre de lignes valeursfoncieres agrégées';
comment on column foncier.vf_all_ventes.valeur_fonciere is 'Moyenne valeur_fonciere';
comment on column foncier.vf_all_ventes.surface_reelle_bati is 'Moyenne surface_reelle_bati';
comment on column foncier.vf_all_ventes.prix_m2 is 'valeur_fonciere/surface_reelle_bati (pondéré)';
comment on column foncier.vf_all_ventes.nombre_pieces_principales is 'Moyenne nombre_pieces_principales';


-- Vider la table puis réinsérer les données agrégées
TRUNCATE TABLE vf_all_ventes;

-- Agrégation : une ligne par (date_mutation, adresse normalisée, type_local)
INSERT INTO vf_all_ventes (
  code_dept,
  code_postal,
  commune,
  type_local,
  annee,
  date_mutation,
  adresse_norm,
  voie,
  no_voie,
  nb_lignes,
  valeur_fonciere,
  surface_reelle_bati,
  prix_m2,
  nombre_pieces_principales,
  latitude,
  longitude
)
SELECT
  v.code_departement     AS code_dept,
  v.code_postal         AS code_postal,
  v.commune             AS commune,
  v.type_local          AS type_local,
  EXTRACT(YEAR FROM v.date_mutation)     AS annee,               -- ← remplacement de YEAR()
  v.date_mutation       AS date_mutation,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune) AS adresse_norm,
  MAX(v.voie)           AS voie,
  MAX(v.no_voie)        AS no_voie,
  COUNT(*)              AS nb_lignes,
  ROUND(AVG(v.valeur_fonciere), 2) AS valeur_fonciere,
  ROUND(AVG(v.surface_reelle_bati), 2) AS surface_reelle_bati,
  ROUND(SUM(v.valeur_fonciere) / NULLIF(SUM(v.surface_reelle_bati), 0), 2) AS prix_m2,
  ROUND(AVG(v.nombre_pieces_principales), 0) AS nombre_pieces_principales,
  MAX(v.latitude)      AS latitude,
  MAX(v.longitude)      AS longitude
FROM valeursfoncieres v
WHERE v.nature_mutation = 'Vente'
  AND v.type_local IN ('Appartement', 'Maison', 'Dépendance', 'Local industriel. commercial ou assimilé')
GROUP BY
  v.code_departement,
  v.code_postal,
  v.commune,
  v.type_local,
  v.date_mutation,
  CONCAT_WS(' ', COALESCE(v.no_voie,''), COALESCE(v.type_de_voie,''), COALESCE(v.voie,''), v.code_postal, v.commune);