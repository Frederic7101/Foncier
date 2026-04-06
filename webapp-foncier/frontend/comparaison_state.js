/**
 * État mutable partagé entre les modules comparaison (sélection, filtres, tableaux, cartes).
 */
export const S = {
  API_BASE: "",
  geo: { regions: [], departements: [], deptNomByCode: {} },
  listCommunes: [],
  selectedCommunes: [],
  lastComparaisonMode: "communes",
  lastRenderTableArgs: null,
  lastDistanceOverlay: null,
  lastComparaisonDistanceScopeKey: null,
  lastComparaisonRows: [],
  lastComparaisonJobs: [],
  lastRowsByFilterKey: {},
  selectionMassMode: null,
  excludedCommunes: [],
  legendCustomMin: null,
  legendCustomMax: null,
  lastTableConfigs: [],
  sortByTable: {},
  mapViz: {
    map: null,
    layer: null,
    mode: null,
    communeMaps: [],
    multiChoroMaps: [],
    geoCache: { departements: null, regions: null, communesAll: null },
    mapSyncIgnore: false,
    lastCommuneMapScorePrincipal: null,
    /** Vues sauvegardées par sous-onglet cartes communes (dept / region / france) : { lat, lng, zoom }[] */
    communeViewsBySubTab: { dept: null, region: null, france: null },
    /** Cartes construites pendant que le panneau cartes était masqué → recadrage au 1er affichage */
    pendingCommuneMapLayoutRefit: false
  },
  COMMUNE_MAP_TAB_STORAGE_KEY: "comparaison_commune_map_subtab",
  refData: { type_logts: [], type_surf: [], nb_pieces: [] },
  REF_TYPE_LOGTS_FALLBACK: [
    { code: "TOUS", libelle: "Tous" },
    { code: "MAISON", libelle: "Maisons" },
    { code: "APPART", libelle: "Appartements" },
    { code: "LOCAL_INDUS", libelle: "Locaux indus. / comm." },
    { code: "PARKING", libelle: "Dépendances" },
    { code: "TERRAIN", libelle: "Terrains" },
    { code: "IMMEUBLE", libelle: "Immeubles" }
  ],
  REF_TYPE_SURF_FALLBACK: [
    { code: "TOUTES", libelle: "Toutes" },
    { code: "S1", libelle: "Tranche S1" },
    { code: "S2", libelle: "Tranche S2" },
    { code: "S3", libelle: "Tranche S3" },
    { code: "S4", libelle: "Tranche S4" },
    { code: "S5", libelle: "Tranche S5" }
  ],
  REF_NB_PIECES_FALLBACK: [
    { code: "TOUS", libelle: "Tous" },
    { code: "T1", libelle: "1 pièce (T1)" },
    { code: "T2", libelle: "2 pièces (T2)" },
    { code: "T3", libelle: "3 pièces (T3)" },
    { code: "T4", libelle: "4 pièces (T4)" },
    { code: "T5", libelle: "5 pièces et + (T5)" }
  ],
  communeMapSeq: 0,
  choroMapSeq: 0,
  GEOJSON_URLS: {
    departements: "data/carto/departements-version-simplifiee.geojson",
    regions: "data/carto/regions-version-simplifiee.geojson",
    communes: "data/carto/communes-version-simplifiee.geojson"
  },
  COMMUNE_MAP_STOPWORDS: {
    le: true,
    la: true,
    les: true,
    de: true,
    du: true,
    des: true,
    en: true,
    sur: true,
    sous: true,
    lès: true,
    lè: true,
    au: true,
    aux: true,
    à: true,
    un: true,
    une: true,
    d: true,
    l: true,
    chez: true,
    devant: true,
    entre: true,
    et: true,
    ou: true,
    dans: true,
    par: true,
    pour: true
  },
  CATEGORY_INDICATORS: { rentabilite: ["renta_brute", "renta_nette"], taxes: ["taux_tfb", "taux_teom"] },
  CATEGORY_LABELS: { rentabilite: "Rentabilité", taxes: "Taxes" },
  TABLE_BLOCKS: [
    { id: "all", keyMap: { renta_brute: "renta_brute", renta_nette: "renta_nette", taux_tfb: "taux_tfb", taux_teom: "taux_teom" } },
    { id: "maisons", keyMap: { renta_brute: "renta_brute_maisons", renta_nette: "renta_nette_maisons", taux_tfb: "taux_tfb", taux_teom: "taux_teom" } },
    { id: "appts", keyMap: { renta_brute: "renta_brute_appts", renta_nette: "renta_nette_appts", taux_tfb: "taux_tfb", taux_teom: "taux_teom" } },
    { id: "parking", keyMap: { renta_brute: "renta_brute_parking", renta_nette: "renta_nette_parking", taux_tfb: "taux_tfb", taux_teom: "taux_teom" } },
    { id: "local_indus", keyMap: { renta_brute: "renta_brute_local_indus", renta_nette: "renta_nette_local_indus", taux_tfb: "taux_tfb", taux_teom: "taux_teom" } },
    { id: "terrain", keyMap: { renta_brute: "renta_brute_terrain", renta_nette: "renta_nette_terrain", taux_tfb: "taux_tfb", taux_teom: "taux_teom" } },
    { id: "immeuble", keyMap: { renta_brute: "renta_brute_immeuble", renta_nette: "renta_nette_immeuble", taux_tfb: "taux_tfb", taux_teom: "taux_teom" } }
  ],
  RENTA_UNAVAILABLE_KEY: "__indicateur_non_calcule__",
  FILTER_KEY_SEP: "\u0001",
  ALL_REGIONS_SPECIAL: "__ALL_REGIONS__",
  ALL_DEPTS_SPECIAL: "__ALL_DEPTS__",
  lastExclusiveTousClick: { type: null, surf: null, pieces: null, regionDept: null, regionOnly: null, dept: null },
  TABLE_PAGE_SIZE: 50,
  refComparaisonControlsWired: false
};

S.selectedCommunesSet = new Set();
