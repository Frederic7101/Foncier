/**
 * Migre comparaison_scores_core.js pour utiliser comparaison_state.js (S).
 * Usage : node scripts/apply_comparaison_state.mjs
 * Ne pas réexécuter sur un fichier déjà migré (déclarations supprimées + remplacements doublons).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const srcPath = path.join(root, "comparaison_scores_core.js");

let lines = fs.readFileSync(srcPath, "utf8").split(/\r?\n/);

/** Plages 1-based inclusives à supprimer (état déjà dans comparaison_state.js) */
const ranges = [
  [3369, 3369],
  [3253, 3253],
  [669, 669],
  [590, 594],
  [588, 589],
  [580, 587],
  [572, 579],
  [563, 571],
  [562, 562],
  [525, 525],
  [515, 524],
  [513, 514],
  [511, 512],
  [509, 509],
  [507, 507],
  [505, 505],
  [503, 504],
  [267, 270],
  [204, 204],
  [180, 184],
  [178, 179],
  [65, 92],
  [4, 13]
];

ranges.sort((a, b) => b[0] - a[0]);
for (const [start, end] of ranges) {
  lines.splice(start - 1, end - start + 1);
}

let out = lines.join("\n");

out = out.replace(
  /import \{ initDistancesSection \} from '\.\/comparaison_filters\.js';/,
  "import { initDistancesSection } from './comparaison_filters.js';\nimport { S } from './comparaison_state.js';"
);

out = out.replace(
  /\(function \(\) \{\s*/,
  `(function () {
        const isLocal = (typeof window !== "undefined" && (window.location?.hostname === "localhost" || window.location?.hostname === "127.0.0.1" || window.location?.protocol === "file:"));
        S.API_BASE = isLocal && window.location?.port !== "8000" ? "http://localhost:8000" : "";

`
);

const ids = [
  "API_BASE",
  "geo",
  "listCommunes",
  "selectedCommunes",
  "lastComparaisonMode",
  "lastRenderTableArgs",
  "lastDistanceOverlay",
  "lastComparaisonDistanceScopeKey",
  "lastComparaisonRows",
  "lastComparaisonJobs",
  "lastRowsByFilterKey",
  "selectionMassMode",
  "excludedCommunes",
  "legendCustomMin",
  "legendCustomMax",
  "lastTableConfigs",
  "sortByTable",
  "mapViz",
  "refData",
  "communeMapSeq",
  "choroMapSeq",
  "COMMUNE_MAP_TAB_STORAGE_KEY",
  "CATEGORY_INDICATORS",
  "CATEGORY_LABELS",
  "TABLE_BLOCKS",
  "RENTA_UNAVAILABLE_KEY",
  "FILTER_KEY_SEP",
  "ALL_REGIONS_SPECIAL",
  "ALL_DEPTS_SPECIAL",
  "lastExclusiveTousClick",
  "COMMUNE_MAP_STOPWORDS",
  "GEOJSON_URLS",
  "REF_TYPE_LOGTS_FALLBACK",
  "REF_TYPE_SURF_FALLBACK",
  "REF_NB_PIECES_FALLBACK",
  "TABLE_PAGE_SIZE",
  "selectedCommunesSet",
  "refComparaisonControlsWired"
];

ids.sort((a, b) => b.length - a.length);
for (const id of ids) {
  out = out.replace(new RegExp(`\\b${id}\\b`, "g"), `S.${id}`);
}

out = out.replace(/S\.S\./g, "S.");

fs.writeFileSync(srcPath, out, "utf8");
console.log("OK", srcPath, "bytes", out.length);
