/**
 * Utilitaires partagés (formatage, normalisation) — sans logique DOM métier comparaison.
 */
import { S } from "./comparaison_state.js";

export function escapeHtml(s) {
  if (s == null) return "";
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

/**
 * Code département INSEE métropole sur 2 caractères (ex. 02 pour l'Aisne, pas 2).
 * Préserve 2A / 2B et les départements DOM à 3 chiffres (971…).
 */
export function normalizeDeptCodeTwoChars(code) {
  var x = String(code == null ? "" : code).trim().toUpperCase();
  if (!x) return "";
  if (/^2[AB]$/i.test(x)) return x;
  if (/^\d+$/.test(x)) {
    if (x.length >= 3 && (x.startsWith("97") || x.startsWith("98"))) {
      return x.slice(0, 3);
    }
    if (x.length === 1 && x >= "1" && x <= "9") return "0" + x;
    if (x.length === 2) return x;
    var n = parseInt(x, 10);
    if (n >= 1 && n <= 9) return "0" + n;
    if (n >= 10 && n <= 95) return String(n);
  }
  return x;
}

/**
 * Forme canonique pour comparaison de noms (communes, etc.), alignée sur le backend
 * _normalize_name_canonical et sur stats.js normalizeNameCanonical.
 */
export function normalizeNameCanonical(s) {
  if (s == null || typeof s !== "string") return "";
  var str = s.trim();
  if (!str) return "";
  var APOSTROPHE_VARIANTS = "\u2019\u02bc\u02b9\u2032";
  str = str.replace(/\s*\([^)]*\)\s*$/, "").trim();
  for (var i = 0; i < APOSTROPHE_VARIANTS.length; i++) {
    str = str.split(APOSTROPHE_VARIANTS[i]).join("");
  }
  str = str.replace(/'/g, "");
  var nfd = str.normalize("NFD");
  var sansAccent = nfd.replace(/\p{Mn}/gu, "");
  var lettersOnly = sansAccent.replace(/[^A-Za-z]/g, "");
  return lettersOnly.toUpperCase();
}

function _asciiFoldLower(w) {
  return String(w || "")
    .normalize("NFD")
    .replace(/\p{Mn}/gu, "")
    .toLowerCase();
}

function expandCommuneAbbrevToken(word) {
  var w = String(word || "").trim();
  if (!w) return w;
  var base = _asciiFoldLower(w).replace(/\.$/, "");
  if (base === "st") return "saint";
  if (base === "ste") return "sainte";
  if (base === "stes") return "saintes";
  if (base === "ss") return "saints";
  return w;
}

/**
 * Clé de correspondance API ↔ fond communal GeoJSON.
 */
export function normalizeCommuneNameForMapMatch(name) {
  var str = String(name || "").trim();
  if (!str) return "";
  while (/\s*\([^)]*\)\s*$/.test(str)) {
    str = str.replace(/\s*\([^)]*\)\s*$/, "").trim();
  }
  str = str.replace(/[-–—]/g, " ");
  var APOSTROPHE_VARIANTS = "\u2019\u02bc\u02b9\u2032";
  for (var i = 0; i < APOSTROPHE_VARIANTS.length; i++) {
    str = str.split(APOSTROPHE_VARIANTS[i]).join(" ");
  }
  str = str.replace(/'/g, " ");
  var rawTokens = str.split(/\s+/).filter(Boolean);
  var expanded = rawTokens.map(function (t) {
    return expandCommuneAbbrevToken(t);
  });
  var filtered = [];
  for (var k = 0; k < expanded.length; k++) {
    var tk = _asciiFoldLower(expanded[k]).replace(/\.$/, "");
    if (k + 1 < expanded.length && tk === "de") {
      var next = _asciiFoldLower(expanded[k + 1]).replace(/\.$/, "");
      if (next === "la" || next === "l" || next === "les") {
        k++;
        continue;
      }
    }
    if (S.COMMUNE_MAP_STOPWORDS[tk]) continue;
    filtered.push(expanded[k]);
  }
  var joined = filtered.join(" ").trim();
  var key = normalizeNameCanonical(joined);
  if (key.length >= 2) return key;
  return normalizeNameCanonical(name);
}

export function formatPct(value) {
  const v = Number(value);
  if (!Number.isFinite(v)) return "—";
  return v.toLocaleString("fr-FR", { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + " %";
}

export function formatNbLocaux(value) {
  if (value == null || value === "") return "—";
  var n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return String(Math.round(n));
}

export function getScoreLabel(key) {
  var labels = {
    renta_brute: "Renta. brute",
    renta_nette: "Renta. nette",
    renta_brute_maisons: "Renta. brute",
    renta_nette_maisons: "Renta. nette",
    renta_brute_appts: "Renta. brute",
    renta_nette_appts: "Renta. nette",
    taux_tfb: "Taux TF",
    taux_teom: "Taux TEOM"
  };
  return labels[key] || key;
}

export function makeFilterKey(typeLogt, typeSurf, nbPieces) {
  return (
    String(typeLogt || "") +
    S.FILTER_KEY_SEP +
    String(typeSurf || "") +
    S.FILTER_KEY_SEP +
    String(nbPieces || "")
  );
}

/** Mode d’analyse : communes | départements | régions (radio page comparaison). */
export function getComparaisonMode() {
  var r = document.querySelector('input[name="comparaison-mode"]:checked');
  return (r && r.value) || "communes";
}

export function getNiveauLabel() {
  var mode = getComparaisonMode();
  return mode === "communes" ? "Communes" : mode === "departements" ? "Départements" : "Régions";
}

/** Identifiant de région (API geo) pour un code département INSEE. */
export function getRegionForDepartment(codeDept) {
  var d = normalizeDeptCodeTwoChars(codeDept);
  if (!d) return null;
  var reg = (S.geo.regions || []).find(function (r) {
    return (r.departements || []).some(function (dep) {
      return normalizeDeptCodeTwoChars(dep) === d;
    });
  });
  return reg ? reg.id : null;
}
