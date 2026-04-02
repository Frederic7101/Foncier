/**
 * Cartes Leaflet : choroplèthe départements/régions, communes, légende.
 */
import { S } from "./comparaison_state.js";
import {
  escapeHtml,
  normalizeDeptCodeTwoChars,
  normalizeCommuneNameForMapMatch,
  getScoreLabel,
  getRegionForDepartment
} from "./comparaison_utils.js";
import {
  buildFilteredRowsByFilterKeyForMaps,
  mergeRowsForBounds,
  normalizeCodeInseeFr
} from "./comparaison_table.js";

function getFranceMetroLeafletBounds() {
  if (typeof L === "undefined") return null;
  return L.latLngBounds([41.26, -5.26], [51.23, 9.84]);
}

/**
 * Score numérique pour cartes : ignorer null et chaîne vide explicitement.
 * En JavaScript, Number(null) === 0 : sans ce garde-fou, une donnée absente est traitée comme 0 et les cartes
 * (ex. Terrains / Parkings) restent affichées alors qu’aucune commune n’a de valeur pour cette colonne.
 */
export function numericScoreFromRow(r, key) {
  if (!key || key === S.RENTA_UNAVAILABLE_KEY) return NaN;
  var raw = r[key];
  if (raw == null || raw === "") return NaN;
  var v = Number(raw);
  return Number.isFinite(v) ? v : NaN;
}

/** Une carte n’est rendue que si au moins une ligne a une valeur finie pour la colonne score de ce job (vérifié par carte, pas globalement). */
export function jobHasMapDisplayableData(rows, job) {
  if (!job || !job.scoreKey) return false;
  if (job.scoreKey === S.RENTA_UNAVAILABLE_KEY) return false;
  rows = rows || [];
  return rows.some(function (r) {
    return Number.isFinite(numericScoreFromRow(r, job.scoreKey));
  });
}

export function filterJobsForMapRendering(jobs, rowsByFilterKey) {
  return (jobs || []).filter(function (j) {
    return jobHasMapDisplayableData(rowsByFilterKey[j.filterKey] || [], j);
  });
}

// "dept" | "region" | "france" | null — mémorise si la sélection en masse a eu lieu
// Communes retirées d'une sélection en masse (pour envoyer exclude_code_insee au backend)
// Bornes personnalisées de la légende (null = auto)
export function getCommuneMapSubTab() {
  try {
    var v = sessionStorage.getItem(S.COMMUNE_MAP_TAB_STORAGE_KEY);
    if (v === "dept" || v === "region" || v === "france") return v;
  } catch (e) {}
  return "dept";
}
export function setCommuneMapSubTab(tab) {
  if (tab !== "dept" && tab !== "region" && tab !== "france") return;
  try {
    sessionStorage.setItem(S.COMMUNE_MAP_TAB_STORAGE_KEY, tab);
  } catch (e) {}
}

export function syncCommuneMapSubtabButtons(active) {
  var root = document.getElementById("comparaison-commune-map-subtabs");
  if (!root) return;
  var buttons = root.querySelectorAll("[data-commune-map-tab]");
  for (var i = 0; i < buttons.length; i += 1) {
    var btn = buttons[i];
    var t = btn.getAttribute("data-commune-map-tab");
    var isAct = t === active;
    if (isAct) btn.classList.add("is-active");
    else btn.classList.remove("is-active");
    btn.setAttribute("aria-selected", isAct ? "true" : "false");
  }
}

export function refreshCommuneMapsIfCurrentTab() {
  var modeEl = document.querySelector('input[name="comparaison-mode"]:checked');
  if (!modeEl || modeEl.value !== "communes") return;
  var sp = S.mapViz.lastCommuneMapScorePrincipal;
  if (sp == null || !S.lastComparaisonJobs.length) return;
  renderComparaisonMap(S.lastComparaisonJobs, S.lastRowsByFilterKey, "communes", sp, {
    preserveLegendBounds: true
  });
}

function formatLegendValue(value) {
  var v = Number(value);
  if (!Number.isFinite(v)) return "—";
  return v.toLocaleString("fr-FR", { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + " %";
}

function parseLegendNumberInput(raw) {
  if (raw == null) return NaN;
  var s = String(raw).trim().replace(/\s/g, "").replace(/%/g, "").replace(",", ".");
  return parseFloat(s);
}

function createIgnBaseLayer() {
  // Fond IGN détaillé (PLAN IGN v2) via WMTS Géoplateforme.
  return L.tileLayer(
    (S.API_BASE || "") + "/api/ign-tiles/{z}/{x}/{y}.png",
    {
      maxZoom: 19,
      tileSize: 256,
      attribution: '&copy; <a href="https://www.ign.fr/" target="_blank" rel="noopener noreferrer">IGN</a> (cache local)'
    }
  );
}

function createOsmFallbackLayer() {
  return L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a>'
  });
}

function attachPreferredBaseLayer(map) {
  if (!map || typeof L === "undefined") return;
  var ignLayer = createIgnBaseLayer();
  var switched = false;
  function switchToFallback() {
    if (switched) return;
    switched = true;
    try { map.removeLayer(ignLayer); } catch (e) {}
    createOsmFallbackLayer().addTo(map);
  }
  ignLayer.on("tileerror", switchToFallback);
  ignLayer.addTo(map);
}

export function clearCommuneMaps() {
  if (Array.isArray(S.mapViz.communeMaps)) {
    S.mapViz.communeMaps.forEach(function (entry) {
      if (entry && entry.resizeObserver && typeof entry.resizeObserver.disconnect === "function") {
        try {
          entry.resizeObserver.disconnect();
        } catch (eRo) {}
      }
      if (entry && entry.map && typeof entry.map.remove === "function") {
        entry.map.remove();
      }
    });
  }
  S.mapViz.communeMaps = [];
  var grid = document.getElementById("comparaison-communes-maps-grid");
  if (grid) {
    grid.innerHTML = "";
    grid.hidden = true;
  }
}

export function resetComparaisonMapVisuals() {
  S.mapViz.mapSyncIgnore = false;
  var singleMap = document.getElementById("comparaison-score-map-single");
  var mapsZone = document.getElementById("comparaison-maps-zone");
  if (singleMap) singleMap.style.display = "none";
  if (mapsZone) {
    mapsZone.innerHTML = "";
    mapsZone.style.display = "";
  }
  if (Array.isArray(S.mapViz.multiChoroMaps)) {
    S.mapViz.multiChoroMaps.forEach(function (entry) {
      if (entry && entry.map && typeof entry.map.remove === "function") {
        try { entry.map.remove(); } catch (e) {}
      }
    });
  }
  S.mapViz.multiChoroMaps = [];
  if (S.mapViz.layer && S.mapViz.map) {
    S.mapViz.map.removeLayer(S.mapViz.layer);
    S.mapViz.layer = null;
  }
  clearCommuneMaps();
}

export function clearComparaisonMap() {
  var mapWrap = document.getElementById("comparaison-map-wrap");
  var legend = document.getElementById("comparaison-map-legend");
  var status = document.getElementById("comparaison-map-status");
  var communeHint = document.getElementById("comparaison-map-commune-legend-hint");
  var subTabs = document.getElementById("comparaison-commune-map-subtabs");
  if (mapWrap) mapWrap.setAttribute("aria-hidden", "true");
  if (legend) legend.hidden = true;
  if (communeHint) communeHint.hidden = true;
  if (subTabs) subTabs.hidden = true;
  if (status) status.textContent = "";
  resetComparaisonMapVisuals();
  S.mapViz.mode = null;
}

function deptCodeFromInsee(inseeCode) {
  var c = String(inseeCode || "").trim().toUpperCase();
  if (!c) return "";
  if (c.startsWith("97") || c.startsWith("98")) return c.slice(0, 3);
  return c.slice(0, 2);
}

function ensureCommunesGeoJsonAll() {
  if (S.mapViz.geoCache.communesAll) return Promise.resolve(S.mapViz.geoCache.communesAll);
  return fetch(S.GEOJSON_URLS.communes).then(function (r) {
    if (!r.ok) throw new Error("Chargement du fond communal impossible (" + r.status + ").");
    return r.json();
  }).then(function (gj) {
    S.mapViz.geoCache.communesAll = gj;
    return gj;
  });
}

function buildDeptGeoJsonFromAllCommunes(allGeoJson, codeDept) {
  var code = String(codeDept || "").trim().toUpperCase();
  if (!allGeoJson || !Array.isArray(allGeoJson.features)) {
    return { type: "FeatureCollection", features: [] };
  }
  var features = allGeoJson.features.filter(function (f) {
    var props = (f && f.properties) || {};
    return deptCodeFromInsee(props.code || "") === code;
  });
  return { type: "FeatureCollection", features: features };
}

function buildRegionGeoJsonFromAllCommunes(allGeoJson, regionId) {
  var rid = String(regionId || "").trim();
  var reg = (S.geo.regions || []).find(function (r) {
    return String(r.id) === rid;
  });
  if (!reg || !Array.isArray(reg.departements) || !allGeoJson || !Array.isArray(allGeoJson.features)) {
    return { type: "FeatureCollection", features: [] };
  }
  var deptSet = new Set(reg.departements.map(function (d) { return String(d).trim().toUpperCase(); }));
  var features = allGeoJson.features.filter(function (f) {
    var props = (f && f.properties) || {};
    return deptSet.has(deptCodeFromInsee(props.code || ""));
  });
  return { type: "FeatureCollection", features: features };
}

function isMetropolitanDeptCode(codeDept) {
  var d = String(codeDept || "").trim().toUpperCase();
  if (!d) return false;
  if ((d.startsWith("97") || d.startsWith("98")) && d.length >= 3) return false;
  return true;
}

function collectMetropolitanDeptCodesFromRows(rows) {
  var s = new Set();
  (rows || []).forEach(function (r) {
    var d = normalizeDeptCodeTwoChars(r.code_dept);
    if (d && isMetropolitanDeptCode(d)) s.add(d);
  });
  return s;
}

function filterCommuneFeaturesByMetropolitanDeptSet(baseFeatures, deptSet) {
  if (!deptSet || !deptSet.size || !Array.isArray(baseFeatures)) return [];
  return baseFeatures.filter(function (f) {
    var props = (f && f.properties) || {};
    var dc = deptCodeFromInsee(props.code || "");
    return dc && deptSet.has(dc) && isMetropolitanDeptCode(dc);
  });
}

function buildInseeLookupFromPartitionRows(rows) {
  var buckets = {};
  (rows || []).forEach(function (r) {
    var ins = normalizeCodeInseeFr(r.code_insee);
    if (!ins) return;
    if (!buckets[ins]) buckets[ins] = { communeRaw: r.communeRaw || "", scores: [] };
    if (r.score != null && Number.isFinite(Number(r.score))) buckets[ins].scores.push(Number(r.score));
  });
  var out = {};
  Object.keys(buckets).forEach(function (k) {
    var arr = buckets[k].scores;
    var nk = normalizeCodeInseeFr(k);
    out[nk] = {
      name: buckets[k].communeRaw,
      score: arr.length ? arr.reduce(function (a, b) { return a + b; }, 0) / arr.length : null
    };
  });
  return out;
}

function classifyCommuneFeatureForStyle(feature, valuesByCommune, inseeLookup) {
  var props = (feature && feature.properties) || {};
  var insee = normalizeCodeInseeFr(props.code || "");
  var normGeoName = normalizeCommuneNameForMapMatch(props.nom || props.name || "");
  var byInsee = inseeLookup && inseeLookup[insee];
  var byName = valuesByCommune && valuesByCommune[normGeoName];
  if (byInsee) {
    var sv = byInsee.score != null && Number.isFinite(Number(byInsee.score)) ? Number(byInsee.score) : null;
    if (sv == null) {
      return { kind: "empty", score: null };
    }
    var normRow = normalizeCommuneNameForMapMatch(byInsee.name || "");
    var nameMismatch = normRow !== "" && normRow !== normGeoName;
    if (nameMismatch) return { kind: "name_mismatch", score: sv };
    return { kind: "score", score: sv };
  }
  if (byName) {
    var sv2 = byName.score != null && Number.isFinite(Number(byName.score)) ? Number(byName.score) : null;
    return { kind: sv2 != null ? "score" : "empty", score: sv2 };
  }
  return { kind: "neutral", score: null };
}

function getCommunePolygonStyle(feature, valuesByCommune, inseeLookup, min, max) {
  var c = classifyCommuneFeatureForStyle(feature, valuesByCommune, inseeLookup);
  if (c.kind === "neutral") {
    return { color: "#e5e7eb", weight: 0.25, fillColor: "#f8fafc", fillOpacity: 0.32 };
  }
  if (c.kind === "name_mismatch") {
    return { color: "#ca8a04", weight: 0.35, fillColor: "#fef9c3", fillOpacity: 0.55 };
  }
  if (c.kind === "empty") {
    return { color: "#6b7280", weight: 0.35, fillColor: "#d1d5db", fillOpacity: 0.52 };
  }
  return {
    color: "#15803d",
    weight: 0.35,
    fillColor: scoreColor(c.score, min, max),
    fillOpacity: 0.55
  };
}

/** Clés des communes présentes dans le tableau filtré (carte France métro : le reste du département en neutre). */
function buildFilteredCommuneTableKeys(rows) {
  var inseeSet = {};
  var nameNormSet = {};
  (rows || []).forEach(function (r) {
    var ci = normalizeCodeInseeFr(r.code_insee);
    if (ci) inseeSet[ci] = true;
    var nn = normalizeCommuneNameForMapMatch(r.commune || r.nom_commune || r.communeRaw || "");
    if (nn) nameNormSet[nn] = true;
  });
  return { inseeSet: inseeSet, nameNormSet: nameNormSet };
}
function communePolygonInFilteredTableKeys(feature, keys) {
  if (!keys || (!keys.inseeSet && !keys.nameNormSet)) return true;
  var props = (feature && feature.properties) || {};
  var ci = normalizeCodeInseeFr(props.code != null ? props.code : props.code_insee);
  if (ci && keys.inseeSet[ci]) return true;
  var nn = normalizeCommuneNameForMapMatch(props.nom || props.name || "");
  if (nn && keys.nameNormSet[nn]) return true;
  return false;
}
function getCommunePolygonStyleForFranceMassTable(feature, valuesByCommune, inseeLookup, min, max, filteredKeys) {
  if (filteredKeys && !communePolygonInFilteredTableKeys(feature, filteredKeys)) {
    return { color: "#e5e7eb", weight: 0.25, fillColor: "#f8fafc", fillOpacity: 0.32 };
  }
  return getCommunePolygonStyle(feature, valuesByCommune, inseeLookup, min, max);
}

function communeFeatureTooltipHtml(feature, valuesByCommune, inseeLookup, scoreKey) {
  var props = (feature && feature.properties) || {};
  var nom = props.nom || props.name || "Commune";
  var cl = classifyCommuneFeatureForStyle(feature, valuesByCommune, inseeLookup);
  var label;
  if (cl.kind === "neutral") label = "Hors périmètre du tableau filtré";
  else if (cl.kind === "name_mismatch") label = "Pas de correspondance du nom (données / carte)";
  else if (cl.kind === "empty") label = "Indicateur non renseigné";
  else label = escapeHtml(formatLegendValue(cl.score));
  return (
    "<strong>" +
    escapeHtml(nom) +
    "</strong><br/>" +
    escapeHtml(getScoreLabel(scoreKey)) +
    " : " +
    label
  );
}

function getFeatureCode(props, mode) {
  var keys = mode === "regions"
    ? ["code", "code_region", "codeRegion", "region_code", "insee_reg"]
    : ["code", "code_dept", "codeDepartement", "insee_dep", "dep_code"];
  for (var i = 0; i < keys.length; i += 1) {
    var v = props ? props[keys[i]] : null;
    if (v != null && String(v).trim() !== "") return String(v).trim();
  }
  return "";
}

function getFeatureName(props, mode) {
  var keys = mode === "regions"
    ? ["nom", "name", "nom_region", "region_name"]
    : ["nom", "name", "nom_dept", "departement_name"];
  for (var i = 0; i < keys.length; i += 1) {
    var v = props ? props[keys[i]] : null;
    if (v != null && String(v).trim() !== "") return String(v).trim();
  }
  return "";
}

function getLegendRawBounds() {
  var im = document.getElementById("legend-input-min");
  var ix = document.getElementById("legend-input-max");
  var a = im && im._rawMin != null && Number.isFinite(Number(im._rawMin)) ? Number(im._rawMin) : 0;
  var b = ix && ix._rawMax != null && Number.isFinite(Number(ix._rawMax)) ? Number(ix._rawMax) : 100;
  if (!Number.isFinite(a)) a = 0;
  if (!Number.isFinite(b)) b = 100;
  return { rawMin: a, rawMax: b };
}

/** Met à jour la barre de dégradé + les ticks de graduation selon les bornes actives. */
function updateLegendGradient(rawMin, rawMax) {
  var lo = S.legendCustomMin !== null ? S.legendCustomMin : rawMin;
  var hi = S.legendCustomMax !== null ? S.legendCustomMax : rawMax;
  var minEl  = document.getElementById("comparaison-map-legend-min");
  var maxEl  = document.getElementById("comparaison-map-legend-max");
  var ticks  = document.getElementById("comparaison-map-legend-ticks");
  if (minEl) minEl.textContent = "Min : " + formatLegendValue(lo);
  if (maxEl) maxEl.textContent = "Max : " + formatLegendValue(hi);
  // Graduation : ticks tous les 20 % (0 % … 100 %)
  if (ticks) {
    var steps = [0, 0.2, 0.4, 0.6, 0.8, 1];
    ticks.innerHTML = steps.map(function (t) {
      var val = lo + t * (hi - lo);
      return '<span class="tick" style="left:' + (t * 100) + '%">' + formatLegendValue(val) + '</span>';
    }).join("");
  }
  // Sync des inputs
  var inMin = document.getElementById("legend-input-min");
  var inMax = document.getElementById("legend-input-max");
  var rMin  = document.getElementById("legend-range-min");
  var rMax  = document.getElementById("legend-range-max");
  if (inMin) inMin.value = Number.isFinite(lo) ? String(lo) : "";
  if (inMax) inMax.value = Number.isFinite(hi) ? String(hi) : "";
  if (rMin)  { rMin.min = rawMin; rMin.max = rawMax; rMin.value = lo; }
  if (rMax)  { rMax.min = rawMin; rMax.max = rawMax; rMax.value = hi; }
}

/** Recolorie toutes les cartes visibles avec les nouvelles bornes min/max. */
function recolorAllMaps() {
  var rb = getLegendRawBounds();
  var lo = S.legendCustomMin !== null ? S.legendCustomMin : rb.rawMin;
  var hi = S.legendCustomMax !== null ? S.legendCustomMax : rb.rawMax;
  // Cartes choroplèthes (département / région)
  (S.mapViz.multiChoroMaps || []).forEach(function (entry) {
    if (!entry || !entry.layer) return;
    entry.layer.setStyle(function (feature) {
      var props = (feature && feature.properties) || {};
      var code = getFeatureCode(props, entry.mode || "departements");
      var row = entry.rowByCode && entry.rowByCode[code];
      var v = row ? numericScoreFromRow(row, entry.scoreKey) : null;
      return {
        fillColor: (v != null && Number.isFinite(v)) ? scoreColor(v, lo, hi) : "#d1d5db",
        fillOpacity: (v != null) ? 0.8 : 0.3,
        color: "#374151", weight: 0.8
      };
    });
  });
  // Cartes communes
  (S.mapViz.communeMaps || []).forEach(function (entry) {
    if (!entry || !entry.layer) return;
    entry.layer.setStyle(function (feature) {
      if (entry.franceMassFilteredKeys) {
        return getCommunePolygonStyleForFranceMassTable(
          feature,
          entry.valuesByCommune,
          entry.inseeLookup || null,
          lo,
          hi,
          entry.franceMassFilteredKeys
        );
      }
      return getCommunePolygonStyle(
        feature,
        entry.valuesByCommune,
        entry.inseeLookup || null,
        lo,
        hi
      );
    });
  });
}

/** Branche les contrôles min/max sur la légende (une seule fois par chargement de page). */
function initLegendControls(rawMin, rawMax) {
  var inMin = document.getElementById("legend-input-min");
  var inMax = document.getElementById("legend-input-max");
  var rMin  = document.getElementById("legend-range-min");
  var rMax  = document.getElementById("legend-range-max");
  if (inMin) inMin._rawMin = rawMin;
  if (inMax) inMax._rawMax = rawMax;
  if (initLegendControls._wired) return;
  initLegendControls._wired = true;

  function applyMin(v) {
    var val = parseLegendNumberInput(v);
    if (!Number.isFinite(val)) return;
    S.legendCustomMin = val;
    var b = getLegendRawBounds();
    updateLegendGradient(b.rawMin, b.rawMax);
    recolorAllMaps();
  }
  function applyMax(v) {
    var val = parseLegendNumberInput(v);
    if (!Number.isFinite(val)) return;
    S.legendCustomMax = val;
    var b = getLegendRawBounds();
    updateLegendGradient(b.rawMin, b.rawMax);
    recolorAllMaps();
  }
  if (inMin) { inMin.addEventListener("change", function () { applyMin(this.value); }); }
  if (inMax) { inMax.addEventListener("change", function () { applyMax(this.value); }); }
  if (rMin)  {
    rMin.addEventListener("input", function () {
      applyMin(this.value);
      if (inMin && S.legendCustomMin != null && Number.isFinite(S.legendCustomMin)) inMin.value = String(S.legendCustomMin);
    });
  }
  if (rMax)  {
    rMax.addEventListener("input", function () {
      applyMax(this.value);
      if (inMax && S.legendCustomMax != null && Number.isFinite(S.legendCustomMax)) inMax.value = String(S.legendCustomMax);
    });
  }
}

function scoreColor(value, min, max) {
  var v = Number(value);
  if (!Number.isFinite(v)) return "#d1d5db";
  if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) return "#22c55e";
  var t = Math.max(0, Math.min(1, (v - min) / (max - min)));
  if (t < 0.2) return "#f0fdf4";
  if (t < 0.4) return "#bbf7d0";
  if (t < 0.6) return "#4ade80";
  if (t < 0.8) return "#16a34a";
  return "#14532d";
}

function boundsForChoroplethSelection(geojson, mapMode, rows) {
  if (!geojson || !Array.isArray(geojson.features) || typeof L === "undefined") return null;
  var codes = new Set();
  rows.forEach(function (r) {
    var code = mapMode === "regions" ? String(r.code_region || "").trim() : String(r.code_dept || "").trim();
    if (code) codes.add(code);
  });
  if (codes.size === 0) return null;
  var b = L.latLngBounds([]);
  var ok = false;
  geojson.features.forEach(function (feature) {
    var code = getFeatureCode((feature && feature.properties) || {}, mapMode);
    if (!codes.has(code)) return;
    try {
      var fb = L.geoJSON(feature).getBounds();
      if (fb && fb.isValid && fb.isValid()) {
        b.extend(fb);
        ok = true;
      }
    } catch (e) {}
  });
  return ok && b.isValid() ? b : null;
}

function applySameBoundsToMaps(maps, bounds, options) {
  if (!maps || !maps.length || typeof L === "undefined") return false;
  if (!bounds || typeof bounds.isValid !== "function" || !bounds.isValid()) return false;
  S.mapViz.mapSyncIgnore = true;
  var opts = options || { padding: [12, 12], maxZoom: 11 };
  maps.forEach(function (m) {
    try {
      m.fitBounds(bounds, opts);
    } catch (e) {}
  });
  S.mapViz.mapSyncIgnore = false;
  return true;
}

function bindLinkedLeafletMaps(maps) {
  if (!maps || maps.length < 2 || typeof L === "undefined") return;
  maps.forEach(function (m) {
    function syncOthers() {
      if (S.mapViz.mapSyncIgnore) return;
      S.mapViz.mapSyncIgnore = true;
      var center = m.getCenter();
      var zoom = m.getZoom();
      maps.forEach(function (other) {
        if (other === m) return;
        try {
          var oc = other.getCenter();
          var oz = other.getZoom();
          if (
            Math.abs(oc.lat - center.lat) > 1e-7 ||
            Math.abs(oc.lng - center.lng) > 1e-7 ||
            Math.abs(oz - zoom) > 0.001
          ) {
            other.setView(center, zoom, { animate: false });
          }
        } catch (e2) {}
      });
      S.mapViz.mapSyncIgnore = false;
    }
    m.on("moveend", syncOthers);
    m.on("zoomend", syncOthers);
  });
}

function invalidateMapsSoon(maps, delayMs) {
  var d = delayMs == null ? 100 : delayMs;
  setTimeout(function () {
    (maps || []).forEach(function (m) {
      if (m && typeof m.invalidateSize === "function") m.invalidateSize();
    });
  }, d);
}

function ensureGeoJson(mode) {
  if (mode !== "regions" && mode !== "departements") return Promise.resolve(null);
  if (S.mapViz.geoCache[mode]) return Promise.resolve(S.mapViz.geoCache[mode]);
  return fetch(S.GEOJSON_URLS[mode]).then(function (r) {
    if (!r.ok) throw new Error("Chargement du fond de carte impossible (" + r.status + ").");
    return r.json();
  }).then(function (gj) {
    S.mapViz.geoCache[mode] = gj;
    return gj;
  });
}
function renderChoroplethIntoDiv(divId, rows, displayMode, scoreKey, min, max) {
  var mapMode = displayMode;
  return ensureGeoJson(mapMode).then(function (geojson) {
    if (typeof L === "undefined" || !geojson) throw new Error("Librairie cartographique indisponible.");
    var map = L.map(divId, { zoomControl: true, scrollWheelZoom: true, minZoom: 4, maxZoom: 11 });
    attachPreferredBaseLayer(map);
    map.setView([46.7, 2.5], 5);
    var codesInScope = new Set();
    rows.forEach(function (r) {
      var code = mapMode === "regions" ? String(r.code_region || "").trim() : String(r.code_dept || "").trim();
      if (code) codesInScope.add(code);
    });
    var valueByCode = {};
    rows.forEach(function (r) {
      var code = mapMode === "regions" ? String(r.code_region || "").trim() : String(r.code_dept || "").trim();
      var val = numericScoreFromRow(r, scoreKey);
      if (!code || !Number.isFinite(val)) return;
      valueByCode[code] = val;
    });
    if (codesInScope.size === 0 && Object.keys(valueByCode).length > 0) {
      Object.keys(valueByCode).forEach(function (k) {
        codesInScope.add(k);
      });
    }
    var layer = L.geoJSON(geojson, {
      style: function (feature) {
        var code = getFeatureCode((feature && feature.properties) || {}, mapMode);
        var inScope = codesInScope.size === 0 ? false : codesInScope.has(code);
        var raw = valueByCode[code];
        var value = inScope ? (Number.isFinite(raw) ? raw : null) : null;
        var active = inScope;
        var hasScore = active && value !== null;
        return {
          color: active ? "#475569" : "#cbd5e1",
          weight: active ? 1.1 : 0.7,
          fillColor: !active ? "#f1f5f9" : hasScore ? scoreColor(value, min, max) : "#d1d5db",
          fillOpacity: active ? 0.43 : 0.11
        };
      },
      onEachFeature: function (feature, lyr) {
        var props = (feature && feature.properties) || {};
        var code = getFeatureCode(props, mapMode);
        var name = getFeatureName(props, mapMode) || code || "Zone";
        var inScope = codesInScope.size === 0 ? false : codesInScope.has(code);
        var raw = valueByCode[code];
        var value = inScope ? (Number.isFinite(raw) ? raw : null) : null;
        var content = "<strong>" + escapeHtml(name) + "</strong>";
        if (code) content += " (" + escapeHtml(code) + ")";
        content +=
          "<br/>" +
          escapeHtml(getScoreLabel(scoreKey)) +
          " : " +
          (value !== null ? escapeHtml(formatLegendValue(value)) : "—");
        lyr.bindTooltip(content, { sticky: true });
      }
    }).addTo(map);
    // Stocker rowByCode/scoreKey/mode pour permettre le recoloriage via les contrôles min/max
    var rowByCode = {};
    rows.forEach(function (r) {
      var code = mapMode === "regions" ? String(r.code_region || "").trim() : String(r.code_dept || "").trim();
      if (code) rowByCode[code] = r;
    });
    S.mapViz.multiChoroMaps.push({ map: map, layer: layer, rowByCode: rowByCode, scoreKey: scoreKey, mode: mapMode });
  });
}

function aggregateCommuneScoresFromRows(deptRows) {
  var buckets = {};
  deptRows.forEach(function (x) {
    if (!buckets[x.communeNorm]) {
      buckets[x.communeNorm] = { name: x.communeRaw, scores: [] };
    }
    buckets[x.communeNorm].scores.push(x.score);
  });
  var out = {};
  Object.keys(buckets).forEach(function (k) {
    var arr = buckets[k].scores;
    var finite = arr.filter(function (s) {
      return s != null && Number.isFinite(Number(s));
    });
    var score =
      finite.length > 0
        ? finite.reduce(function (a, b) {
            return a + Number(b);
          }, 0) / finite.length
        : null;
    out[k] = { name: buckets[k].name, score: score };
  });
  return out;
}

/** Regroupe les lignes API par département pour une colonne score (type, tranche S/T, etc.). */
function partitionRowsByDeptForCommuneMaps(rows, scoreKey) {
  var byDept = {};
  (rows || []).forEach(function (r) {
    var deptCode = normalizeDeptCodeTwoChars(r.code_dept);
    var communeName = normalizeCommuneNameForMapMatch(r.commune || r.nom_commune || "");
    if (!deptCode || !communeName) return;
    var raw = r[scoreKey];
    var num = raw != null && raw !== "" ? Number(raw) : NaN;
    var score = Number.isFinite(num) ? num : null;
    if (!byDept[deptCode]) byDept[deptCode] = [];
    byDept[deptCode].push({
      communeNorm: communeName,
      communeRaw: r.commune || r.nom_commune || "",
      score: score,
      code_insee: String(r.code_insee || "").trim()
    });
  });
  var deptEntries = Object.keys(byDept).map(function (code) {
    return { code: code, rows: byDept[code] };
  });
  // Tri par code département (numérique si possible, sinon alpha)
  deptEntries.sort(function (a, b) {
    var na = parseInt(a.code, 10), nb = parseInt(b.code, 10);
    if (!isNaN(na) && !isNaN(nb)) return na - nb;
    return a.code.localeCompare(b.code, "fr");
  });
  return { byDept: byDept, limited: deptEntries, deptEntriesTotal: deptEntries };
}

function partitionRowsByRegionForCommuneMaps(rows, scoreKey) {
  var byReg = {};
  function regionCodeForRow(r) {
    var rc = String(r.code_region || "").trim();
    if (rc) return rc;
    var d = normalizeDeptCodeTwoChars(r.code_dept);
    if (!d) return "";
    var id = getRegionForDepartment(d);
    return id != null ? String(id) : "";
  }
  (rows || []).forEach(function (r) {
    var regCode = regionCodeForRow(r);
    var communeName = normalizeCommuneNameForMapMatch(r.commune || r.nom_commune || "");
    if (!regCode || !communeName) return;
    var raw = r[scoreKey];
    var num = raw != null && raw !== "" ? Number(raw) : NaN;
    var score = Number.isFinite(num) ? num : null;
    if (!byReg[regCode]) byReg[regCode] = [];
    byReg[regCode].push({
      communeNorm: communeName,
      communeRaw: r.commune || r.nom_commune || "",
      score: score,
      code_insee: String(r.code_insee || "").trim()
    });
  });
  var regEntries = Object.keys(byReg).map(function (code) {
    return { code: code, rows: byReg[code] };
  });
  regEntries.sort(function (a, b) {
    var na = parseInt(a.code, 10), nb = parseInt(b.code, 10);
    if (!isNaN(na) && !isNaN(nb)) return na - nb;
    return a.code.localeCompare(b.code, "fr");
  });
  return { byRegion: byReg, limited: regEntries, regionEntriesTotal: regEntries };
}

/**
 * Cadrage initial par carte : Leaflet a besoin que le conteneur ait une taille réelle (surtout la 1ère carte).
 * Chaque appel est indépendant (pas d’état partagé entre cartes).
 */
function scheduleIndependentCommuneMapFit(map, layer, selBounds, fitOpts) {
  function applyFit() {
    try {
      if (map && typeof map.invalidateSize === "function") {
        map.invalidateSize({ animate: false });
      }
      if (selBounds && selBounds.isValid && selBounds.isValid()) {
        map.fitBounds(selBounds, fitOpts);
      } else if (layer && layer.getBounds && layer.getBounds().isValid()) {
        map.fitBounds(layer.getBounds(), fitOpts);
      }
    } catch (e) {}
  }
  requestAnimationFrame(function () {
    requestAnimationFrame(applyFit);
  });
  setTimeout(applyFit, 80);
  setTimeout(applyFit, 200);
  setTimeout(applyFit, 400);
  setTimeout(applyFit, 650);
}

/**
 * Cartes communales : sous-onglets (département / région / France métropolitaine), grille par critère si besoin.
 * Même échelle de légende pour tous les panneaux ; zoom / pan indépendants (sauf liaison éventuelle choroplèthe).
 */
/**
 * France métropolitaine : une carte (par job si plusieurs critères) avec tous les polygones
 * des départements métropolitains contenant au moins une commune du tableau, coloration des
 * communes filtrées, molette + redimensionnement vertical du conteneur.
 */
function renderCommunesMassMap(jobs, rowsByFilterKey, min, max) {
  var status = document.getElementById("comparaison-map-status");
  var singleMap = document.getElementById("comparaison-score-map-single");
  var grid = document.getElementById("comparaison-communes-maps-grid");
  if (!grid) return;

  clearCommuneMaps();
  if (singleMap) singleMap.style.display = "none";

  grid.hidden = false;

  ensureCommunesGeoJsonAll()
    .then(function (allGeoJson) {
      if (typeof L === "undefined") throw new Error("Librairie cartographique indisponible.");
      if (status) status.textContent = "";

      var baseFeatures = allGeoJson.features || [];
      var allMaps = [];
      var fitOpts = { padding: [12, 12], maxZoom: 11 };
      var totalMaps = 0;

      jobs.forEach(function (job) {
        var rows = rowsByFilterKey[job.filterKey] || [];
        var scoreKey = job.scoreKey;
        var deptMetSet = collectMetropolitanDeptCodesFromRows(rows);
        if (!deptMetSet.size) return;

        var features = filterCommuneFeaturesByMetropolitanDeptSet(baseFeatures, deptMetSet);
        if (!features.length) return;

        var valuesByCommune = aggregateCommuneScoresFromRows(
          rows.map(function (r) {
            return {
              communeNorm: normalizeCommuneNameForMapMatch(r.commune || ""),
              communeRaw: r.commune || "",
              score: numericScoreFromRow(r, scoreKey)
            };
          })
        );
        var partitionRows = rows.map(function (r) {
          var raw = r[scoreKey];
          var num = raw != null && raw !== "" ? Number(raw) : NaN;
          return {
            communeNorm: normalizeCommuneNameForMapMatch(r.commune || r.nom_commune || ""),
            communeRaw: r.commune || r.nom_commune || "",
            score: Number.isFinite(num) ? num : null,
            code_insee: String(r.code_insee || "").trim()
          };
        });
        var inseeLookup = buildInseeLookupFromPartitionRows(partitionRows);
        var filteredGeoJson = { type: "FeatureCollection", features: features };
        // Aligné sur les lignes renvoyées pour le tableau (pas d’ajout depuis la sélection UI).
        var franceMassFilteredKeys = buildFilteredCommuneTableKeys(partitionRows);

        var jobWrap = document.createElement("div");
        jobWrap.className = "comparaison-communes-maps-job";
        if (jobs.length > 1) {
          var titleJob = document.createElement("h4");
          titleJob.className = "comparaison-communes-maps-job-title";
          titleJob.textContent = job.groupTitle + " — " + job.cardTitle;
          jobWrap.appendChild(titleJob);
          if (job.criteriaLine) {
            var critP = document.createElement("p");
            critP.className = "comparaison-table-criteria";
            critP.style.marginBottom = "0.5rem";
            critP.textContent = job.criteriaLine;
            jobWrap.appendChild(critP);
          }
        }
        var innerGrid = document.createElement("div");
        innerGrid.className = "comparaison-communes-maps-grid-inner";
        jobWrap.appendChild(innerGrid);
        grid.appendChild(jobWrap);

        S.communeMapSeq += 1;
        var mapId = "comparaison-commune-map-" + S.communeMapSeq;
        var card = document.createElement("div");
        card.className = "comparaison-commune-map-card";
        card.style.width = "100%";
        card.innerHTML =
          '<div id="' +
          escapeHtml(mapId) +
          '" class="comparaison-commune-map comparaison-commune-map--france-met"></div>';
        innerGrid.appendChild(card);

        var map = L.map(mapId, { zoomControl: true, scrollWheelZoom: true, minZoom: 4, maxZoom: 13 });
        attachPreferredBaseLayer(map);

        var layer = L.geoJSON(filteredGeoJson, {
          style: function (feature) {
            return getCommunePolygonStyleForFranceMassTable(
              feature,
              valuesByCommune,
              inseeLookup,
              min,
              max,
              franceMassFilteredKeys
            );
          },
          onEachFeature: function (feature, fLayer) {
            fLayer.bindTooltip(
              communeFeatureTooltipHtml(feature, valuesByCommune, inseeLookup, scoreKey),
              { sticky: true }
            );
          }
        });

        layer.addTo(map);

        var frBounds = getFranceMetroLeafletBounds();
        var frFitOpts = { padding: [12, 12], maxZoom: 6 };
        function fitFranceMetroView() {
          try {
            map.invalidateSize({ animate: false });
            if (frBounds && frBounds.isValid && frBounds.isValid()) {
              map.fitBounds(frBounds, frFitOpts);
            }
          } catch (eF) {}
        }
        requestAnimationFrame(function () {
          requestAnimationFrame(fitFranceMetroView);
        });
        setTimeout(fitFranceMetroView, 80);
        setTimeout(fitFranceMetroView, 250);
        setTimeout(fitFranceMetroView, 500);

        var massEntry = {
          map: map,
          layer: layer,
          job: job,
          valuesByCommune: valuesByCommune,
          inseeLookup: inseeLookup,
          scoreKey: scoreKey,
          preferFullLayerBounds: false,
          franceMetroFixedBounds: frBounds,
          franceMetroFitOpts: frFitOpts,
          fitOpts: frFitOpts,
          franceMassFilteredKeys: franceMassFilteredKeys,
          resizeObserver: null
        };
        S.mapViz.communeMaps = S.mapViz.communeMaps || [];
        S.mapViz.communeMaps.push(massEntry);
        totalMaps += 1;

        var mapEl = document.getElementById(mapId);
        if (mapEl && typeof ResizeObserver !== "undefined") {
          var ro = new ResizeObserver(function () {
            try {
              map.invalidateSize({ animate: false });
              fitFranceMetroView();
            } catch (eRz) {}
          });
          ro.observe(mapEl);
          massEntry.resizeObserver = ro;
        }

        allMaps.push(map);
      });

      invalidateMapsSoon(allMaps, 120);
      if (status) {
        status.textContent =
          totalMaps === 0
            ? "Aucune commune métropolitaine à cartographier pour la sélection actuelle."
            : "France métropolitaine : fond complet des communes des départements concernés ; " +
              "molette pour zoomer ; poignée en bas du cadre pour redimensionner la hauteur. " +
              totalMaps +
              " carte(s).";
      }
    })
    .catch(function (err) {
      if (status) status.textContent = "Erreur carte : " + (err.message || err);
    });
}

function renderCommuneDeptMapsMulti(jobs, rowsByFilterKey, min, max) {
  var status = document.getElementById("comparaison-map-status");
  var singleMap = document.getElementById("comparaison-score-map-single");
  var grid = document.getElementById("comparaison-communes-maps-grid");
  if (!grid) return;
  if (singleMap) singleMap.style.display = "none";
  if (S.mapViz.layer && S.mapViz.map) {
    S.mapViz.map.removeLayer(S.mapViz.layer);
    S.mapViz.layer = null;
  }
  clearCommuneMaps();
  grid.hidden = false;

  if (!jobs || !jobs.length) {
    if (status) status.textContent = "Aucun critère de carte.";
    var h0 = document.getElementById("comparaison-map-commune-legend-hint");
    if (h0) h0.hidden = true;
    return;
  }

  var anyDeptMaps = jobs.some(function (j) {
    var p = partitionRowsByDeptForCommuneMaps(rowsByFilterKey[j.filterKey] || [], j.scoreKey);
    return p.limited.length > 0;
  });
  if (!anyDeptMaps) {
    if (status) status.textContent = "Aucune commune exploitable pour la cartographie.";
    var h1 = document.getElementById("comparaison-map-commune-legend-hint");
    if (h1) h1.hidden = true;
    return;
  }

  ensureCommunesGeoJsonAll()
    .then(function (allGeoJson) {
      if (typeof L === "undefined") throw new Error("Librairie cartographique indisponible.");
      var allMaps = [];
      var fitOpts = {
        padding: [10, 10],
        maxZoom: S.selectedCommunes.length === 1 ? 17 : 15
      };

      jobs.forEach(function (job) {
        var rowsJob = rowsByFilterKey[job.filterKey] || [];
        var part = partitionRowsByDeptForCommuneMaps(rowsJob, job.scoreKey);
        var scoreKey = job.scoreKey;

        var jobWrap = document.createElement("div");
        jobWrap.className = "comparaison-communes-maps-job";
        if (jobs.length > 1) {
          var titleJob = document.createElement("h4");
          titleJob.className = "comparaison-communes-maps-job-title";
          titleJob.textContent = job.groupTitle + " — " + job.cardTitle;
          jobWrap.appendChild(titleJob);
          if (job.criteriaLine) {
            var critP = document.createElement("p");
            critP.className = "comparaison-table-criteria";
            critP.style.marginBottom = "0.5rem";
            critP.textContent = job.criteriaLine;
            jobWrap.appendChild(critP);
          }
        }
        var innerGrid = document.createElement("div");
        innerGrid.className = "comparaison-communes-maps-grid-inner";
        jobWrap.appendChild(innerGrid);
        grid.appendChild(jobWrap);

        part.limited.forEach(function (deptEntry) {
          var rowsForDept = part.byDept[deptEntry.code] || [];

          S.communeMapSeq += 1;
          var mapId = "comparaison-commune-map-" + S.communeMapSeq;
          var deptLabel = deptEntry.code + " " + (S.geo.deptNomByCode[deptEntry.code] || "");
          var card = document.createElement("div");
          card.className = "comparaison-commune-map-card";
          card.innerHTML =
            "<h4>" +
            escapeHtml(deptLabel.trim()) +
            "</h4><div id=\"" +
            escapeHtml(mapId) +
            "\" class=\"comparaison-commune-map\"></div>";
          innerGrid.appendChild(card);

          var valuesByCommune = aggregateCommuneScoresFromRows(rowsForDept);
          var inseeLookup = buildInseeLookupFromPartitionRows(rowsForDept);
          var geojson = buildDeptGeoJsonFromAllCommunes(allGeoJson, deptEntry.code);
          if (!geojson.features || !geojson.features.length) {
            return;
          }
          var map = L.map(mapId, { zoomControl: true, scrollWheelZoom: true, minZoom: 4, maxZoom: 17 });
          attachPreferredBaseLayer(map);
          var layer = L.geoJSON(geojson, {
            style: function (feature) {
              return getCommunePolygonStyle(feature, valuesByCommune, inseeLookup, min, max);
            },
            onEachFeature: function (feature, layerEl) {
              layerEl.bindTooltip(
                communeFeatureTooltipHtml(feature, valuesByCommune, inseeLookup, scoreKey),
                { sticky: true }
              );
            }
          }).addTo(map);
          /* Cadrage sur l’ensemble du département (tous les polygones communaux). */
          scheduleIndependentCommuneMapFit(map, layer, null, fitOpts);
          S.mapViz.communeMaps.push({
            map: map,
            layer: layer,
            mapId: mapId,
            deptCode: deptEntry.code,
            fitOpts: fitOpts,
            valuesByCommune: valuesByCommune,
            inseeLookup: inseeLookup,
            scoreKey: scoreKey,
            preferFullLayerBounds: true
          });
          allMaps.push(map);
        });
      });

      invalidateMapsSoon(allMaps, 120);
      var nj = jobs.length;
      var msg =
        "Par département : toutes les communes du département en fond ; " +
        "les communes du tableau sont colorées selon la légende ; " +
        "cadrage sur le département entier ; zoom / pan indépendants entre cartes. " +
        allMaps.length +
        " carte(s)" +
        (nj > 1 ? " (" + nj + " critères)." : ".");
      if (status) status.textContent = msg;
      S.mapViz.mode = "communes";
      var communeHint = document.getElementById("comparaison-map-commune-legend-hint");
      if (communeHint) communeHint.hidden = false;
    })
    .catch(function (err) {
      var communeHint = document.getElementById("comparaison-map-commune-legend-hint");
      if (communeHint) communeHint.hidden = true;
      if (status) status.textContent = (err && err.message) ? err.message : "Impossible de charger les cartes communales.";
    });
}

function regionNomById(regionId) {
  var rid = String(regionId || "").trim();
  var reg = (S.geo.regions || []).find(function (r) {
    return String(r.id) === rid;
  });
  return reg && reg.nom ? reg.nom : rid;
}

function renderCommuneRegionMapsMulti(jobs, rowsByFilterKey, min, max) {
  var status = document.getElementById("comparaison-map-status");
  var singleMap = document.getElementById("comparaison-score-map-single");
  var grid = document.getElementById("comparaison-communes-maps-grid");
  if (!grid) return;
  if (singleMap) singleMap.style.display = "none";
  if (S.mapViz.layer && S.mapViz.map) {
    S.mapViz.map.removeLayer(S.mapViz.layer);
    S.mapViz.layer = null;
  }
  clearCommuneMaps();
  grid.hidden = false;

  if (!jobs || !jobs.length) {
    if (status) status.textContent = "Aucun critère de carte.";
    var h0 = document.getElementById("comparaison-map-commune-legend-hint");
    if (h0) h0.hidden = true;
    return;
  }

  var anyRegMaps = jobs.some(function (j) {
    var p = partitionRowsByRegionForCommuneMaps(rowsByFilterKey[j.filterKey] || [], j.scoreKey);
    return p.limited.length > 0;
  });
  if (!anyRegMaps) {
    if (status) status.textContent = "Aucune commune exploitable pour la cartographie par région.";
    var h1 = document.getElementById("comparaison-map-commune-legend-hint");
    if (h1) h1.hidden = true;
    return;
  }

  ensureCommunesGeoJsonAll()
    .then(function (allGeoJson) {
      if (typeof L === "undefined") throw new Error("Librairie cartographique indisponible.");
      var allMaps = [];
      var fitOpts = {
        padding: [10, 10],
        maxZoom: S.selectedCommunes.length === 1 ? 17 : 15
      };

      jobs.forEach(function (job) {
        var rowsJob = rowsByFilterKey[job.filterKey] || [];
        var part = partitionRowsByRegionForCommuneMaps(rowsJob, job.scoreKey);
        var scoreKey = job.scoreKey;

        var jobWrap = document.createElement("div");
        jobWrap.className = "comparaison-communes-maps-job";
        if (jobs.length > 1) {
          var titleJob = document.createElement("h4");
          titleJob.className = "comparaison-communes-maps-job-title";
          titleJob.textContent = job.groupTitle + " — " + job.cardTitle;
          jobWrap.appendChild(titleJob);
          if (job.criteriaLine) {
            var critP = document.createElement("p");
            critP.className = "comparaison-table-criteria";
            critP.style.marginBottom = "0.5rem";
            critP.textContent = job.criteriaLine;
            jobWrap.appendChild(critP);
          }
        }
        var innerGrid = document.createElement("div");
        innerGrid.className = "comparaison-communes-maps-grid-inner";
        jobWrap.appendChild(innerGrid);
        grid.appendChild(jobWrap);

        part.limited.forEach(function (regEntry) {
          var rowsForReg = part.byRegion[regEntry.code] || [];

          S.communeMapSeq += 1;
          var mapId = "comparaison-commune-map-" + S.communeMapSeq;
          var regLabel = regEntry.code + " " + regionNomById(regEntry.code);
          var card = document.createElement("div");
          card.className = "comparaison-commune-map-card";
          card.innerHTML =
            "<h4>" +
            escapeHtml(regLabel.trim()) +
            "</h4><div id=\"" +
            escapeHtml(mapId) +
            "\" class=\"comparaison-commune-map\"></div>";
          innerGrid.appendChild(card);

          var valuesByCommune = aggregateCommuneScoresFromRows(rowsForReg);
          var inseeLookup = buildInseeLookupFromPartitionRows(rowsForReg);
          var geojson = buildRegionGeoJsonFromAllCommunes(allGeoJson, regEntry.code);
          if (!geojson.features || !geojson.features.length) {
            return;
          }
          var map = L.map(mapId, { zoomControl: true, scrollWheelZoom: true, minZoom: 4, maxZoom: 17 });
          attachPreferredBaseLayer(map);
          var layer = L.geoJSON(geojson, {
            style: function (feature) {
              return getCommunePolygonStyle(feature, valuesByCommune, inseeLookup, min, max);
            },
            onEachFeature: function (feature, layerEl) {
              layerEl.bindTooltip(
                communeFeatureTooltipHtml(feature, valuesByCommune, inseeLookup, scoreKey),
                { sticky: true }
              );
            }
          }).addTo(map);
          scheduleIndependentCommuneMapFit(map, layer, null, fitOpts);
          S.mapViz.communeMaps.push({
            map: map,
            layer: layer,
            mapId: mapId,
            regionCode: regEntry.code,
            fitOpts: fitOpts,
            valuesByCommune: valuesByCommune,
            inseeLookup: inseeLookup,
            scoreKey: scoreKey,
            preferFullLayerBounds: true
          });
          allMaps.push(map);
        });
      });

      invalidateMapsSoon(allMaps, 120);
      var nj = jobs.length;
      var msg =
        "Par région : toutes les communes de la région en fond ; " +
        "les communes du tableau sont colorées selon la légende ; " +
        "cadrage sur la région entière ; zoom / pan indépendants entre cartes. " +
        allMaps.length +
        " carte(s)" +
        (nj > 1 ? " (" + nj + " critères)." : ".");
      if (status) status.textContent = msg;
      S.mapViz.mode = "communes";
      var communeHint = document.getElementById("comparaison-map-commune-legend-hint");
      if (communeHint) communeHint.hidden = false;
    })
    .catch(function (err) {
      var communeHint = document.getElementById("comparaison-map-commune-legend-hint");
      if (communeHint) communeHint.hidden = true;
      if (status) status.textContent = (err && err.message) ? err.message : "Impossible de charger les cartes communales.";
    });
}

export function renderComparaisonMap(jobs, rowsByFilterKey, displayMode, scorePrincipal, opts) {
  opts = opts || {};
  if (opts.preserveLegendBounds === undefined) opts.preserveLegendBounds = true;
  var mapWrap = document.getElementById("comparaison-map-wrap");
  var status = document.getElementById("comparaison-map-status");
  var legend = document.getElementById("comparaison-map-legend");
  var minEl = document.getElementById("comparaison-map-legend-min");
  var maxEl = document.getElementById("comparaison-map-legend-max");
  var titleEl = document.getElementById("comparaison-map-main-title");
  var mapsZone = document.getElementById("comparaison-maps-zone");
  if (!mapWrap || !status || !legend || !minEl || !maxEl) return;

  if (!jobs || !jobs.length) {
    clearComparaisonMap();
    return;
  }
  var hasAnyRows = jobs.some(function (j) {
    return (rowsByFilterKey[j.filterKey] || []).length > 0;
  });
  if (!hasAnyRows) {
    clearComparaisonMap();
    return;
  }
  var rowsForMaps =
    displayMode === "communes"
      ? buildFilteredRowsByFilterKeyForMaps(rowsByFilterKey, jobs, displayMode)
      : rowsByFilterKey;
  var mapJobs = filterJobsForMapRendering(jobs, rowsForMaps);
  if (!mapJobs.length) {
    clearComparaisonMap();
    if (status) {
      status.textContent =
        "Aucune carte à afficher : pour chaque combinaison (type de local, surface, pièces), des valeurs sont nécessaires ; les panneaux sans donnée pour ce score sont masqués.";
    }
    return;
  }
  resetComparaisonMapVisuals();
  mapWrap.removeAttribute("aria-hidden");
  if (status) status.textContent = "Chargement des cartes…";
  if (titleEl) titleEl.textContent = "Cartes — " + getScoreLabel(scorePrincipal);

  var allValues = [];
  mapJobs.forEach(function (job) {
    var rows = rowsForMaps[job.filterKey] || [];
    rows.forEach(function (r) {
      var v = numericScoreFromRow(r, job.scoreKey);
      if (Number.isFinite(v)) allValues.push(v);
    });
  });
  if (!allValues.length) {
    clearComparaisonMap();
    if (status) {
      status.textContent =
        "Aucune valeur exploitable pour afficher une échelle de score pour les colonnes demandées.";
    }
    return;
  }
  var min = Math.min.apply(null, allValues);
  var max = Math.max.apply(null, allValues);
  if (!opts.preserveLegendBounds) {
    S.legendCustomMin = null;
    S.legendCustomMax = null;
  }
  var inMinData = document.getElementById("legend-input-min");
  var inMaxData = document.getElementById("legend-input-max");
  if (inMinData) inMinData._rawMin = min;
  if (inMaxData) inMaxData._rawMax = max;
  legend.hidden = false;
  updateLegendGradient(min, max);
  initLegendControls(min, max);
  var loDisp = S.legendCustomMin !== null ? S.legendCustomMin : min;
  var hiDisp = S.legendCustomMax !== null ? S.legendCustomMax : max;

  var rowsMerged = mergeRowsForBounds(rowsByFilterKey, mapJobs);

  if (displayMode === "communes") {
    if (mapsZone) mapsZone.innerHTML = "";
    if (titleEl) {
      titleEl.textContent =
        mapJobs.length > 1
          ? "Cartes — " + getScoreLabel(scorePrincipal) + " (échelle commune à tous les panneaux)"
          : "Cartes — " + getScoreLabel(scorePrincipal);
    }
    var communeHintPre = document.getElementById("comparaison-map-commune-legend-hint");
    if (communeHintPre) communeHintPre.hidden = true;
    var subTabsEl = document.getElementById("comparaison-commune-map-subtabs");
    if (subTabsEl) subTabsEl.hidden = false;
    S.mapViz.lastCommuneMapScorePrincipal = scorePrincipal;
    var subTab = getCommuneMapSubTab();
    syncCommuneMapSubtabButtons(subTab);
    if (subTab === "dept") {
      renderCommuneDeptMapsMulti(mapJobs, rowsForMaps, loDisp, hiDisp);
    } else if (subTab === "region") {
      renderCommuneRegionMapsMulti(mapJobs, rowsForMaps, loDisp, hiDisp);
    } else {
      renderCommunesMassMap(mapJobs, rowsForMaps, loDisp, hiDisp);
    }
    S.mapViz.mode = "communes";
    return;
  }

  var subTabsHide = document.getElementById("comparaison-commune-map-subtabs");
  if (subTabsHide) subTabsHide.hidden = true;
  var communeHintOff = document.getElementById("comparaison-map-commune-legend-hint");
  if (communeHintOff) communeHintOff.hidden = true;
  // Trier les rows de chaque job par code (région ou département) avant rendu
  mapJobs.forEach(function (job) {
    var rows = rowsByFilterKey[job.filterKey];
    if (!rows) return;
    rows.sort(function (a, b) {
      var ca = displayMode === "regions" ? String(a.code_region || "") : String(a.code_dept || "");
      var cb = displayMode === "regions" ? String(b.code_region || "") : String(b.code_dept || "");
      var na = parseInt(ca, 10), nb = parseInt(cb, 10);
      if (!isNaN(na) && !isNaN(nb)) return na - nb;
      return ca.localeCompare(cb, "fr");
    });
  });
  var entries = mapJobs.map(function (job) {
    return { job: job, mapId: "comparaison-choro-" + S.choroMapSeq++ };
  });
  var groupOrder = [];
  var byGroup = {};
  entries.forEach(function (e) {
    var g = e.job.groupTitle || "Cartes";
    if (!byGroup[g]) {
      byGroup[g] = [];
      groupOrder.push(g);
    }
    byGroup[g].push(e);
  });
  var htmlParts = [];
  groupOrder.forEach(function (gTitle) {
    htmlParts.push(
      '<div class="comparaison-map-group"><h4 class="comparaison-map-group-title">' +
        escapeHtml(gTitle) +
        '</h4><div class="comparaison-map-cards-grid">'
    );
    byGroup[gTitle].forEach(function (e) {
      var j = e.job;
      htmlParts.push(
        '<article class="comparaison-map-card-choro"><div class="card-head"><strong>' +
          escapeHtml(j.groupTitle) +
          "</strong> — " +
          escapeHtml(j.cardTitle) +
          "</div>" +
          (j.criteriaLine ? '<div class="card-legend">' + escapeHtml(j.criteriaLine) + "</div>" : "") +
          '<div id="' +
          escapeHtml(e.mapId) +
          '" class="comparaison-map-instance"></div>' +
          (j.footnote ? '<p class="card-footnote">' + escapeHtml(j.footnote) + "</p>" : "") +
          "</article>"
      );
    });
    htmlParts.push("</div></div>");
  });
  if (mapsZone) mapsZone.innerHTML = htmlParts.join("");

  var renderPromises = entries.map(function (e) {
    var rows = rowsByFilterKey[e.job.filterKey] || [];
    return renderChoroplethIntoDiv(e.mapId, rows, displayMode, e.job.scoreKey, min, max);
  });
  Promise.all(renderPromises)
    .then(function () {
      return ensureGeoJson(displayMode);
    })
    .then(function (gj) {
      var maps = (S.mapViz.multiChoroMaps || []).map(function (x) {
        return x.map;
      });
      var b = boundsForChoroplethSelection(gj, displayMode, rowsMerged);
      if (!b || !b.isValid()) {
        try {
          var first = S.mapViz.multiChoroMaps[0];
          if (first && first.layer && typeof first.layer.getBounds === "function") {
            b = first.layer.getBounds();
          }
        } catch (e0) {}
      }
      var fitOpts = { padding: [12, 12], maxZoom: 11 };
      if (b && b.isValid()) {
        applySameBoundsToMaps(maps, b, fitOpts);
        // Stocker bounds+fitOpts dans chaque entrée pour showMaps
        (S.mapViz.multiChoroMaps || []).forEach(function (entry) {
          entry.savedBounds = b;
          entry.fitOpts = fitOpts;
        });
      }
      bindLinkedLeafletMaps(maps);
      invalidateMapsSoon(maps, 120);
      status.textContent =
        displayMode === "regions"
          ? "Carte(s) par région — même cadrage, zoom et déplacement synchronisés entre les vues."
          : "Carte(s) par département — même cadrage, zoom et déplacement synchronisés entre les vues.";
      S.mapViz.mode = displayMode;
    })
    .catch(function (err) {
      status.textContent = (err && err.message) ? err.message : "Impossible de charger les cartes.";
    });
}
