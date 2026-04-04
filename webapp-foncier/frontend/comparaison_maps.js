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
  normalizeCodeInseeFr,
  jobHasMapDisplayableData,
  getEffectiveMapScoreKeyForJob,
  parseDistanceFilterInputs
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

export function filterJobsForMapRendering(jobs, rowsByFilterKey, cat, displayIndicators) {
  return (jobs || []).filter(function (j) {
    return jobHasMapDisplayableData(rowsByFilterKey[j.filterKey] || [], j, cat, displayIndicators);
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

function ensureCommuneViewsBySubTabStore() {
  if (!S.mapViz.communeViewsBySubTab) {
    S.mapViz.communeViewsBySubTab = { dept: null, region: null, france: null };
  }
}

/** Sauvegarde le centre / zoom courants des cartes communales pour un sous-onglet (avant changement d’onglet ou re-rendu). */
export function saveCommuneMapViewsForSubTab(subTab) {
  ensureCommuneViewsBySubTabStore();
  if (subTab !== "dept" && subTab !== "region" && subTab !== "france") return;
  if (!S.mapViz.communeMaps || !S.mapViz.communeMaps.length) return;
  S.mapViz.communeViewsBySubTab[subTab] = S.mapViz.communeMaps.map(function (entry) {
    if (!entry || !entry.map) return null;
    try {
      var c = entry.map.getCenter();
      return { lat: c.lat, lng: c.lng, zoom: entry.map.getZoom() };
    } catch (e) {
      return null;
    }
  });
}

function restoreCommuneMapViewsForSubTab(subTab) {
  ensureCommuneViewsBySubTabStore();
  var arr = S.mapViz.communeViewsBySubTab[subTab];
  if (!arr || !S.mapViz.communeMaps || !S.mapViz.communeMaps.length) return;
  for (var i = 0; i < Math.min(arr.length, S.mapViz.communeMaps.length); i++) {
    var s = arr[i];
    var e = S.mapViz.communeMaps[i];
    if (!s || !e || !e.map) continue;
    try {
      e.map.setView([s.lat, s.lng], s.zoom, { animate: false });
    } catch (err) {}
  }
}

function scheduleRestoreCommuneMapViews(subTab) {
  var tab = subTab || getCommuneMapSubTab();
  setTimeout(function () {
    restoreCommuneMapViewsForSubTab(tab);
  }, 280);
}

/** True si on peut réappliquer les vues sauvegardées sans refaire de fitBounds (même nombre de cartes). */
function savedCommuneSubTabViewsMatch(subTab, nMaps) {
  ensureCommuneViewsBySubTabStore();
  var arr = S.mapViz.communeViewsBySubTab[subTab];
  if (!arr || !Array.isArray(arr) || arr.length !== nMaps || nMaps <= 0) return false;
  return arr.every(function (s) {
    return s && Number.isFinite(s.lat) && Number.isFinite(s.lng) && Number.isFinite(s.zoom);
  });
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
  var args = S.lastRenderTableArgs;
  renderComparaisonMap(S.lastComparaisonJobs, S.lastRowsByFilterKey, "communes", sp, {
    preserveLegendBounds: true,
    cat: args ? args.cat : undefined,
    displayIndicators: args ? args.displayIndicators : undefined
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
  // crossOrigin: "anonymous" nécessaire pour pouvoir dessiner les tuiles dans un canvas (export JPG).
  return L.tileLayer(
    (S.API_BASE || "") + "/api/ign-tiles/{z}/{x}/{y}.png",
    {
      maxZoom: 19,
      tileSize: 256,
      crossOrigin: "anonymous",
      attribution: '&copy; <a href="https://www.ign.fr/" target="_blank" rel="noopener noreferrer">IGN</a> (cache local)'
    }
  );
}

function createOsmFallbackLayer() {
  // crossOrigin: "anonymous" — OSM envoie Access-Control-Allow-Origin: *, requis pour canvas.
  return L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    crossOrigin: "anonymous",
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

function markPendingLayoutRefitIfMapsPanelHidden() {
  var panelMaps = document.getElementById("comparaison-panel-maps");
  if (panelMaps && panelMaps.hidden) {
    S.mapViz.pendingCommuneMapLayoutRefit = true;
  }
}

/**
 * Recadre les cartes communales sur les données (dépt / région / France) après affichage du panneau
 * si le rendu avait eu lieu alors que le panneau était masqué (Leaflet : taille 0 → mauvais zoom).
 * @param {boolean} [clearPending] — si true, efface le drapeau après ce passage
 */
export function applyPendingCommuneMapLayoutRefit(clearPending) {
  if (!S.mapViz.pendingCommuneMapLayoutRefit) return;
  (S.mapViz.communeMaps || []).forEach(function (entry) {
    if (!entry || !entry.map) return;
    try {
      entry.map.invalidateSize({ animate: false });
      if (entry.preferFullLayerBounds && entry.layer && entry.layer.getBounds && entry.layer.getBounds().isValid()) {
        entry.map.fitBounds(entry.layer.getBounds(), entry.fitOpts || { padding: [10, 10], maxZoom: 15 });
      } else if (
        entry.franceMetroFixedBounds &&
        entry.franceMetroFixedBounds.isValid &&
        entry.franceMetroFixedBounds.isValid()
      ) {
        entry.map.fitBounds(entry.franceMetroFixedBounds, entry.franceMetroFitOpts || { padding: [12, 12], maxZoom: 6 });
      }
    } catch (e) {}
  });
  if (clearPending) {
    S.mapViz.pendingCommuneMapLayoutRefit = false;
  }
}

export function clearCommuneMaps() {
  S.mapViz.pendingCommuneMapLayoutRefit = false;
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
  var filterSummaryEl = document.getElementById("comparaison-map-filter-summary");
  var exportBtn = document.getElementById("btn-export-map-jpg");
  if (mapWrap) mapWrap.setAttribute("aria-hidden", "true");
  if (legend) legend.hidden = true;
  if (communeHint) communeHint.hidden = true;
  if (subTabs) subTabs.hidden = true;
  if (filterSummaryEl) filterSummaryEl.hidden = true;
  if (exportBtn) exportBtn.hidden = true;
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

function classifyCommuneFeatureForStyle(feature, valuesByCommune, inseeLookup, filteredKeys) {
  if (filteredKeys && !communePolygonInFilteredTableKeys(feature, filteredKeys)) {
    return { kind: "out_of_filter", score: null };
  }
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

function getCommunePolygonStyle(feature, valuesByCommune, inseeLookup, min, max, filteredKeys) {
  var c = classifyCommuneFeatureForStyle(feature, valuesByCommune, inseeLookup, filteredKeys);
  if (c.kind === "out_of_filter") {
    return { color: "#fca5a5", weight: 0.28, fillColor: "#fecaca", fillOpacity: 0.52 };
  }
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
  return getCommunePolygonStyle(feature, valuesByCommune, inseeLookup, min, max, filteredKeys);
}

function communeFeatureTooltipHtml(feature, valuesByCommune, inseeLookup, scoreKey, filteredKeys) {
  var props = (feature && feature.properties) || {};
  var nom = props.nom || props.name || "Commune";
  if (filteredKeys && !communePolygonInFilteredTableKeys(feature, filteredKeys)) {
    return (
      "<strong>" +
      escapeHtml(nom) +
      "</strong><br/>" +
      escapeHtml(getScoreLabel(scoreKey)) +
      " : hors périmètre du tableau filtré"
    );
  }
  var cl = classifyCommuneFeatureForStyle(feature, valuesByCommune, inseeLookup, filteredKeys);
  var label;
  if (cl.kind === "neutral") label = "Sans agrégation pour cette vue";
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
    var fk = entry.franceMassFilteredKeys || entry.tableFilteredKeys || null;
    entry.layer.setStyle(function (feature) {
      return getCommunePolygonStyle(
        feature,
        entry.valuesByCommune,
        entry.inseeLookup || null,
        lo,
        hi,
        fk
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

/** Poignée sur le bord bas pour redimensionner la hauteur (carte unique). */
function attachCommuneMapBottomResize(mapEl, map) {
  if (!mapEl || !map || mapEl._comparaisonResizeBound) return;
  mapEl._comparaisonResizeBound = true;
  var strip = document.createElement("div");
  strip.className = "comparaison-map-resize-handle";
  strip.setAttribute("role", "separator");
  strip.setAttribute("aria-orientation", "horizontal");
  strip.setAttribute("aria-label", "Redimensionner la hauteur de la carte");
  var card = mapEl.closest(".comparaison-commune-map-card");
  if (card) card.classList.add("comparaison-commune-map-card--with-resize-handle");
  mapEl.parentNode.insertBefore(strip, mapEl.nextSibling);
  strip.addEventListener("mousedown", function (e) {
    if (e.button !== 0) return;
    e.preventDefault();
    var startY = e.clientY;
    var startH = mapEl.getBoundingClientRect().height;
    var minH = 200;
    var maxH = Math.min(window.innerHeight * 0.92, 1200);
    function move(ev) {
      var dy = ev.clientY - startY;
      var nh = Math.max(minH, Math.min(maxH, startH + dy));
      mapEl.style.height = nh + "px";
      try {
        map.invalidateSize({ animate: false });
      } catch (eInv) {}
    }
    function up() {
      document.removeEventListener("mousemove", move);
      document.removeEventListener("mouseup", up);
    }
    document.addEventListener("mousemove", move);
    document.addEventListener("mouseup", up);
  });
}

/** Une seule carte communale : hauteur ajustable par la poignée basse (pas le coin natif). */
function maybeAttachSingleCommuneMapResize() {
  var arr = S.mapViz.communeMaps || [];
  if (arr.length !== 1) return;
  var ent = arr[0];
  if (ent.resizeObserver && typeof ent.resizeObserver.disconnect === "function") {
    try {
      ent.resizeObserver.disconnect();
    } catch (e0) {}
    ent.resizeObserver = null;
  }
  var map = ent.map;
  var mel = map && map.getContainer();
  if (!mel) return;
  mel.classList.add("comparaison-commune-map--single-resize");
  mel.classList.remove("comparaison-commune-map--france-met");
  attachCommuneMapBottomResize(mel, map);
  if (typeof ResizeObserver !== "undefined") {
    var ro = new ResizeObserver(function () {
      try {
        map.invalidateSize({ animate: false });
      } catch (eR) {}
    });
    ro.observe(mel);
    ent.resizeObserver = ro;
  }
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
function scheduleIndependentCommuneMapFit(map, layer, selBounds, fitOpts, doFit) {
  if (doFit === false) {
    function invOnly() {
      try {
        if (map && typeof map.invalidateSize === "function") {
          map.invalidateSize({ animate: false });
        }
      } catch (e) {}
    }
    requestAnimationFrame(function () {
      requestAnimationFrame(invOnly);
    });
    setTimeout(invOnly, 80);
    setTimeout(invOnly, 200);
    return;
  }
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
  setTimeout(applyFit, 1000);
  setTimeout(applyFit, 1650);
  var container = map && map.getContainer && map.getContainer();
  if (container && typeof ResizeObserver !== "undefined") {
    var roT = null;
    var ro = new ResizeObserver(function () {
      var r = container.getBoundingClientRect();
      if (r.width < 32 || r.height < 32) return;
      if (roT) clearTimeout(roT);
      roT = setTimeout(function () {
        roT = null;
        applyFit();
      }, 120);
    });
    ro.observe(container);
    setTimeout(function () {
      try {
        ro.disconnect();
      } catch (e1) {}
    }, 6000);
  }
  if (container && typeof IntersectionObserver !== "undefined") {
    var ioDone = false;
    var io = new IntersectionObserver(
      function (entries) {
        for (var i = 0; i < entries.length; i++) {
          if (entries[i].isIntersecting && entries[i].intersectionRatio > 0) {
            if (ioDone) return;
            ioDone = true;
            try {
              io.disconnect();
            } catch (eIo) {}
            requestAnimationFrame(function () {
              requestAnimationFrame(applyFit);
            });
            return;
          }
        }
      },
      { root: null, rootMargin: "0px", threshold: [0, 0.01, 0.05] }
    );
    io.observe(container);
    setTimeout(function () {
      try {
        io.disconnect();
      } catch (eIo2) {}
    }, 12000);
  }
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
      var franceFitEntries = [];

      jobs.forEach(function (job) {
        var rows = rowsByFilterKey[job.filterKey] || [];
        var scoreKey = job.mapScoreKey || job.scoreKey;
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
              communeFeatureTooltipHtml(feature, valuesByCommune, inseeLookup, scoreKey, franceMassFilteredKeys),
              { sticky: true }
            );
          }
        });

        layer.addTo(map);

        var frBounds = getFranceMetroLeafletBounds();
        var frFitOpts = { padding: [12, 12], maxZoom: 6 };
        franceFitEntries.push({ map: map, frBounds: frBounds, frFitOpts: frFitOpts, mapId: mapId });

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

        allMaps.push(map);
      });

      var restoreFrance = savedCommuneSubTabViewsMatch("france", totalMaps);
      franceFitEntries.forEach(function (ent, idx) {
        var map = ent.map;
        var frBounds = ent.frBounds;
        var frFitOpts = ent.frFitOpts;
        var massEntry = S.mapViz.communeMaps[idx];
        function fitFranceMetroView() {
          if (restoreFrance) return;
          try {
            map.invalidateSize({ animate: false });
            if (frBounds && frBounds.isValid && frBounds.isValid()) {
              map.fitBounds(frBounds, frFitOpts);
            }
          } catch (eF) {}
        }
        function invOnly() {
          try {
            map.invalidateSize({ animate: false });
          } catch (e) {}
        }
        if (restoreFrance) {
          requestAnimationFrame(function () {
            requestAnimationFrame(invOnly);
          });
          setTimeout(invOnly, 80);
          setTimeout(invOnly, 200);
        } else {
          requestAnimationFrame(function () {
            requestAnimationFrame(fitFranceMetroView);
          });
          setTimeout(fitFranceMetroView, 80);
          setTimeout(fitFranceMetroView, 250);
          setTimeout(fitFranceMetroView, 500);
          var container = map.getContainer && map.getContainer();
          if (container && typeof ResizeObserver !== "undefined") {
            var roDone = false;
            var roFit = new ResizeObserver(function () {
              if (roDone) return;
              var r = container.getBoundingClientRect();
              if (r.width < 40 || r.height < 40) return;
              roDone = true;
              try {
                roFit.disconnect();
              } catch (e0) {}
              fitFranceMetroView();
            });
            roFit.observe(container);
            setTimeout(function () {
              try {
                roFit.disconnect();
              } catch (e1) {}
            }, 5000);
          }
        }
        var mapEl = document.getElementById(ent.mapId);
        if (mapEl && typeof ResizeObserver !== "undefined") {
          var ro = new ResizeObserver(function () {
            try {
              map.invalidateSize({ animate: false });
              if (!restoreFrance) fitFranceMetroView();
            } catch (eRz) {}
          });
          ro.observe(mapEl);
          if (massEntry) massEntry.resizeObserver = ro;
        }
      });

      if (!restoreFrance) {
        markPendingLayoutRefitIfMapsPanelHidden();
      }
      invalidateMapsSoon(allMaps, 120);
      maybeAttachSingleCommuneMapResize();
      scheduleRestoreCommuneMapViews("france");
      var communeHintFr = document.getElementById("comparaison-map-commune-legend-hint");
      if (communeHintFr) communeHintFr.hidden = totalMaps === 0;
      if (status) {
        status.textContent =
          totalMaps === 0
            ? "Aucune commune métropolitaine à cartographier pour la sélection actuelle."
            : "France métropolitaine : fond complet des communes des départements concernés ; " +
              "communes hors tableau filtré en rouge clair ; molette pour zoomer ; " +
              (totalMaps === 1
                ? "poignée horizontale sous la carte pour redimensionner la hauteur. "
                : "redimensionnement vertical du cadre de chaque carte (poignée du navigateur). ") +
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
    var p = partitionRowsByDeptForCommuneMaps(rowsByFilterKey[j.filterKey] || [], j.mapScoreKey || j.scoreKey);
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
      var fitTasks = [];

      jobs.forEach(function (job) {
        var rowsJob = rowsByFilterKey[job.filterKey] || [];
        var part = partitionRowsByDeptForCommuneMaps(rowsJob, job.mapScoreKey || job.scoreKey);
        var scoreKey = job.mapScoreKey || job.scoreKey;

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

        var tableFilteredKeys = buildFilteredCommuneTableKeys(
          rowsJob.map(function (r) {
            return {
              code_insee: r.code_insee,
              commune: r.commune || r.nom_commune,
              nom_commune: r.nom_commune
            };
          })
        );

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
              return getCommunePolygonStyle(feature, valuesByCommune, inseeLookup, min, max, tableFilteredKeys);
            },
            onEachFeature: function (feature, layerEl) {
              layerEl.bindTooltip(
                communeFeatureTooltipHtml(feature, valuesByCommune, inseeLookup, scoreKey, tableFilteredKeys),
                { sticky: true }
              );
            }
          }).addTo(map);
          /* Cadrage différé : si vues sauvegardées (sous-onglet), pas de fitBounds après restore. */
          fitTasks.push({ map: map, layer: layer, fitOpts: fitOpts });
          S.mapViz.communeMaps.push({
            map: map,
            layer: layer,
            mapId: mapId,
            deptCode: deptEntry.code,
            fitOpts: fitOpts,
            valuesByCommune: valuesByCommune,
            inseeLookup: inseeLookup,
            scoreKey: scoreKey,
            preferFullLayerBounds: true,
            tableFilteredKeys: tableFilteredKeys
          });
          allMaps.push(map);
        });
      });

      var restoreDept = savedCommuneSubTabViewsMatch("dept", S.mapViz.communeMaps.length);
      fitTasks.forEach(function (t) {
        scheduleIndependentCommuneMapFit(t.map, t.layer, null, t.fitOpts, !restoreDept);
      });

      if (!restoreDept) {
        markPendingLayoutRefitIfMapsPanelHidden();
      }
      invalidateMapsSoon(allMaps, 120);
      maybeAttachSingleCommuneMapResize();
      scheduleRestoreCommuneMapViews("dept");
      var nj = jobs.length;
      var msg =
        "Par département : toutes les communes du département en fond ; " +
        "les communes du tableau filtré sont colorées selon la légende ; les autres en rouge clair ; " +
        "cadrage sur le département entier ; zoom / pan indépendants entre cartes. " +
        (allMaps.length === 1 ? "Poignée sous la carte pour ajuster la hauteur. " : "") +
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
    var p = partitionRowsByRegionForCommuneMaps(rowsByFilterKey[j.filterKey] || [], j.mapScoreKey || j.scoreKey);
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
      var fitTasksReg = [];

      jobs.forEach(function (job) {
        var rowsJob = rowsByFilterKey[job.filterKey] || [];
        var part = partitionRowsByRegionForCommuneMaps(rowsJob, job.mapScoreKey || job.scoreKey);
        var scoreKey = job.mapScoreKey || job.scoreKey;

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

        var tableFilteredKeys = buildFilteredCommuneTableKeys(
          rowsJob.map(function (r) {
            return {
              code_insee: r.code_insee,
              commune: r.commune || r.nom_commune,
              nom_commune: r.nom_commune
            };
          })
        );

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
              return getCommunePolygonStyle(feature, valuesByCommune, inseeLookup, min, max, tableFilteredKeys);
            },
            onEachFeature: function (feature, layerEl) {
              layerEl.bindTooltip(
                communeFeatureTooltipHtml(feature, valuesByCommune, inseeLookup, scoreKey, tableFilteredKeys),
                { sticky: true }
              );
            }
          }).addTo(map);
          fitTasksReg.push({ map: map, layer: layer, fitOpts: fitOpts });
          S.mapViz.communeMaps.push({
            map: map,
            layer: layer,
            mapId: mapId,
            regionCode: regEntry.code,
            fitOpts: fitOpts,
            valuesByCommune: valuesByCommune,
            inseeLookup: inseeLookup,
            scoreKey: scoreKey,
            preferFullLayerBounds: true,
            tableFilteredKeys: tableFilteredKeys
          });
          allMaps.push(map);
        });
      });

      var restoreRegion = savedCommuneSubTabViewsMatch("region", S.mapViz.communeMaps.length);
      fitTasksReg.forEach(function (t) {
        scheduleIndependentCommuneMapFit(t.map, t.layer, null, t.fitOpts, !restoreRegion);
      });

      if (!restoreRegion) {
        markPendingLayoutRefitIfMapsPanelHidden();
      }
      invalidateMapsSoon(allMaps, 120);
      maybeAttachSingleCommuneMapResize();
      scheduleRestoreCommuneMapViews("region");
      var nj = jobs.length;
      var msg =
        "Par région : toutes les communes de la région en fond ; " +
        "les communes du tableau filtré sont colorées selon la légende ; les autres en rouge clair ; " +
        "cadrage sur la région entière ; zoom / pan indépendants entre cartes. " +
        (allMaps.length === 1 ? "Poignée sous la carte pour ajuster la hauteur. " : "") +
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

function _parseLeafletTx(styleTransform) {
  if (!styleTransform) return [0, 0];
  var m = styleTransform.match(/translate3d\(\s*([-\d.]+)px\s*,\s*([-\d.]+)px/);
  if (!m) m = styleTransform.match(/translate\(\s*([-\d.]+)px\s*,\s*([-\d.]+)px/);
  return m ? [parseFloat(m[1]) || 0, parseFloat(m[2]) || 0] : [0, 0];
}

/**
 * Capture une carte Leaflet en canvas :
 *  1. Tuiles de fond (même origine → drawImage direct, pas de CORS)
 *  2. Overlay SVG polygones (viewBox recalculé depuis les transforms cumulées)
 */
function captureLeafletMapFull(leafletContainer) {
  return new Promise(function (resolve) {
    /* getBoundingClientRect est plus fiable que clientWidth pour les éléments
       avec des tailles CSS complexes (ex. France --france-met h:520px) */
    var rect = leafletContainer.getBoundingClientRect();
    var w = Math.round(rect.width) || leafletContainer.offsetWidth;
    var h = Math.round(rect.height) || leafletContainer.offsetHeight;
    if (!w || !h) { resolve(null); return; }

    var SCALE = 2;
    var canvas = document.createElement("canvas");
    canvas.width = w * SCALE;
    canvas.height = h * SCALE;
    var ctx = canvas.getContext("2d");
    ctx.scale(SCALE, SCALE);
    ctx.fillStyle = "#f8fafc";
    ctx.fillRect(0, 0, w, h);

    // Transforms cumulés : mapPane > overlayPane > svg
    // Référence de position : coin supérieur gauche du conteneur Leaflet à l'écran.
    // getBoundingClientRect() intègre TOUS les transforms CSS de la hiérarchie DOM,
    // éliminant le besoin de recalculer mp / tp / sp à la main.
    var cLeft = rect.left;
    var cTop  = rect.top;

    // 1. Tuiles de fond (crossOrigin:"anonymous" positionné sur les couches IGN et OSM)
    ctx.save();
    ctx.beginPath();
    ctx.rect(0, 0, w, h);
    ctx.clip();
    var tilePane = leafletContainer.querySelector(".leaflet-tile-pane");
    if (tilePane) {
      tilePane.querySelectorAll("img.leaflet-tile, img.leaflet-tile-loaded").forEach(function (tile) {
        if (!tile.complete || !tile.naturalWidth) return;
        var tr = tile.getBoundingClientRect();
        var tx = tr.left - cLeft;
        var ty = tr.top  - cTop;
        var tw = tr.width  || tile.naturalWidth  || 256;
        var th = tr.height || tile.naturalHeight || 256;
        try { ctx.drawImage(tile, tx, ty, tw, th); } catch (e) {}
      });
    }
    ctx.restore();

    // 2. Overlay SVG (polygones)
    // Utiliser getBoundingClientRect() sur le SVG lui-même pour obtenir sa position/taille réelles,
    // puis conserver le viewBox natif de Leaflet pour le mappage coordonnées→pixels.
    var svg = leafletContainer.querySelector(".leaflet-overlay-pane svg");
    if (!svg) { resolve(canvas); return; }

    var svgSR = svg.getBoundingClientRect();
    var svgX  = svgSR.left - cLeft;
    var svgY  = svgSR.top  - cTop;
    var svgW  = svgSR.width  || w;
    var svgH  = svgSR.height || h;

    var svgClone = svg.cloneNode(true);
    svgClone.style.cssText = "";
    svgClone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    // viewBox, width et height natifs de Leaflet conservés intacts.

    var svgStr = new XMLSerializer().serializeToString(svgClone);
    var url = URL.createObjectURL(new Blob([svgStr], { type: "image/svg+xml;charset=utf-8" }));
    var img = new Image();
    img.onload = function () {
      ctx.save();
      ctx.beginPath();
      ctx.rect(0, 0, w, h);
      ctx.clip();
      ctx.drawImage(img, svgX, svgY, svgW, svgH);
      ctx.restore();
      URL.revokeObjectURL(url);
      resolve(canvas);
    };
    img.onerror = function () { URL.revokeObjectURL(url); resolve(canvas); };
    img.src = url;
  });
}

function _composeMapExportCanvas(captured, titleText, filterSummaryText, legendMinText, legendMaxText, hasLegend, hasHint) {
  var SCALE = 2;
  var MARGIN = 24;
  var LABEL_H = 18;
  var nMaps = captured.length;
  var COLS = nMaps <= 1 ? 1 : nMaps <= 4 ? 2 : 3;
  var ROWS = Math.ceil(nMaps / COLS);
  var mapW = Math.round(captured[0].canvas.width / SCALE);
  var mapH = Math.round(captured[0].canvas.height / SCALE);
  var contentW = COLS * mapW + (COLS - 1) * MARGIN;
  var totalW = contentW + 2 * MARGIN;
  var headerH = MARGIN + 20 + (filterSummaryText ? 18 : 0) + MARGIN * 0.5;
  var gridH = ROWS * (LABEL_H + mapH) + Math.max(0, ROWS - 1) * MARGIN;
  var legendH = hasLegend ? (hasHint ? 92 : 60) : 0;
  var totalH = Math.ceil(headerH + gridH + (legendH ? MARGIN + legendH : 0) + MARGIN);

  var canvas = document.createElement("canvas");
  canvas.width = totalW * SCALE;
  canvas.height = totalH * SCALE;
  var ctx = canvas.getContext("2d");
  ctx.scale(SCALE, SCALE);
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, totalW, totalH);

  // Titre
  var y = MARGIN + 16;
  ctx.font = "bold 16px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
  ctx.fillStyle = "#1e293b";
  ctx.fillText(titleText, MARGIN, y);

  if (filterSummaryText) {
    y += 18;
    ctx.font = "11px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
    ctx.fillStyle = "#64748b";
    ctx.fillText(filterSummaryText, MARGIN, y);
  }
  y += MARGIN * 0.75;

  // Grille de cartes
  captured.forEach(function (item, i) {
    var col = i % COLS;
    var row = Math.floor(i / COLS);
    var x = MARGIN + col * (mapW + MARGIN);
    var ry = y + row * (LABEL_H + mapH + MARGIN);
    if (item.label) {
      ctx.font = "bold 10px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
      ctx.fillStyle = "#374151";
      var lbl = item.label.length > 70 ? item.label.substring(0, 67) + "\u2026" : item.label;
      ctx.fillText(lbl, x, ry + 11);
    }
    ctx.drawImage(item.canvas, 0, 0, item.canvas.width, item.canvas.height, x, ry + LABEL_H, mapW, mapH);
    ctx.strokeStyle = "#d1d5db";
    ctx.lineWidth = 0.5;
    ctx.strokeRect(x, ry + LABEL_H, mapW, mapH);
  });

  // Barre de légende (dégradé exact de scoreColor / CSS)
  if (hasLegend) {
    var legendY = y + ROWS * (LABEL_H + mapH + MARGIN) - MARGIN * 0.5;
    var barW = contentW; // pleine largeur du contenu
    var barH = 14;
    var lx = MARGIN;
    // Dégradé
    var grad = ctx.createLinearGradient(lx, legendY, lx + barW, legendY);
    grad.addColorStop(0,    "#f0fdf4");
    grad.addColorStop(0.25, "#bbf7d0");
    grad.addColorStop(0.50, "#4ade80");
    grad.addColorStop(0.75, "#16a34a");
    grad.addColorStop(1,    "#14532d");
    ctx.fillStyle = grad;
    ctx.fillRect(lx, legendY, barW, barH);
    ctx.strokeStyle = "#d1d5db";
    ctx.lineWidth = 0.5;
    ctx.strokeRect(lx, legendY, barW, barH);
    // Graduations lues depuis le DOM
    var ticksEl = document.getElementById("comparaison-map-legend-ticks");
    var tickTopY = legendY + barH + 2;
    if (ticksEl) {
      var tickEls = ticksEl.querySelectorAll(".tick");
      ctx.font = "9px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
      ctx.textAlign = "center";
      tickEls.forEach(function (tick) {
        var leftPct = parseFloat(tick.style.left) || 0; // "20%" → 20
        var tx = lx + (leftPct / 100) * barW;
        // Petite barre verticale
        ctx.fillStyle = "#9ca3af";
        ctx.fillRect(tx - 0.5, tickTopY, 1, 5);
        // Valeur
        ctx.fillStyle = "#6b7280";
        ctx.fillText(tick.textContent.trim(), tx, tickTopY + 5 + 9);
      });
      ctx.textAlign = "left";
    }
    // Étiquettes Min / Max sous les ticks
    var labelsY = tickTopY + 5 + 9 + 5;
    ctx.font = "10px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
    ctx.fillStyle = "#4b5563";
    ctx.textAlign = "left";
    ctx.fillText(legendMinText, lx, labelsY);
    ctx.textAlign = "right";
    ctx.fillText(legendMaxText, lx + barW, labelsY);
    ctx.textAlign = "left";

    // Cases colorées (légende des couleurs des polygones — mode communes)
    var communeHintEl = document.getElementById("comparaison-map-commune-legend-hint");
    if (hasHint && communeHintEl) {
      var hintItems   = communeHintEl.querySelectorAll(".legend-item");
      var hintSwatches = communeHintEl.querySelectorAll(".commune-legend-swatch");
      var swatchSz   = 10;
      var hintStartY = labelsY + 14;    // une ligne sous Min/Max
      var itemColW   = Math.floor(contentW / 2); // 2 colonnes
      ctx.font = "9px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
      ctx.textAlign = "left";
      hintItems.forEach(function (item, idx) {
        var col = idx % 2;
        var row = Math.floor(idx / 2);
        var ix = lx + col * itemColW;
        var iy = hintStartY + row * 16;
        var swatch = hintSwatches[idx];
        var bg     = swatch ? swatch.style.background     : "#d1d5db";
        var border = swatch ? swatch.style.borderColor    : "#9ca3af";
        // Tracé du carré coloré (gradient ou couleur unie)
        if (bg.indexOf("gradient") >= 0) {
          var gr = ctx.createLinearGradient(ix, iy, ix + swatchSz, iy + swatchSz);
          gr.addColorStop(0, "#bbf7d0");
          gr.addColorStop(1, "#16a34a");
          ctx.fillStyle = gr;
        } else {
          ctx.fillStyle = bg;
        }
        ctx.fillRect(ix, iy, swatchSz, swatchSz);
        ctx.strokeStyle = border || "#9ca3af";
        ctx.lineWidth = 0.5;
        ctx.strokeRect(ix, iy, swatchSz, swatchSz);
        // Libellé
        var text = item.textContent.trim().replace(/\s+/g, " ");
        ctx.fillStyle = "#374151";
        ctx.fillText(text, ix + swatchSz + 4, iy + 8);
      });
    }
  }
  return canvas;
}

function exportMapAsJpg() {
  var mapWrap = document.getElementById("comparaison-map-wrap");
  if (!mapWrap) return;
  var exportBtn = document.getElementById("btn-export-map-jpg");
  if (exportBtn) { exportBtn.disabled = true; exportBtn.textContent = "Export\u2026"; }

  var items = [];
  mapWrap.querySelectorAll(".comparaison-commune-map-card").forEach(function (card) {
    var lc = card.querySelector(".leaflet-container");
    if (!lc) return;
    var h4 = card.querySelector("h4");
    items.push({ label: h4 ? h4.textContent.trim() : "", container: lc });
  });
  mapWrap.querySelectorAll(".comparaison-map-card-choro").forEach(function (card) {
    var lc = card.querySelector(".leaflet-container");
    if (!lc) return;
    var head = card.querySelector(".card-head");
    items.push({ label: head ? head.textContent.trim() : "", container: lc });
  });

  if (!items.length) {
    if (exportBtn) { exportBtn.disabled = false; exportBtn.textContent = "Exporter JPG"; }
    alert("Aucune carte disponible pour l\u2019export.");
    return;
  }

  var titleEl = document.getElementById("comparaison-map-main-title");
  var filterSummaryEl = document.getElementById("comparaison-map-filter-summary");
  var legendEl = document.getElementById("comparaison-map-legend");
  var legendMinEl = document.getElementById("comparaison-map-legend-min");
  var legendMaxEl = document.getElementById("comparaison-map-legend-max");
  var titleText = titleEl ? titleEl.textContent.trim() : "Cartes";
  var filterSummaryText = (filterSummaryEl && !filterSummaryEl.hidden) ? filterSummaryEl.textContent.trim() : "";
  var legendMinText = legendMinEl ? legendMinEl.textContent.trim() : "";
  var legendMaxText = legendMaxEl ? legendMaxEl.textContent.trim() : "";
  var hasLegend = !!(legendEl && !legendEl.hidden);
  // La légende des couleurs (cases hors-périmètre/correspondance/…) s'applique
  // à toutes les cartes communales, quel que soit le sous-onglet actif.
  var hasHint = mapWrap.querySelectorAll(".comparaison-commune-map-card").length > 0;

  Promise.all(items.map(function (item) {
    return captureLeafletMapFull(item.container).then(function (canvas) {
      return { label: item.label, canvas: canvas };
    });
  })).then(function (captured) {
    captured = captured.filter(function (c) { return c.canvas; });
    if (!captured.length) throw new Error("Aucune carte captur\u00e9e.");
    var finalCanvas = _composeMapExportCanvas(captured, titleText, filterSummaryText, legendMinText, legendMaxText, hasLegend, hasHint);
    var link = document.createElement("a");
    link.download = "comparaison_cartes.jpg";
    link.href = finalCanvas.toDataURL("image/jpeg", 0.92);
    document.body.appendChild(link);
    link.click();
    setTimeout(function () { document.body.removeChild(link); }, 500);
  }).catch(function (err) {
    alert("Impossible d\u2019exporter les cartes.\n" + (err && err.message ? err.message : String(err)));
  }).finally(function () {
    if (exportBtn) { exportBtn.disabled = false; exportBtn.textContent = "Exporter JPG"; }
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
  var rowsForMaps =
    displayMode === "communes"
      ? buildFilteredRowsByFilterKeyForMaps(rowsByFilterKey, jobs, displayMode)
      : rowsByFilterKey;
  var hasAnyRowsFiltered = jobs.some(function (j) {
    return (rowsForMaps[j.filterKey] || []).length > 0;
  });
  if (!hasAnyRowsFiltered) {
    clearComparaisonMap();
    if (status) {
      var distFilt0 = S.lastDistanceOverlay ? parseDistanceFilterInputs() : { maxKm: null, maxMin: null };
      var activeDist0 =
        (distFilt0.maxKm != null && distFilt0.maxKm > 0) || (distFilt0.maxMin != null && distFilt0.maxMin > 0);
      if (S.lastDistanceOverlay && activeDist0) {
        status.textContent =
          "Aucune commune ne correspond aux critères de distance ou de durée de trajet ; assouplissez les seuils ou réinitialisez les filtres.";
      }
    }
    return;
  }
  if (!opts.preserveLegendBounds) {
    S.mapViz.communeViewsBySubTab = { dept: null, region: null, france: null };
  }
  var cat =
    opts.cat != null && opts.cat !== ""
      ? opts.cat
      : S.lastRenderTableArgs && S.lastRenderTableArgs.cat != null
        ? S.lastRenderTableArgs.cat
        : (function () {
            var el = document.getElementById("comparaison-categorie");
            return el ? el.value : "rentabilite";
          })();
  var displayIndicators =
    opts.displayIndicators != null
      ? opts.displayIndicators
      : S.lastRenderTableArgs && S.lastRenderTableArgs.displayIndicators
        ? S.lastRenderTableArgs.displayIndicators
        : [];
  var mapJobs = filterJobsForMapRendering(jobs, rowsForMaps, cat, displayIndicators).map(function (j) {
    return Object.assign({}, j, {
      mapScoreKey: getEffectiveMapScoreKeyForJob(j, cat, displayIndicators)
    });
  });
  if (!mapJobs.length) {
    clearComparaisonMap();
    if (status) {
      var distFilt1 = S.lastDistanceOverlay ? parseDistanceFilterInputs() : { maxKm: null, maxMin: null };
      var activeDist1 =
        (distFilt1.maxKm != null && distFilt1.maxKm > 0) || (distFilt1.maxMin != null && distFilt1.maxMin > 0);
      if (S.lastDistanceOverlay && activeDist1) {
        status.textContent =
          "Aucune carte à afficher : après filtre distance / durée, il ne reste aucune valeur de score exploitable pour la légende (assouplissez les seuils ou vérifiez les indicateurs).";
      } else {
        status.textContent =
          "Aucune carte à afficher : pour chaque combinaison (type de local, surface, pièces), des valeurs sont nécessaires ; les panneaux sans donnée pour ce score sont masqués.";
      }
    }
    return;
  }
  resetComparaisonMapVisuals();
  mapWrap.removeAttribute("aria-hidden");
  if (status) status.textContent = "Chargement des cartes…";
  if (titleEl) titleEl.textContent = "Cartes — " + getScoreLabel(scorePrincipal);

  var _filterSummary = S.lastRenderTableArgs ? (S.lastRenderTableArgs.filterSummary || "") : "";
  var _filterSummaryEl = document.getElementById("comparaison-map-filter-summary");
  if (_filterSummaryEl) {
    _filterSummaryEl.textContent = _filterSummary;
    _filterSummaryEl.hidden = !_filterSummary;
  }
  var _exportBtn = document.getElementById("btn-export-map-jpg");
  if (_exportBtn) {
    _exportBtn.hidden = false;
    if (!_exportBtn._wired) {
      _exportBtn._wired = true;
      _exportBtn.addEventListener("click", exportMapAsJpg);
    }
  }

  var allValues = [];
  mapJobs.forEach(function (job) {
    var rows = rowsForMaps[job.filterKey] || [];
    var mk = job.mapScoreKey || job.scoreKey;
    rows.forEach(function (r) {
      var v = numericScoreFromRow(r, mk);
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
    if (scorePrincipal === "renta_brute" || scorePrincipal === "renta_nette") {
      S.legendCustomMin = 0;
      S.legendCustomMax = 10;
    } else {
      S.legendCustomMin = null;
      S.legendCustomMax = null;
    }
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
    return renderChoroplethIntoDiv(e.mapId, rows, displayMode, e.job.mapScoreKey || e.job.scoreKey, min, max);
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
