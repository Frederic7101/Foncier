// =============================================================================
// Variables Globales — À modifier ici pour changer d’API, limites, délais, couleurs
// =============================================================================

// — API backend (vide = même origine ; file:// → localhost:8000)
const API_BASE =
  typeof window !== "undefined" && window.location?.protocol === "file:"
    ? "http://localhost:8000"
    : "";
const API_PATH_GEO = "/api/geo";
const API_PATH_PERIOD = "/api/period";
const API_PATH_COMMUNES = "/api/communes";
const API_PATH_STATS = "/api/stats";

// — Communes (liste déroulante et autocomplete)
const COMMUNES_CHUNK_SIZE = 1500;
const COMMUNE_AUTOCOMPLETE_MAX = 50;
const COMMUNE_DROPDOWN_HIDE_DELAY_MS = 150;

// — Période (bornes min/max par défaut, toast)
const PERIOD_YEAR_MIN_DEFAULT = "2000";
const PERIOD_YEAR_MAX_DEFAULT = "2030";
const PERIOD_TOAST_DURATION_MS = 4000;
const PERIOD_TOAST_FADEOUT_MS = 220;

// — Comparaison multi-lieux
const MAX_COMPARE_PLACES = 4;
const OVERLAY_CHART_KEYS = ["overlay-prix", "overlay-surface", "overlay-prixm2"];

/** Configs datasets pour buildChartOverlayMulti. group: 0 = moyenne, 1 = médiane, 2 = Q1, 3 = Q3. */
const PRIX_CONFIG = [
  { key: "prix_moyen", label: "Prix moyen (€)", group: 0 },
  { key: "prix_median", label: "Prix médian (€)", group: 1 },
  { key: "prix_q1", label: "Prix Q1 (€)", group: 2 },
  { key: "prix_q3", label: "Prix Q3 (€)", group: 3 },
];
const SURFACE_CONFIG = [
  { key: "surface_moyenne", label: "Surface moy. (m²)", group: 0 },
  { key: "surface_mediane", label: "Surface méd. (m²)", group: 1 },
];
const PRIXM2_CONFIG = [
  { key: "prix_m2_moyenne", label: "Prix/m² moy. (€)", group: 0 },
  { key: "prix_m2_mediane", label: "Prix/m² méd. (€)", group: 1 },
  { key: "prix_m2_q1", label: "Prix/m² Q1 (€)", group: 2 },
  { key: "prix_m2_q3", label: "Prix/m² Q3 (€)", group: 3 },
];

// — Graphiques : palettes de couleurs (4 courbes : moyenne, médiane, Q1, Q3)
const CHART_COLORS = [
  { border: "#2563eb", fill: "rgba(37, 99, 235, 0.1)" },
  { border: "#059669", fill: "rgba(5, 150, 105, 0.1)" },
  { border: "#d97706", fill: "rgba(217, 119, 6, 0.1)" },
  { border: "#7c3aed", fill: "rgba(124, 58, 237, 0.1)" },
];
const CHART_COLORS_LIGHT = [
  { border: "#93c5fd", fill: "rgba(147, 197, 253, 0.08)" },
  { border: "#6ee7b7", fill: "rgba(110, 231, 183, 0.08)" },
  { border: "#fcd34d", fill: "rgba(252, 211, 77, 0.08)" },
  { border: "#c4b5fd", fill: "rgba(196, 181, 253, 0.08)" },
];
const CHART_COLORS_2 = [
  { border: "#ea580c", fill: "rgba(234, 88, 12, 0.1)" },
  { border: "#ca8a04", fill: "rgba(202, 138, 4, 0.1)" },
  { border: "#c026d3", fill: "rgba(192, 38, 211, 0.1)" },
  { border: "#0891b2", fill: "rgba(8, 145, 178, 0.1)" },
];
const CHART_COLORS_3 = [
  { border: "#fb923c", fill: "rgba(251, 146, 60, 0.08)" },
  { border: "#eab308", fill: "rgba(234, 179, 8, 0.08)" },
  { border: "#d946ef", fill: "rgba(217, 70, 239, 0.08)" },
  { border: "#22d3ee", fill: "rgba(34, 211, 238, 0.08)" },
];
const OVERLAY_PALETTES = [CHART_COLORS, CHART_COLORS_LIGHT, CHART_COLORS_2, CHART_COLORS_3];
const OVERLAY_DASH = [[], [5, 4], [2, 2], [8, 4, 2, 4]];

// — Chart.js : mise en page
const CHART_LAYOUT_PADDING_LEFT = 14;
const CHART_TICKS_PADDING = 10;

// =============================================================================
// État applicatif (variables mutables)
// =============================================================================

let geo = { regions: [], departements: [], deptNomByCode: {} };
let communesList = [];
let communesList2 = [];
let communesList3 = [];
let communesList4 = [];
let comparePlaceCount = 2;
let allCommunesList = [];
let charts = { prix: null, surface: null, prixm2: null };
let compareMode = false;
let lastCompareDataForSingle = null;
let lastCompareData = [null, null, null, null];
let lastCompareTitre = [null, null, null, null];
let overlayMode = false;
let overlayActiveGroup = 0;

/** Liste des clés des graphiques en mode comparaison (prix-1, surface-1, prixm2-1, prix-2, …) pour synchronisation. */
function getCompareChartKeys() {
  if (!compareMode) return [];
  const keys = [];
  for (let s = 1; s <= comparePlaceCount; s++) {
    keys.push("prix-" + s, "surface-" + s, "prixm2-" + s);
  }
  return keys;
}

/** En mode comparaison : clés des graphiques de la même métrique que chartKey (ex. prix-1 → [prix-1, prix-2, …]). */
function getCompareChartKeysForMetric(chartKey) {
  if (!compareMode) return [];
  const m = String(chartKey).match(/^(prix|surface|prixm2)-(\d+)$/);
  if (!m) return [];
  const metric = m[1];
  const keys = [];
  for (let s = 1; s <= comparePlaceCount; s++) keys.push(metric + "-" + s);
  return keys;
}

function getDepartmentsForRegion(regionId) {
  const reg = geo.regions.find((r) => r.id === regionId);
  return reg ? reg.departements || [] : [];
}

function getRegionForDepartment(codeDept) {
  const reg = geo.regions.find((r) => r.departements && r.departements.includes(codeDept));
  return reg ? reg.id : null;
}

function fillDepartmentSelect(regionId, deptSelectId) {
  const id = deptSelectId || "dept-select";
  const deptSelect = document.getElementById(id);
  if (!deptSelect) return;
  const codes = regionId ? getDepartmentsForRegion(regionId) : (geo.departements || []).map((d) => (typeof d === "object" ? d.code : d));
  deptSelect.innerHTML = "<option value=''>— Choisir un département —</option>";
  codes.forEach((code) => {
    const libelle = geo.deptNomByCode[code] ? `${code} ${geo.deptNomByCode[code]}` : code;
    deptSelect.innerHTML += `<option value="${escapeHtml(code)}">${escapeHtml(libelle)}</option>`;
  });
}

function formatNum(v) {
  if (v == null || v === "") return "—";
  if (Number.isInteger(v)) return v.toLocaleString("fr-FR");
  return Number(v).toLocaleString("fr-FR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function renderList(ul, items) {
  ul.innerHTML = items
    .map(
      (item) =>
        `<li><span class="stat-label">${item.label}</span><span class="stat-value">${formatNum(item.value)}</span></li>`
    )
    .join("");
}

function destroyCharts() {
  ["prix", "surface", "prixm2"].forEach((id) => {
    if (charts[id]) {
      charts[id].destroy();
      charts[id] = null;
    }
  });
}

function destroyChartsForSide(side) {
  ["prix", "surface", "prixm2"].forEach((key) => {
    const id = `${key}-${side}`;
    if (charts[id]) {
      charts[id].destroy();
      charts[id] = null;
    }
  });
}

function buildTitre(p, built, side) {
  const { region_id_for_api, code_dept } = built;
  const { commune } = p;
  const list = side === 2 ? communesList2 : side === 3 ? communesList3 : side === 4 ? communesList4 : communesList;
  const regionNom = region_id_for_api
    ? geo.regions.find((r) => r.id === region_id_for_api)?.nom
    : (code_dept ? geo.regions.find((r) => r.departements && r.departements.includes(code_dept))?.nom : null);
  const deptLibelle = geo.deptNomByCode[code_dept] ? `${code_dept} ${geo.deptNomByCode[code_dept]}` : code_dept;
  if (built.niveau === "region") return regionNom || "Région";
  if (built.niveau === "department") return regionNom ? `${regionNom} / ${deptLibelle}` : deptLibelle;
  const postaux = list
    .filter((c) => c.commune === commune && c.code_dept === code_dept)
    .map((c) => c.code_postal)
    .filter((v, i, a) => a.indexOf(v) === i)
    .sort();
  const base = regionNom ? `${regionNom} / ${deptLibelle}` : deptLibelle;
  return `${base} / ${commune} (${postaux.join(", ")})`;
}

function renderCompareSide(side, data, titre, annee_min, annee_max) {
  const suffix = "-" + side;
  document.getElementById("stats-title-" + side).textContent = data == null ? "Erreur" : (titre || "—");
  const g = data?.global || {};
  const series = data?.series || [];
  document.getElementById("nb-ventes-" + side).textContent = formatNum(g.nb_ventes);
  renderList(document.getElementById("section-prix" + suffix), [
    { label: "Prix moyen", value: g.prix_moyen },
    { label: "Prix médian", value: g.prix_median },
    { label: "Prix Q1", value: g.prix_q1 },
    { label: "Prix Q3", value: g.prix_q3 },
  ]);
  renderList(document.getElementById("section-surface" + suffix), [
    { label: "Surface moyenne", value: g.surface_moyenne },
    { label: "Surface médiane", value: g.surface_mediane },
    { label: "Surface Q1", value: g.surface_q1 },
    { label: "Surface Q3", value: g.surface_q3 },
  ]);
  renderList(document.getElementById("section-prixm2" + suffix), [
    { label: "Prix/m² moyen", value: g.prix_m2_moyenne },
    { label: "Prix/m² médian", value: g.prix_m2_mediane },
    { label: "Prix/m² Q1", value: g.prix_m2_q1 },
    { label: "Prix/m² Q3", value: g.prix_m2_q3 },
  ]);
  const yMinDefault = 0;
  if (series.length > 1) {
    ["chart-prix" + suffix, "chart-surface" + suffix, "chart-prixm2" + suffix].forEach((id) => {
      const wrap = document.getElementById(id)?.closest(".chart-wrap");
      if (wrap) wrap.style.display = "block";
    });
    buildChartMulti("chart-prix" + suffix, series, [{ key: "prix_moyen", label: "Prix moyen (€)" }, { key: "prix_median", label: "Prix médian (€)" }, { key: "prix_q1", label: "Prix Q1 (€)" }, { key: "prix_q3", label: "Prix Q3 (€)" }], yMinDefault);
    buildChartMulti("chart-surface" + suffix, series, [{ key: "surface_moyenne", label: "Surface moyenne (m²)" }, { key: "surface_mediane", label: "Surface médiane (m²)" }], yMinDefault);
    buildChartMulti("chart-prixm2" + suffix, series, [{ key: "prix_m2_moyenne", label: "Prix/m² moyen (€)" }, { key: "prix_m2_mediane", label: "Prix/m² médian (€)" }, { key: "prix_m2_q1", label: "Prix/m² Q1 (€)" }, { key: "prix_m2_q3", label: "Prix/m² Q3 (€)" }], yMinDefault);
  } else {
    ["chart-prix" + suffix, "chart-surface" + suffix, "chart-prixm2" + suffix].forEach((id) => {
      const wrap = document.getElementById(id)?.closest(".chart-wrap");
      if (wrap) wrap.style.display = "none";
    });
  }
  const singleYear = annee_min && annee_max && String(annee_min).trim() === String(annee_max).trim();
  const tabsEl = document.querySelector("#stats-result-" + side + " .stats-tabs");
  if (tabsEl) tabsEl.classList.toggle("single-year", !!singleYear);
}

/** Affiche en vue simple (sans split) des données déjà chargées (ex. Lieu 1 après comparaison). */
function renderSingleView(data, titre, annee_min, annee_max) {
  const emptyEl = document.getElementById("stats-empty");
  const contentEl = document.getElementById("stats-content");
  const g = data?.global || {};
  const series = data?.series || [];
  const hasData = (g.nb_ventes != null && Number(g.nb_ventes) > 0) || (series && series.length > 0);
  if (!hasData) {
    emptyEl.textContent = data == null ? "Erreur" : "Aucune donnée pour ces critères.";
    emptyEl.style.display = "block";
    contentEl.setAttribute("aria-hidden", "true");
    document.querySelector(".stats-tabs")?.classList.remove("single-year");
    switchTab("prix");
    return;
  }
  emptyEl.style.display = "none";
  contentEl.setAttribute("aria-hidden", "false");
  document.getElementById("stats-title").textContent = titre || "—";
  document.getElementById("nb-ventes").textContent = formatNum(g.nb_ventes);
  renderList(document.getElementById("section-prix"), [
    { label: "Prix moyen", value: g.prix_moyen },
    { label: "Prix médian", value: g.prix_median },
    { label: "Prix Q1", value: g.prix_q1 },
    { label: "Prix Q3", value: g.prix_q3 },
  ]);
  renderList(document.getElementById("section-surface"), [
    { label: "Surface moyenne", value: g.surface_moyenne },
    { label: "Surface médiane", value: g.surface_mediane },
    { label: "Surface Q1", value: g.surface_q1 },
    { label: "Surface Q3", value: g.surface_q3 },
  ]);
  renderList(document.getElementById("section-prixm2"), [
    { label: "Prix/m² moyen", value: g.prix_m2_moyenne },
    { label: "Prix/m² médian", value: g.prix_m2_mediane },
    { label: "Prix/m² Q1", value: g.prix_m2_q1 },
    { label: "Prix/m² Q3", value: g.prix_m2_q3 },
  ]);
  if (series.length > 1) {
    ["chart-prix", "chart-surface", "chart-prixm2"].forEach((id) => {
      const wrap = document.getElementById(id).closest(".chart-wrap");
      if (wrap) wrap.style.display = "block";
    });
    document.querySelectorAll(".chart-y-axis-ctrl").forEach((ctrl) => {
      ctrl.classList.add("fixed");
      ctrl.querySelector(".chart-y-axis-btn").classList.add("fixed");
      ctrl.querySelector(".chart-y-axis-min").value = "0";
    });
    const yMinDefault = 0;
    buildChartMulti("chart-prix", series, [{ key: "prix_moyen", label: "Prix moyen (€)" }, { key: "prix_median", label: "Prix médian (€)" }, { key: "prix_q1", label: "Prix Q1 (€)" }, { key: "prix_q3", label: "Prix Q3 (€)" }], yMinDefault);
    buildChartMulti("chart-surface", series, [{ key: "surface_moyenne", label: "Surface moyenne (m²)" }, { key: "surface_mediane", label: "Surface médiane (m²)" }], yMinDefault);
    buildChartMulti("chart-prixm2", series, [{ key: "prix_m2_moyenne", label: "Prix/m² moyen (€)" }, { key: "prix_m2_mediane", label: "Prix/m² médian (€)" }, { key: "prix_m2_q1", label: "Prix/m² Q1 (€)" }, { key: "prix_m2_q3", label: "Prix/m² Q3 (€)" }], yMinDefault);
    requestAnimationFrame(() => {
      ["prix", "surface", "prixm2"].forEach((key) => { if (charts[key]) charts[key].resize(); });
    });
  } else {
    ["chart-prix", "chart-surface", "chart-prixm2"].forEach((id) => {
      const wrap = document.getElementById(id).closest(".chart-wrap");
      if (wrap) wrap.style.display = "none";
    });
  }
  const singleYear = annee_min && annee_max && String(annee_min).trim() === String(annee_max).trim();
  const tabsEl = document.querySelector(".stats-tabs");
  if (tabsEl) {
    tabsEl.classList.toggle("single-year", !!singleYear);
    if (!singleYear) switchTab("prix");
  }
}

/**
 * Construit un graphique avec plusieurs courbes.
 * yMin: null = axe Y automatique ; number = origine fixe (ex. 0).
 * Clic sur la légende affiche/masque la courbe correspondante.
 */
function buildChartMulti(canvasId, series, datasetsConfig, yMin = null) {
  const chartKey = canvasId.replace("chart-", "");
  const ctx = document.getElementById(canvasId).getContext("2d");
  if (charts[chartKey]) {
    charts[chartKey].destroy();
    charts[chartKey] = null;
  }
  const labels = series.map((s) => String(s.annee));
  const datasets = datasetsConfig.map((cfg, i) => {
    const color = CHART_COLORS[i % CHART_COLORS.length];
    return {
      label: cfg.label,
      data: series.map((s) => (s[cfg.key] != null ? s[cfg.key] : null)),
      borderColor: color.border,
      backgroundColor: color.fill,
      fill: true,
      tension: 0.2,
      hidden: false,
    };
  });
  const yScale = {
    beginAtZero: yMin === 0,
    ...(yMin != null && typeof yMin === "number" ? { min: yMin } : {}),
  };
  charts[chartKey] = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { left: CHART_LAYOUT_PADDING_LEFT } },
      interaction: { mode: "index", intersect: false },
      scales: {
        y: { ...yScale, ticks: { padding: CHART_TICKS_PADDING } },
      },
      plugins: {
        legend: {
          onClick: (e, legendItem, legend) => {
            const idx = legendItem.datasetIndex;
            const chart = legend.chart;
            const meta = chart.getDatasetMeta(idx);
            meta.hidden = !meta.hidden;
            chart.update();
            /* En mode comparaison : synchroniser visibilité de la courbe sur les autres graphiques du même type (prix, surface, prixm2). */
            const m = chartKey.match(/^(prix|surface|prixm2)-(\d+)$/);
            if (compareMode && m) {
              const metric = m[1];
              const newHidden = meta.hidden;
              for (let s = 1; s <= comparePlaceCount; s++) {
                const key = metric + "-" + s;
                const ch = charts[key];
                if (ch && ch.getDatasetMeta(idx)) {
                  ch.getDatasetMeta(idx).hidden = newHidden;
                  ch.update();
                }
              }
            }
          },
        },
      },
    },
  });
  /* Lieu unique : redimensionner après mise en page pour utiliser la largeur réelle de la zone. */
  if (["chart-prix", "chart-surface", "chart-prixm2"].includes(canvasId)) {
    requestAnimationFrame(() => { if (charts[chartKey]) charts[chartKey].resize(); });
  }
}

function destroyChartsForOverlay() {
  ["overlay-prix", "overlay-surface", "overlay-prixm2"].forEach((key) => {
    if (charts[key]) {
      charts[key].destroy();
      charts[key] = null;
    }
  });
}

/** Affiche la vue superposée (courbes de 2 à 4 lieux sur les mêmes graphiques). count = 2, 3 ou 4. */
function renderOverlayView(count) {
  overlayActiveGroup = 0;
  const n = count || (lastCompareData[1] ? (lastCompareData[2] ? (lastCompareData[3] ? 4 : 3) : 2) : 1);
  const titres = [];
  const seriesArray = [];
  for (let i = 0; i < n; i++) {
    titres.push(lastCompareTitre[i] || "—");
    seriesArray.push(lastCompareData[i]?.series || []);
  }
  document.getElementById("stats-overlay-title").textContent = titres.map((t, i) => "Lieu " + (i + 1) + " : " + t).join("  |  ");
  const hasEnough = seriesArray.some((s) => s.length > 1) || (seriesArray.every((s) => s.length === 1) && n > 0);
  const yMinDefault = 0;
  if (hasEnough) {
    if (n === 2) {
      buildChartOverlay("chart-overlay-prix", seriesArray[0], seriesArray[1], PRIX_CONFIG, yMinDefault);
      buildChartOverlay("chart-overlay-surface", seriesArray[0], seriesArray[1], SURFACE_CONFIG, yMinDefault);
      buildChartOverlay("chart-overlay-prixm2", seriesArray[0], seriesArray[1], PRIXM2_CONFIG, yMinDefault);
    } else {
      buildChartOverlayMulti("chart-overlay-prix", seriesArray, PRIX_CONFIG, yMinDefault);
      buildChartOverlayMulti("chart-overlay-surface", seriesArray, SURFACE_CONFIG, yMinDefault);
      buildChartOverlayMulti("chart-overlay-prixm2", seriesArray, PRIXM2_CONFIG, yMinDefault);
    }
  }
  document.getElementById("stats-overlay-results").setAttribute("aria-hidden", "false");
  document.getElementById("stats-compare-results").setAttribute("aria-hidden", "true");
  const overlayResults = document.getElementById("stats-overlay-results");
  overlayResults.querySelectorAll(".chart-wrap").forEach((wrap) => { wrap.style.display = hasEnough ? "block" : "none"; });
  if (hasEnough) {
    overlayResults.querySelectorAll(".chart-y-axis-ctrl").forEach((ctrl) => {
      ctrl.classList.add("fixed");
      ctrl.querySelector(".chart-y-axis-btn").classList.add("fixed");
      ctrl.querySelector(".chart-y-axis-min").value = "0";
    });
  }
  switchTabInContainer(overlayResults, "overlay-prix");
}

/**
 * Fusionne deux séries par année (union des années, triées).
 * Retourne { labels, map1, map2 } où map1[annee] = row du lieu 1, map2[annee] = row du lieu 2.
 */
function mergeSeriesByYear(series1, series2) {
  const years = new Set([
    ...(series1 || []).map((s) => s.annee),
    ...(series2 || []).map((s) => s.annee),
  ]);
  const labels = Array.from(years).sort((a, b) => Number(a) - Number(b)).map(String);
  const map1 = Object.fromEntries((series1 || []).map((s) => [String(s.annee), s]));
  const map2 = Object.fromEntries((series2 || []).map((s) => [String(s.annee), s]));
  return { labels, map1, map2 };
}

/** Fusionne N séries par année. Retourne { labels, maps } avec maps[i][annee] = row du lieu i+1. */
function mergeSeriesByYearMulti(seriesArray) {
  const years = new Set();
  seriesArray.forEach((s) => (s || []).forEach((row) => years.add(row.annee)));
  const labels = Array.from(years).sort((a, b) => Number(a) - Number(b)).map(String);
  const maps = seriesArray.map((s) => Object.fromEntries((s || []).map((row) => [String(row.annee), row])));
  return { labels, maps };
}

/**
 * Clic légende mode comparaison : un seul type de courbe à la fois (groupe). Afficher le groupe cliqué pour tous les lieux.
 * Synchronise le groupe actif sur les 3 graphiques overlay (prix, surface, prix/m²).
 * Si un graphique n'a pas de courbe pour ce groupe (ex. Surface n'a pas Q1/Q3), on affiche le groupe 0 pour ce graphique.
 */
function overlayLegendClick(chart, datasetIndex) {
  const ds = chart.data.datasets[datasetIndex];
  const group = ds && (ds.group != null ? ds.group : 0);
  overlayActiveGroup = group;
  const updateChartVisibility = (ch) => {
    if (!ch) return;
    const hasGroup = ch.data.datasets.some((d) => (d.group != null ? d.group : 0) === group);
    const g = hasGroup ? group : 0;
    ch.data.datasets.forEach((d, i) => {
      const meta = ch.getDatasetMeta(i);
      meta.hidden = (d.group != null ? d.group : 0) !== g;
    });
    ch.update();
  };
  OVERLAY_CHART_KEYS.forEach((key) => updateChartVisibility(charts[key]));
}

/**
 * Graphique superposé : courbes Lieu 1 (couleurs foncées, trait plein) et Lieu 2 (couleurs claires, pointillés).
 * En mode comparaison : seule la moyenne (group 0) est affichée par défaut ; la légende permet de basculer par type (médiane, Q1, Q3).
 */
function buildChartOverlay(canvasId, series1, series2, datasetsConfig, yMin = null) {
  const chartKey = canvasId.replace("chart-", "");
  const ctx = document.getElementById(canvasId).getContext("2d");
  if (charts[chartKey]) {
    charts[chartKey].destroy();
    charts[chartKey] = null;
  }
  const { labels, map1, map2 } = mergeSeriesByYear(series1, series2);
  const datasets = [];
  datasetsConfig.forEach((cfg, i) => {
    const group = cfg.group != null ? cfg.group : 0;
    const dark = CHART_COLORS[i % CHART_COLORS.length];
    const light = CHART_COLORS_LIGHT[i % CHART_COLORS_LIGHT.length];
    datasets.push({
      label: cfg.label + " (Lieu 1)",
      data: labels.map((y) => (map1[y] && map1[y][cfg.key] != null ? map1[y][cfg.key] : null)),
      borderColor: dark.border,
      backgroundColor: dark.fill,
      fill: true,
      tension: 0.2,
      borderDash: [],
      group,
    });
    datasets.push({
      label: cfg.label + " (Lieu 2)",
      data: labels.map((y) => (map2[y] && map2[y][cfg.key] != null ? map2[y][cfg.key] : null)),
      borderColor: light.border,
      backgroundColor: light.fill,
      fill: true,
      tension: 0.2,
      borderDash: [5, 4],
      group,
    });
  });
  const yScale = {
    beginAtZero: yMin === 0,
    ...(yMin != null && typeof yMin === "number" ? { min: yMin } : {}),
  };
  charts[chartKey] = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { left: CHART_LAYOUT_PADDING_LEFT } },
      interaction: { mode: "index", intersect: false },
      scales: { y: { ...yScale, ticks: { padding: CHART_TICKS_PADDING } } },
      plugins: {
        legend: {
          onClick: (e, legendItem, legend) => {
            overlayLegendClick(legend.chart, legendItem.datasetIndex);
          },
        },
      },
    },
  });
  /* Afficher uniquement le groupe actif (synchronisé avec les autres graphiques overlay). */
  charts[chartKey].data.datasets.forEach((d, i) => {
    const g = d.group != null ? d.group : 0;
    charts[chartKey].getDatasetMeta(i).hidden = g !== overlayActiveGroup;
  });
  charts[chartKey].update();
}

/**
 * Graphique superposé pour N lieux (2 à 4). Chaque métrique a N courbes (Lieu 1, Lieu 2, …).
 * Par défaut seule la moyenne (group 0) est affichée ; la légende permet de basculer par type (médiane, Q1, Q3).
 */
function buildChartOverlayMulti(canvasId, seriesArray, datasetsConfig, yMin = null) {
  const chartKey = canvasId.replace("chart-", "");
  const ctx = document.getElementById(canvasId).getContext("2d");
  if (charts[chartKey]) {
    charts[chartKey].destroy();
    charts[chartKey] = null;
  }
  const N = seriesArray.length;
  const { labels, maps } = mergeSeriesByYearMulti(seriesArray);
  const datasets = [];
  datasetsConfig.forEach((cfg, metricIdx) => {
    const group = cfg.group != null ? cfg.group : 0;
    for (let placeIdx = 0; placeIdx < N; placeIdx++) {
      const palette = OVERLAY_PALETTES[placeIdx % OVERLAY_PALETTES.length];
      const color = palette[metricIdx % palette.length];
      datasets.push({
        label: cfg.label + " (Lieu " + (placeIdx + 1) + ")",
        data: labels.map((y) => (maps[placeIdx][y] && maps[placeIdx][y][cfg.key] != null ? maps[placeIdx][y][cfg.key] : null)),
        borderColor: color.border,
        backgroundColor: color.fill,
        fill: true,
        tension: 0.2,
        borderDash: OVERLAY_DASH[placeIdx % OVERLAY_DASH.length] || [],
        group,
      });
    }
  });
  const yScale = {
    beginAtZero: yMin === 0,
    ...(yMin != null && typeof yMin === "number" ? { min: yMin } : {}),
  };
  charts[chartKey] = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { left: CHART_LAYOUT_PADDING_LEFT } },
      interaction: { mode: "index", intersect: false },
      scales: { y: { ...yScale, ticks: { padding: CHART_TICKS_PADDING } } },
      plugins: {
        legend: {
          onClick: (e, legendItem, legend) => {
            overlayLegendClick(legend.chart, legendItem.datasetIndex);
          },
        },
      },
    },
  });
  /* Afficher uniquement le groupe actif (synchronisé avec les autres graphiques overlay). */
  charts[chartKey].data.datasets.forEach((d, i) => {
    const g = d.group != null ? d.group : 0;
    charts[chartKey].getDatasetMeta(i).hidden = g !== overlayActiveGroup;
  });
  charts[chartKey].update();
}

async function loadGeo() {
  const res = await fetch(`${API_BASE}${API_PATH_GEO}`);
  if (!res.ok) throw new Error("Erreur chargement geo");
  geo = await res.json();
  // Map code → nom pour l’affichage (source : ref_departements en base)
  geo.deptNomByCode = Object.fromEntries((geo.departements || []).map((d) => [d.code, d.nom]));
  const regionOpts = "<option value=''>— Choisir une région —</option>" + geo.regions.map((r) => `<option value="${r.id}">${escapeHtml(r.nom)}</option>`).join("");
  ["region-select", "region-1-select", "region-2-select", "region-3-select", "region-4-select"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = regionOpts;
  });
  fillDepartmentSelect(null);
  fillDepartmentSelect(null, "dept-1-select");
  fillDepartmentSelect(null, "dept-2-select");
  fillDepartmentSelect(null, "dept-3-select");
  fillDepartmentSelect(null, "dept-4-select");
}

async function loadPeriod() {
  const res = await fetch(`${API_BASE}${API_PATH_PERIOD}`);
  if (!res.ok) return;
  const p = await res.json();
  document.getElementById("annee-min").value = p.annee_min ?? "";
  document.getElementById("annee-max").value = p.annee_max ?? "";
  updatePeriodConstraints();
}

function fillCommuneOptionsChunked(communeSelect, list, startIndex) {
  const end = Math.min(startIndex + COMMUNES_CHUNK_SIZE, list.length);
  const fragment = document.createDocumentFragment();
  for (let i = startIndex; i < end; i++) {
    const c = list[i];
    const opt = document.createElement("option");
    opt.value = i;
    opt.textContent = `${c.commune} (${c.code_postal})`;
    fragment.appendChild(opt);
  }
  communeSelect.appendChild(fragment);
  if (end < list.length) {
    requestAnimationFrame(() => fillCommuneOptionsChunked(communeSelect, list, end));
  }
}

function getCommuneSelectId(suffix) {
  const s = String(suffix ?? "");
  if (s === "1") return "commune-1-select";
  if (s === "2") return "commune-2-select";
  if (s === "3") return "commune-3-select";
  if (s === "4") return "commune-4-select";
  return "commune-select";
}

async function loadCommunes(codeDept, suffix) {
  const s = String(suffix ?? "");
  const selectId = getCommuneSelectId(s);
  const communeSelect = document.getElementById(selectId);
  if (!communeSelect) return;
  // Réinitialiser le select
  communeSelect.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "— Choisir une commune —";
  communeSelect.appendChild(placeholder);
  const url = codeDept
    ? `${API_BASE}${API_PATH_COMMUNES}?code_dept=${encodeURIComponent(codeDept)}`
    : `${API_BASE}${API_PATH_COMMUNES}`;
  const res = await fetch(url);
  if (!res.ok) {
    return;
  }
  const list = await res.json();
  if (s === "2") communesList2 = list.slice(0);
  else if (s === "3") communesList3 = list.slice(0);
  else if (s === "4") communesList4 = list.slice(0);
  else {
    communesList = list.slice(0);
    if (!codeDept) allCommunesList = list.slice(0);
  }
  fillCommuneOptionsChunked(communeSelect, list, 0);
}

function restoreAllCommunesInDropdown(suffix) {
  const s = String(suffix ?? "");
  const list = allCommunesList.slice(0);
  const selectId = getCommuneSelectId(s);
  const communeSelect = document.getElementById(selectId);
  if (!communeSelect || !list.length) return;
  if (s === "2") communesList2 = list.slice(0);
  else if (s === "3") communesList3 = list.slice(0);
  else if (s === "4") communesList4 = list.slice(0);
  else communesList = list.slice(0);
  // Reconstruire complètement la liste déroulante
  communeSelect.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "— Choisir une commune —";
  communeSelect.appendChild(placeholder);
  fillCommuneOptionsChunked(communeSelect, list, 0);
}

/** Normalise une chaîne pour la recherche : minuscules, sans accents, tirets et espaces multiples ramenés à un espace. */
function normalizeForCommuneSearch(str) {
  if (str == null || typeof str !== "string") return "";
  const s = str
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[\s\-]+/g, " ")
    .trim();
  return s;
}

function getCommuneListForInputId(inputId) {
  if (!inputId) return [];
  if (inputId === "commune-select" || inputId === "commune-1-select") return communesList || [];
  if (inputId === "commune-2-select") return communesList2 || [];
  if (inputId === "commune-3-select") return communesList3 || [];
  if (inputId === "commune-4-select") return communesList4 || [];
  return [];
}

function showCommuneAutocomplete(input, dropdown, dataEl, list) {
  const rawQuery = (input.value || "").trim();
  const q = normalizeForCommuneSearch(rawQuery);
  const filtered = !q
    ? list.slice(0, COMMUNE_AUTOCOMPLETE_MAX)
    : list.filter((c) => {
        const normCommune = normalizeForCommuneSearch(c.commune);
        const normPostal = String(c.code_postal || "").replace(/\s/g, "");
        const queryNoSpace = q.replace(/\s/g, "");
        return (
          (normCommune && normCommune.includes(q)) ||
          (normCommune && normCommune.replace(/\s/g, "").includes(queryNoSpace)) ||
          (normPostal && normPostal.includes(queryNoSpace))
        );
      }).slice(0, COMMUNE_AUTOCOMPLETE_MAX);
  dropdown.innerHTML = "";
  dropdown.removeAttribute("aria-hidden");
  input.setAttribute("aria-expanded", "true");
  if (filtered.length === 0) {
    const empty = document.createElement("div");
    empty.className = "stats-commune-option-empty";
    empty.textContent = q ? "Aucune commune trouvée" : "Choisissez un département ou tapez pour rechercher";
    dropdown.appendChild(empty);
  } else {
    filtered.forEach((c) => {
      const opt = document.createElement("div");
      opt.className = "stats-commune-option";
      opt.role = "option";
      opt.textContent = `${c.commune} (${c.code_postal})`;
      opt.dataset.commune = c.commune || "";
      opt.dataset.codePostal = c.code_postal || "";
      opt.dataset.codeDept = c.code_dept || "";
      dropdown.appendChild(opt);
    });
  }
}

function initCommuneAutocomplete(inputId, dataId, dropdownId, placeOrSingle) {
  const input = document.getElementById(inputId);
  const dataEl = document.getElementById(dataId);
  const dropdown = document.getElementById(dropdownId);
  if (!input || !dropdown) return;
  let hideTimeout = null;
  function getList() {
    return getCommuneListForInputId(inputId);
  }
  function hide() {
    hideTimeout = setTimeout(() => {
      dropdown.setAttribute("aria-hidden", "true");
      input.setAttribute("aria-expanded", "false");
    }, COMMUNE_DROPDOWN_HIDE_DELAY_MS);
  }
  function selectCommune(c) {
    input.value = `${c.commune} (${c.code_postal})`;
    if (dataEl) dataEl.value = JSON.stringify({ code_dept: c.code_dept || "", code_postal: c.code_postal || "", commune: c.commune || "" });
    dropdown.setAttribute("aria-hidden", "true");
    input.setAttribute("aria-expanded", "false");
    input.dispatchEvent(new Event("change", { bubbles: true }));
  }
  input.addEventListener("focus", () => {
    if (hideTimeout) clearTimeout(hideTimeout);
    const list = getList();
    if (list.length) showCommuneAutocomplete(input, dropdown, dataEl, list);
  });
  input.addEventListener("input", () => {
    if (hideTimeout) clearTimeout(hideTimeout);
    // Toute saisie manuelle annule la commune précédemment sélectionnée
    if (dataEl) dataEl.value = "";
    const list = getList();
    showCommuneAutocomplete(input, dropdown, dataEl, list);
  });
  input.addEventListener("blur", hide);
  dropdown.addEventListener("mousedown", (e) => e.preventDefault());
  dropdown.addEventListener("click", (e) => {
    const opt = e.target.closest(".stats-commune-option");
    if (!opt) return;
    selectCommune({
      commune: opt.dataset.commune,
      code_postal: opt.dataset.codePostal,
      code_dept: opt.dataset.codeDept,
    });
  });
  input.addEventListener("keydown", (e) => {
    const opts = dropdown.querySelectorAll(".stats-commune-option");
    if (e.key === "Escape") {
      dropdown.setAttribute("aria-hidden", "true");
      input.setAttribute("aria-expanded", "false");
      // Échappe = annuler la sélection de commune en cours
      if (dataEl) dataEl.value = "";
      // on laisse le texte tel quel pour que l'utilisateur puisse corriger ou effacer
      return;
    }
    if (e.key === "ArrowDown" || e.key === "ArrowUp" || e.key === "Enter") {
      e.preventDefault();
      const current = dropdown.querySelector(".stats-commune-option[aria-selected='true']");
      let idx = current ? Array.from(opts).indexOf(current) : -1;
      if (e.key === "ArrowDown") idx = Math.min(idx + 1, opts.length - 1);
      else if (e.key === "ArrowUp") idx = Math.max(idx - 1, 0);
      opts.forEach((o, i) => o.setAttribute("aria-selected", i === idx));
      if (e.key === "Enter" && idx >= 0 && opts[idx]) {
        selectCommune({
          commune: opts[idx].dataset.commune,
          code_postal: opts[idx].dataset.codePostal,
          code_dept: opts[idx].dataset.codeDept,
        });
      }
    }
  });
}

function loadCommunesInBackground() {
  const communeSelect = document.getElementById("commune-select");
  if (!communeSelect || allCommunesList.length > 0) return;
  fetch(`${API_BASE}/api/communes`)
    .then((res) => {
      if (!res.ok) throw new Error("Erreur chargement communes");
      return res.json();
    })
    .then((list) => {
      allCommunesList = list.slice(0);
      communesList = list.slice(0);
      if (document.getElementById("dept-select").value) return;
      restoreAllCommunesInDropdown("");
    })
    .catch(() => {
      /* en cas d'erreur, on laisse simplement la liste vide */
    });
}

function escapeHtml(s) {
  if (s == null) return "";
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

function getCommuneValue() {
  const select = document.getElementById("commune-select");
  if (!select || !select.value) return { code_dept: null, code_postal: null, commune: null };
  const idx = parseInt(select.value, 10);
  const list = communesList && communesList.length ? communesList : allCommunesList;
  const c = !Number.isNaN(idx) && list[idx] ? list[idx] : null;
  if (!c) return { code_dept: null, code_postal: null, commune: null };
  return { code_dept: c.code_dept || null, code_postal: c.code_postal || null, commune: c.commune || null };
}

function getCommuneValueForPlace(place) {
  const p = typeof place === "number" ? place : parseInt(place, 10);
  const select = document.getElementById("commune-" + p + "-select");
  if (!select || !select.value) return { code_dept: null, code_postal: null, commune: null };
  const idx = parseInt(select.value, 10);
  let list = [];
  if (p === 1) list = communesList || [];
  else if (p === 2) list = communesList2 || [];
  else if (p === 3) list = communesList3 || [];
  else if (p === 4) list = communesList4 || [];
  const c = !Number.isNaN(idx) && list[idx] ? list[idx] : null;
  if (!c) return { code_dept: null, code_postal: null, commune: null };
  return { code_dept: c.code_dept || null, code_postal: c.code_postal || null, commune: c.commune || null };
}

function getPlaceValues(place) {
  const regionId = document.getElementById("region-" + place + "-select")?.value || null;
  let code_dept = document.getElementById("dept-" + place + "-select")?.value || null;
  const c = getCommuneValueForPlace(place);
  if (c.code_dept) code_dept = c.code_dept;
  return {
    region_id: regionId,
    code_dept: code_dept || null,
    code_postal: c.code_postal || null,
    commune: c.commune || null,
  };
}

function syncSingleToPlace1() {
  document.getElementById("region-1-select").value = document.getElementById("region-select").value || "";
  document.getElementById("dept-1-select").value = document.getElementById("dept-select").value || "";
  const inp = document.getElementById("commune-select");
  const inp1 = document.getElementById("commune-1-select");
  if (inp && inp1) inp1.value = inp.value;
  communesList = allCommunesList.length ? allCommunesList.slice(0) : communesList.slice(0);
}

function syncPlace1ToSingle() {
  document.getElementById("region-select").value = document.getElementById("region-1-select").value || "";
  document.getElementById("dept-select").value = document.getElementById("dept-1-select").value || "";
  const inp1 = document.getElementById("commune-1-select");
  const inp = document.getElementById("commune-select");
  if (inp1 && inp) inp.value = inp1.value;
  communesList = communesList.slice(0);
}

function setCompareMode(enabled) {
  compareMode = enabled;
  const section = document.getElementById("stats-place-section");
  const single = document.getElementById("stats-place-single");
  const compare = document.getElementById("stats-place-compare");
  const compareBtn = document.getElementById("stats-compare-btn");
  const content = document.getElementById("stats-content");
  const compareResults = document.getElementById("stats-compare-results");
  const overlayBtn = document.getElementById("stats-overlay-btn");
  const overlayResults = document.getElementById("stats-overlay-results");
  if (enabled) {
    section.classList.add("compare-mode");
    single.style.display = "none";
    compare.setAttribute("aria-hidden", "false");
    syncSingleToPlace1();
    comparePlaceCount = 2;
    updateComparePlaceUI();
    switchLieuTab(2);
    compareBtn.textContent = "Quitter la comparaison";
    content.setAttribute("aria-hidden", "true");
    compareResults.setAttribute("aria-hidden", "true");
    overlayResults.setAttribute("aria-hidden", "true");
    overlayMode = false;
  } else {
    section.classList.remove("compare-mode");
    single.style.display = "block";
    compare.setAttribute("aria-hidden", "true");
    syncPlace1ToSingle();
    compareBtn.textContent = "Comparer";
    compareResults.setAttribute("aria-hidden", "true");
    overlayResults.setAttribute("aria-hidden", "true");
    overlayMode = false;
    if (overlayBtn) overlayBtn.style.display = "none";
    destroyChartsForSide(1);
    destroyChartsForSide(2);
    destroyChartsForOverlay();
    if (lastCompareDataForSingle) {
      destroyCharts();
      renderSingleView(
        lastCompareDataForSingle.data,
        lastCompareDataForSingle.titre,
        lastCompareDataForSingle.annee_min,
        lastCompareDataForSingle.annee_max
      );
    } else {
      content.setAttribute("aria-hidden", "true");
    }
  }
}

function buildNiveauAndParams(p, type_local, annee_min, annee_max) {
  const { region_id, code_dept, code_postal, commune } = p;
  let niveau;
  if (commune && code_postal && code_dept) niveau = "commune";
  else if (code_dept) niveau = "department";
  else if (region_id) niveau = "region";
  else return null;
  const region_id_for_api = niveau === "region" ? region_id : (region_id || getRegionForDepartment(code_dept));
  const params = new URLSearchParams({
    niveau,
    ...(niveau === "region" && region_id_for_api && { region_id: region_id_for_api }),
    ...(code_dept && { code_dept }),
    ...(code_postal && { code_postal }),
    ...(commune && { commune }),
    ...(type_local && { type_local }),
    ...(annee_min && { annee_min }),
    ...(annee_max && { annee_max }),
  });
  return { niveau, region_id_for_api, code_dept, params };
}

async function submitStats() {
  const type_local = document.getElementById("type-local").value || undefined;
  const annee_min = document.getElementById("annee-min").value || undefined;
  const annee_max = document.getElementById("annee-max").value || undefined;

  if (compareMode) {
    const places = [];
    const builds = [];
    for (let i = 1; i <= comparePlaceCount; i++) {
      const p = getPlaceValues(i);
      const b = buildNiveauAndParams(p, type_local, annee_min, annee_max);
      if (!b) {
        alert("Choisissez au moins une région, un département ou une commune pour chaque lieu.");
        return;
      }
      places.push(p);
      builds.push(b);
    }
    document.getElementById("stats-loading").setAttribute("aria-hidden", "false");
    document.getElementById("stats-empty").style.display = "none";
    document.getElementById("stats-content").setAttribute("aria-hidden", "true");
    document.getElementById("stats-compare-results").setAttribute("aria-hidden", "true");
    document.getElementById("stats-overlay-results").setAttribute("aria-hidden", "true");
    destroyCharts();
    for (let s = 1; s <= MAX_COMPARE_PLACES; s++) destroyChartsForSide(s);
    try {
      const responses = await Promise.all(builds.map((b) => fetch(`${API_BASE}${API_PATH_STATS}?${b.params}`)));
      document.getElementById("stats-loading").setAttribute("aria-hidden", "true");
      const allData = await Promise.all(responses.map((r) => (r.ok ? r.json() : null)));
      for (let i = 0; i < comparePlaceCount; i++) {
        lastCompareData[i] = allData[i];
        lastCompareTitre[i] = buildTitre(places[i], builds[i], i + 1);
      }
      for (let i = comparePlaceCount; i < MAX_COMPARE_PLACES; i++) {
        lastCompareData[i] = null;
        lastCompareTitre[i] = null;
      }
      lastCompareDataForSingle = { data: allData[0], titre: lastCompareTitre[0], annee_min, annee_max };
      document.getElementById("stats-overlay-btn").style.display = "block";
      if (comparePlaceCount >= 4) {
        overlayMode = true;
        document.getElementById("stats-overlay-btn").textContent = "Quitter la superposition";
        renderOverlayView(comparePlaceCount);
      } else {
        overlayMode = false;
        document.getElementById("stats-overlay-results").setAttribute("aria-hidden", "true");
        document.getElementById("stats-overlay-btn").textContent = "Superposer les graphiques";
        updateComparePlaceUI();
        for (let i = 1; i <= comparePlaceCount; i++) {
          renderCompareSide(i, allData[i - 1], lastCompareTitre[i - 1], annee_min, annee_max);
        }
        document.getElementById("stats-compare-results").setAttribute("aria-hidden", "false");
        syncCompareChartsYScales();
        for (let i = 1; i <= comparePlaceCount; i++) {
          const container = document.getElementById("stats-result-" + i);
          if (container) switchTabInContainer(container, "prix-" + i);
        }
      }
    } catch (e) {
      document.getElementById("stats-loading").setAttribute("aria-hidden", "true");
      alert("Erreur : " + (e.message || String(e)));
    }
    return;
  }

  const region_id = document.getElementById("region-select").value || null;
  let code_dept = document.getElementById("dept-select").value || null;
  const c = getCommuneValue();
  const code_postal = c.code_postal || null;
  const commune = c.commune || null;
  if (c.code_dept) code_dept = c.code_dept;

  const p = { region_id, code_dept, code_postal, commune };
  const built = buildNiveauAndParams(p, type_local, annee_min, annee_max);
  if (!built) {
    alert("Choisissez au moins une région, un département ou une commune.");
    return;
  }
  const { params } = built;

  document.getElementById("stats-loading").setAttribute("aria-hidden", "false");
  document.getElementById("stats-empty").style.display = "none";
  document.getElementById("stats-content").setAttribute("aria-hidden", "true");
  document.getElementById("stats-compare-results").setAttribute("aria-hidden", "true");
  lastCompareDataForSingle = null;
  destroyCharts();
  destroyChartsForSide(1);
  destroyChartsForSide(2);
  try {
    const res = await fetch(`${API_BASE}/api/stats?${params}`);
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || res.statusText);
    }
    const data = await res.json();
    document.getElementById("stats-loading").setAttribute("aria-hidden", "true");
    const g = data.global || {};
    const series = data.series || [];
    const hasData = (g.nb_ventes != null && Number(g.nb_ventes) > 0) || (series && series.length > 0);
    if (!hasData) {
      document.getElementById("stats-empty").textContent = "Aucune donnée pour ces critères.";
      document.getElementById("stats-empty").style.display = "block";
      document.getElementById("stats-content").setAttribute("aria-hidden", "true");
      document.querySelector(".stats-tabs")?.classList.remove("single-year");
      switchTab("prix");
    } else {
      document.getElementById("stats-empty").style.display = "none";
      document.getElementById("stats-content").setAttribute("aria-hidden", "false");
    }
    const titre = buildTitre(p, built, null);
    document.getElementById("stats-title").textContent = titre;
    document.getElementById("nb-ventes").textContent = formatNum(g.nb_ventes);

    renderList(document.getElementById("section-prix"), [
      { label: "Prix moyen", value: g.prix_moyen },
      { label: "Prix médian", value: g.prix_median },
      { label: "Prix Q1", value: g.prix_q1 },
      { label: "Prix Q3", value: g.prix_q3 },
    ]);
    renderList(document.getElementById("section-surface"), [
      { label: "Surface moyenne", value: g.surface_moyenne },
      { label: "Surface médiane", value: g.surface_mediane },
      { label: "Surface Q1", value: g.surface_q1 },
      { label: "Surface Q3", value: g.surface_q3 },
    ]);
    renderList(document.getElementById("section-prixm2"), [
      { label: "Prix/m² moyen", value: g.prix_m2_moyenne },
      { label: "Prix/m² médian", value: g.prix_m2_mediane },
      { label: "Prix/m² Q1", value: g.prix_m2_q1 },
      { label: "Prix/m² Q3", value: g.prix_m2_q3 },
    ]);

    if (series.length > 1) {
      ["chart-prix", "chart-surface", "chart-prixm2"].forEach((id) => {
        const wrap = document.getElementById(id).closest(".chart-wrap");
        if (wrap) wrap.style.display = "block";
      });
      document.querySelectorAll(".chart-y-axis-ctrl").forEach((ctrl) => {
        ctrl.classList.add("fixed");
        ctrl.querySelector(".chart-y-axis-btn").classList.add("fixed");
        ctrl.querySelector(".chart-y-axis-min").value = "0";
      });
      const yMinDefault = 0;
      buildChartMulti("chart-prix", series, [{ key: "prix_moyen", label: "Prix moyen (€)" }, { key: "prix_median", label: "Prix médian (€)" }, { key: "prix_q1", label: "Prix Q1 (€)" }, { key: "prix_q3", label: "Prix Q3 (€)" }], yMinDefault);
      buildChartMulti("chart-surface", series, [{ key: "surface_moyenne", label: "Surface moyenne (m²)" }, { key: "surface_mediane", label: "Surface médiane (m²)" }], yMinDefault);
      buildChartMulti("chart-prixm2", series, [{ key: "prix_m2_moyenne", label: "Prix/m² moyen (€)" }, { key: "prix_m2_mediane", label: "Prix/m² médian (€)" }, { key: "prix_m2_q1", label: "Prix/m² Q1 (€)" }, { key: "prix_m2_q3", label: "Prix/m² Q3 (€)" }], yMinDefault);
      requestAnimationFrame(() => {
        ["prix", "surface", "prixm2"].forEach((key) => { if (charts[key]) charts[key].resize(); });
      });
    } else {
      ["chart-prix", "chart-surface", "chart-prixm2"].forEach((id) => {
        const wrap = document.getElementById(id).closest(".chart-wrap");
        if (wrap) wrap.style.display = "none";
      });
    }

    const singleYear = annee_min && annee_max && String(annee_min).trim() === String(annee_max).trim();
    const tabsEl = document.querySelector(".stats-tabs");
    if (tabsEl) {
      tabsEl.classList.toggle("single-year", !!singleYear);
      if (!singleYear) switchTab("prix");
    }
  } catch (e) {
    document.getElementById("stats-loading").setAttribute("aria-hidden", "true");
    alert("Erreur : " + (e.message || String(e)));
  }
}

function switchTab(tabId) {
  document.querySelectorAll(".stats-tab-btn").forEach((btn) => {
    const isActive = btn.getAttribute("data-tab") === tabId;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive);
  });
  document.querySelectorAll(".stats-tab-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.id === "tab-panel-" + tabId);
  });
  /* Lieu unique : redimensionner le graphique de l’onglet affiché (il a maintenant une largeur réelle). */
  if (["prix", "surface", "prixm2"].includes(tabId) && charts[tabId]) {
    requestAnimationFrame(() => charts[tabId].resize());
  }
}

function applyYAxisToChart(chartKey, minValue, maxValue) {
  const ch = charts[chartKey];
  if (!ch) return;
  const y = ch.options.scales.y;

  // Réinitialisation complète (échelle auto)
  if (minValue == null && maxValue == null) {
    y.min = undefined;
    y.max = undefined;
    y.beginAtZero = false;
    if (y.ticks && Object.prototype.hasOwnProperty.call(y.ticks, "stepSize")) {
      delete y.ticks.stepSize;
    }
    ch.update();
    return;
  }

  // Valeurs numériques brutes fournies à la fonction
  let vMin = minValue != null ? Number(minValue) : null;
  let vMax = maxValue != null ? Number(maxValue) : null;

  if (vMin != null && Number.isNaN(vMin)) vMin = null;
  if (vMax != null && Number.isNaN(vMax)) vMax = null;

  // Appliquer d'abord les bornes telles quelles
  if (vMin != null) {
    y.min = vMin;
    y.beginAtZero = vMin === 0;
  }
  if (vMax != null) {
    y.max = vMax;
  } else {
    y.max = undefined;
  }

  // Arrondi des bornes à la graduation inférieure/supérieure
  const hasMin = vMin != null;
  const hasMax = vMax != null;
  if (hasMin || hasMax) {
    const rangeInfo = getChartDataRange(chartKey);
    let lo = hasMin ? vMin : rangeInfo.min;
    let hi = hasMax ? vMax : rangeInfo.max;

    if (Number.isFinite(lo) && Number.isFinite(hi) && hi > lo) {
      const rawRange = hi - lo;
      const roughStep = rawRange / 6 || 1; // ~6 graduations
      const pow10 = Math.pow(10, Math.floor(Math.log10(Math.abs(roughStep))));
      const candidates = [1, 2, 5, 10];
      let step = pow10;
      for (let i = 0; i < candidates.length; i++) {
        const s = candidates[i] * pow10;
        if (roughStep <= s) {
          step = s;
          break;
        }
      }

      let niceMin = Math.floor(lo / step) * step;
      const loDiv = lo / step;
      if (Math.abs(loDiv - Math.round(loDiv)) < 1e-6) {
        const candidate = lo - step;
        niceMin = candidate < 0 ? 0 : candidate;
      }

      let niceMax = Math.ceil(hi / step) * step;
      const hiDiv = hi / step;
      if (Math.abs(hiDiv - Math.round(hiDiv)) < 1e-6) {
        niceMax = hi + step;
      }

      if (hasMin) {
        y.min = niceMin;
        y.beginAtZero = niceMin === 0;
      }
      if (hasMax) {
        y.max = niceMax;
      }

      if (!y.ticks) y.ticks = {};
      y.ticks.stepSize = step;
    }
  }

  ch.update();
}

/** Retourne le min et max des valeurs affichées dans un graphique (tous datasets visibles). */
function getChartDataRange(chartKey) {
  const ch = charts[chartKey];
  if (!ch || !ch.data || !ch.data.datasets) return { min: 0, max: 1 };
  let dataMin = Infinity;
  let dataMax = -Infinity;
  ch.data.datasets.forEach((ds) => {
    (ds.data || []).forEach((v) => {
      if (v != null && typeof v === "number" && !Number.isNaN(v)) {
        if (v < dataMin) dataMin = v;
        if (v > dataMax) dataMax = v;
      }
    });
  });
  if (dataMin === Infinity) dataMin = 0;
  if (dataMax === -Infinity) dataMax = 1;
  return { min: dataMin, max: dataMax };
}

/** En mode comparaison : retourne le min et max des valeurs sur tous les graphiques de la même métrique que chartKey. */
function getGlobalRangeForMetric(chartKey) {
  const keys = getCompareChartKeysForMetric(chartKey);
  if (!keys.length) return getChartDataRange(chartKey);
  let globalMin = Infinity;
  let globalMax = -Infinity;
  keys.forEach((key) => {
    const range = getChartDataRange(key);
    if (range.min < globalMin) globalMin = range.min;
    if (range.max > globalMax) globalMax = range.max;
  });
  if (globalMin === Infinity) globalMin = 0;
  if (globalMax <= globalMin) globalMax = globalMin + 1;
  return { min: globalMin, max: globalMax };
}

/** En mode comparaison : initialise les échelles Y avec échelle commune (min=0, max=max des max de tous les lieux par métrique) ; les boutons toggler restent inactifs. */
function syncCompareChartsYScales() {
  const metrics = ["prix", "surface", "prixm2"];
  metrics.forEach((metric) => {
    const keys = [];
    for (let s = 1; s <= comparePlaceCount; s++) keys.push(metric + "-" + s);
    const { max: globalMax } = getGlobalRangeForMetric(keys[0]);
    keys.forEach((key) => applyYAxisToChart(key, 0, globalMax));
  });
  document.querySelectorAll("#stats-compare-results .chart-y-axis-ctrl").forEach((ctrl) => {
    ctrl.classList.remove("fixed");
    const btn = ctrl.querySelector(".chart-y-axis-btn");
    if (btn) btn.classList.remove("fixed");
    const input = ctrl.querySelector(".chart-y-axis-min");
    if (input) input.value = "0";
  });
}

document.getElementById("region-select").addEventListener("change", () => {
  const regionId = document.getElementById("region-select").value;
  fillDepartmentSelect(regionId || null);
  document.getElementById("dept-select").value = "";
  restoreAllCommunesInDropdown();
  refreshStatsIfResultsVisible();
});

document.getElementById("dept-select").addEventListener("change", () => {
  const codeDept = document.getElementById("dept-select").value;
  if (!codeDept) {
    restoreAllCommunesInDropdown();
    refreshStatsIfResultsVisible();
    return;
  }
  const inferredRegion = getRegionForDepartment(codeDept);
  if (inferredRegion) document.getElementById("region-select").value = inferredRegion;
  loadCommunes(codeDept);
  refreshStatsIfResultsVisible();
});

document.getElementById("commune-select").addEventListener("change", () => {
  const c = getCommuneValue();
  if (c.code_dept && (c.commune || c.code_postal)) {
    document.getElementById("dept-select").value = c.code_dept;
    const regionId = getRegionForDepartment(c.code_dept);
    if (regionId) document.getElementById("region-select").value = regionId;
  }
  refreshStatsIfResultsVisible();
});

function bindPlaceSelects(place) {
  const s = String(place);
  const regionId = "region-" + place + "-select";
  const deptId = "dept-" + place + "-select";
  const communeId = "commune-" + place + "-select";
  document.getElementById(regionId)?.addEventListener("change", function () {
    const regionVal = this.value;
    fillDepartmentSelect(regionVal || null, deptId);
    document.getElementById(deptId).value = "";
    restoreAllCommunesInDropdown(s);
    refreshStatsIfResultsVisible();
  });
  document.getElementById(deptId)?.addEventListener("change", function () {
    const codeDept = this.value;
    if (!codeDept) {
      restoreAllCommunesInDropdown(s);
      refreshStatsIfResultsVisible();
      return;
    }
    const inferredRegion = getRegionForDepartment(codeDept);
    if (inferredRegion) document.getElementById(regionId).value = inferredRegion;
    loadCommunes(codeDept, s);
    refreshStatsIfResultsVisible();
  });
  document.getElementById(communeId)?.addEventListener("change", function () {
    const c = getCommuneValueForPlace(place);
    if (c.code_dept && (c.commune || c.code_postal)) {
      document.getElementById(deptId).value = c.code_dept;
      const regionIdVal = getRegionForDepartment(c.code_dept);
      if (regionIdVal) document.getElementById(regionId).value = regionIdVal;
    }
    refreshStatsIfResultsVisible();
  });
}
bindPlaceSelects(1);
bindPlaceSelects(2);
bindPlaceSelects(3);
bindPlaceSelects(4);

function updateComparePlaceUI() {
  document.querySelectorAll(".stats-lieu-tab-btn[data-lieu]").forEach((btn) => {
    const n = parseInt(btn.getAttribute("data-lieu"), 10);
    const hidden = n > comparePlaceCount;
    btn.classList.toggle("stats-lieu-tab-hidden", hidden);
    if (hidden) btn.classList.remove("active");
  });
  document.querySelectorAll(".stats-lieu-panel").forEach((panel) => {
    const n = parseInt(panel.id.replace("stats-lieu-panel-", ""), 10);
    panel.classList.toggle("stats-lieu-panel-hidden", n > comparePlaceCount);
  });
  const addBtn = document.getElementById("stats-lieu-add-btn");
  if (addBtn) {
    addBtn.style.display = comparePlaceCount >= MAX_COMPARE_PLACES ? "none" : "inline-flex";
    addBtn.textContent = "+ Lieu " + (comparePlaceCount + 1);
    addBtn.title = "Ajouter le lieu " + (comparePlaceCount + 1) + " (max " + MAX_COMPARE_PLACES + ")";
  }
  document.querySelectorAll("#stats-compare-results .stats-result-half").forEach((el) => {
    const n = parseInt(el.id.replace("stats-result-", ""), 10);
    el.classList.toggle("stats-result-hidden", n > comparePlaceCount);
  });
  const compareResults = document.getElementById("stats-compare-results");
  if (compareResults) {
    compareResults.classList.remove("compare-2", "compare-3", "compare-4");
    if (comparePlaceCount >= 2) compareResults.classList.add("compare-" + comparePlaceCount);
  }
}

function addComparePlace() {
  if (comparePlaceCount >= MAX_COMPARE_PLACES) return;
  comparePlaceCount++;
  updateComparePlaceUI();
  switchLieuTab(comparePlaceCount);
}

function removeComparePlace(placeNum) {
  if (placeNum < 1 || placeNum > comparePlaceCount) return;
  /* Avec 2 lieux : supprimer le 2e lieu = quitter la comparaison ; supprimer le 1er = décalage (le 2e devient le 1er) puis quitter avec une seule donnée. */
  if (comparePlaceCount === 2 && placeNum === 2) {
    setCompareMode(false);
    return;
  }
  /* Copier les valeurs du lieu i+1 vers le lieu i pour combler le trou (sens : i+1 → i). */
  for (let i = placeNum; i < comparePlaceCount; i++) {
    const fromId = i + 1;
    document.getElementById("region-" + i + "-select").value = document.getElementById("region-" + fromId + "-select").value || "";
    document.getElementById("dept-" + i + "-select").value = document.getElementById("dept-" + fromId + "-select").value || "";
    const inpFrom = document.getElementById("commune-" + fromId + "-select");
    const inpTo = document.getElementById("commune-" + i + "-select");
    const dataFrom = document.getElementById("commune-" + fromId + "-data");
    const dataTo = document.getElementById("commune-" + i + "-data");
    if (inpFrom && inpTo) inpTo.value = inpFrom.value;
    if (dataFrom && dataTo) dataTo.value = dataFrom.value;
  }
  /* Décaler les listes de communes : liste(i) = ancienne liste(i+1), en montant pour ne pas écraser une source. */
  for (let i = placeNum; i < comparePlaceCount; i++) {
    const src = i === 1 ? communesList2 : i === 2 ? communesList3 : i === 3 ? communesList4 : null;
    if (i === 1) communesList = (src && src.slice(0)) || [];
    else if (i === 2) communesList2 = (src && src.slice(0)) || [];
    else if (i === 3) communesList3 = (src && src.slice(0)) || [];
  }
  document.getElementById("region-" + comparePlaceCount + "-select").value = "";
  document.getElementById("dept-" + comparePlaceCount + "-select").value = "";
  const clearedInput = document.getElementById("commune-" + comparePlaceCount + "-select");
  const clearedData = document.getElementById("commune-" + comparePlaceCount + "-data");
  if (clearedInput) { clearedInput.value = ""; clearedInput.placeholder = "— Choisir une commune —"; }
  if (clearedData) clearedData.value = "";
  fillDepartmentSelect(null, "dept-" + comparePlaceCount + "-select");
  const prevCount = comparePlaceCount;
  comparePlaceCount--;
  /* Aligner les données avec le formulaire décalé : nouveau slot 1 = ancien slot placeNum, etc. */
  if (placeNum === 1 || placeNum === 2) {
    lastCompareData = lastCompareData.slice(1);
    lastCompareTitre = lastCompareTitre.slice(1);
  } else if (placeNum === prevCount) {
    lastCompareData = lastCompareData.slice(0, prevCount - 1);
    lastCompareTitre = lastCompareTitre.slice(0, prevCount - 1);
  } else {
    lastCompareData = lastCompareData.slice(0, placeNum - 1).concat(lastCompareData.slice(placeNum));
    lastCompareTitre = lastCompareTitre.slice(0, placeNum - 1).concat(lastCompareTitre.slice(placeNum));
  }
  const anneeMin = document.getElementById("annee-min").value || "";
  const anneeMax = document.getElementById("annee-max").value || "";
  if (lastCompareData[0]) {
    lastCompareDataForSingle = { data: lastCompareData[0], titre: lastCompareTitre[0], annee_min: anneeMin, annee_max: anneeMax };
  }
  updateComparePlaceUI();
  /* Après décalage il ne reste qu’un lieu : quitter la comparaison et afficher la vue simple avec ses données. */
  if (comparePlaceCount === 1) {
    setCompareMode(false);
    return;
  }
  if (overlayMode && comparePlaceCount < 4) {
    overlayMode = false;
    document.getElementById("stats-overlay-results").setAttribute("aria-hidden", "true");
    document.getElementById("stats-compare-results").setAttribute("aria-hidden", "false");
    document.getElementById("stats-overlay-btn").textContent = "Superposer les graphiques";
    destroyChartsForOverlay();
    for (let s = 1; s <= MAX_COMPARE_PLACES; s++) destroyChartsForSide(s);
    const compareEl = document.getElementById("stats-compare-results");
    compareEl.classList.remove("compare-2", "compare-3", "compare-4");
    compareEl.classList.add("compare-" + comparePlaceCount);
    for (let i = 1; i <= comparePlaceCount; i++) {
      renderCompareSide(i, lastCompareData[i - 1], lastCompareTitre[i - 1], anneeMin, anneeMax);
    }
    for (let i = 1; i <= comparePlaceCount; i++) {
      const container = document.getElementById("stats-result-" + i);
      if (container) switchTabInContainer(container, "prix-" + i);
    }
  }
  switchLieuTab(Math.min(placeNum, comparePlaceCount));
}

function switchLieuTab(lieuNum) {
  document.querySelectorAll(".stats-lieu-tab-btn:not(.stats-lieu-tab-hidden)").forEach((btn) => {
    const isActive = btn.getAttribute("data-lieu") === String(lieuNum);
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive);
  });
  document.querySelectorAll(".stats-lieu-panel:not(.stats-lieu-panel-hidden)").forEach((panel) => {
    panel.classList.toggle("active", panel.id === "stats-lieu-panel-" + lieuNum);
  });
}
document.getElementById("stats-place-compare").addEventListener("click", (e) => {
  const tabBtn = e.target.closest(".stats-lieu-tab-btn[data-lieu]");
  if (!tabBtn || tabBtn.classList.contains("stats-lieu-tab-hidden")) return;
  if (e.target.closest(".stats-lieu-tab-close")) {
    e.preventDefault();
    e.stopPropagation();
    removeComparePlace(parseInt(tabBtn.getAttribute("data-lieu"), 10));
    return;
  }
  switchLieuTab(tabBtn.getAttribute("data-lieu"));
});
document.getElementById("stats-lieu-add-btn").addEventListener("click", addComparePlace);

document.getElementById("stats-compare-btn").addEventListener("click", () => {
  setCompareMode(!compareMode);
});

document.getElementById("stats-overlay-btn").addEventListener("click", () => {
  if (overlayMode) {
    overlayMode = false;
    document.getElementById("stats-overlay-results").setAttribute("aria-hidden", "true");
    document.getElementById("stats-compare-results").setAttribute("aria-hidden", "false");
    destroyChartsForOverlay();
    document.getElementById("stats-overlay-btn").textContent = "Superposer les graphiques";
    syncCompareChartsYScales();
    for (let i = 1; i <= comparePlaceCount; i++) {
      const container = document.getElementById("stats-result-" + i);
      if (container) switchTabInContainer(container, "prix-" + i);
    }
  } else {
    overlayMode = true;
    renderOverlayView();
    document.getElementById("stats-overlay-btn").textContent = "Quitter la superposition";
  }
});

document.getElementById("stats-btn").addEventListener("click", submitStats);

/** Relance le chargement des stats (et donc met à jour les nb ventes) si des résultats sont déjà affichés. Appelé à chaque modification d'un critère (type, période, lieu). */
function refreshStatsIfResultsVisible() {
  const loading = document.getElementById("stats-loading");
  const compareResults = document.getElementById("stats-compare-results");
  const overlayResults = document.getElementById("stats-overlay-results");
  const content = document.getElementById("stats-content");
  if (compareMode) {
    const compareVisible = compareResults && compareResults.getAttribute("aria-hidden") !== "true";
    const overlayVisible = overlayResults && overlayResults.getAttribute("aria-hidden") !== "true";
    if (compareVisible || overlayVisible) {
      submitStats();
    }
    return;
  }
  if (content && content.getAttribute("aria-hidden") !== "true" && loading && loading.getAttribute("aria-hidden") === "true") {
    submitStats();
  }
}

let periodToastTimeout = null;
function showPeriodToast(message) {
  const el = document.getElementById("stats-period-toast");
  if (!el) return;
  if (periodToastTimeout) clearTimeout(periodToastTimeout);
  el.textContent = message;
  el.removeAttribute("hidden");
  el.setAttribute("data-visible", "true");
  periodToastTimeout = setTimeout(() => {
    el.setAttribute("data-visible", "false");
    periodToastTimeout = setTimeout(() => {
      el.setAttribute("hidden", "");
      el.textContent = "";
      periodToastTimeout = null;
    }, PERIOD_TOAST_FADEOUT_MS);
  }, PERIOD_TOAST_DURATION_MS);
}

function updatePeriodConstraints() {
  const minEl = document.getElementById("annee-min");
  const maxEl = document.getElementById("annee-max");
  const minVal = minEl.value.trim();
  const maxVal = maxEl.value.trim();
  const minNum = minVal ? parseInt(minVal, 10) : NaN;
  const maxNum = maxVal ? parseInt(maxVal, 10) : NaN;
  minEl.max = !Number.isNaN(maxNum) ? String(maxNum) : PERIOD_YEAR_MAX_DEFAULT;
  maxEl.min = !Number.isNaN(minNum) ? String(minNum) : PERIOD_YEAR_MIN_DEFAULT;
}

function validatePeriodBounds() {
  const minEl = document.getElementById("annee-min");
  const maxEl = document.getElementById("annee-max");
  const minVal = minEl.value.trim();
  const maxVal = maxEl.value.trim();
  if (!minVal || !maxVal) return null;
  const min = parseInt(minVal, 10);
  const max = parseInt(maxVal, 10);
  if (Number.isNaN(min) || Number.isNaN(max)) return null;
  if (min > max) return { min, max };
  return null;
}

document.getElementById("annee-min").addEventListener("input", updatePeriodConstraints);
document.getElementById("annee-min").addEventListener("change", () => {
  const r = validatePeriodBounds();
  if (r) {
    document.getElementById("annee-min").value = r.max;
    showPeriodToast("La période est incohérente, elle a été corrigée automatiquement.");
  }
  updatePeriodConstraints();
  refreshStatsIfResultsVisible();
});

document.getElementById("annee-max").addEventListener("input", updatePeriodConstraints);
document.getElementById("annee-max").addEventListener("change", () => {
  const r = validatePeriodBounds();
  if (r) {
    document.getElementById("annee-max").value = r.min;
    showPeriodToast("La période est incohérente, elle a été corrigée automatiquement.");
  }
  updatePeriodConstraints();
  refreshStatsIfResultsVisible();
});

document.getElementById("type-local").addEventListener("change", refreshStatsIfResultsVisible);

document.getElementById("stats-reset-btn").addEventListener("click", () => {
  document.getElementById("region-select").value = "";
  document.getElementById("dept-select").value = "";
  const communeInput = document.getElementById("commune-select");
  const communeData = document.getElementById("commune-data");
  if (communeInput) communeInput.value = "";
  if (communeData) communeData.value = "";
  fillDepartmentSelect(null);
  restoreAllCommunesInDropdown("");
  if (compareMode) {
    for (let i = 1; i <= MAX_COMPARE_PLACES; i++) {
      document.getElementById("region-" + i + "-select").value = "";
      document.getElementById("dept-" + i + "-select").value = "";
      const ci = document.getElementById("commune-" + i + "-select");
      if (ci) ci.value = "";
      fillDepartmentSelect(null, "dept-" + i + "-select");
      restoreAllCommunesInDropdown(String(i));
    }
    // Effacer tout état de résultats de comparaison avant de quitter le mode comparaison
    lastCompareDataForSingle = null;
    lastCompareData = [];
    lastCompareTitre = [];
    // Sortir du mode comparaison : ne conserver que le formulaire simple (un seul « lieu »)
    setCompareMode(false);
  }
  document.getElementById("type-local").value = "";
  document.getElementById("annee-min").value = "";
  document.getElementById("annee-max").value = "";
  document.querySelector(".stats-tabs")?.classList.remove("single-year");
  document.querySelectorAll("[id^='stats-result-'] .stats-tabs").forEach((el) => el?.classList.remove("single-year"));
  switchTab("prix");
  updatePeriodConstraints();
  // Effacer aussi les résultats actuellement affichés (vue simple ou compare/overlay)
  destroyCharts();
  destroyChartsForSide(1);
  destroyChartsForSide(2);
  destroyChartsForOverlay();
  const content = document.getElementById("stats-content");
  const compareResults = document.getElementById("stats-compare-results");
  const overlayResults = document.getElementById("stats-overlay-results");
  if (content) content.setAttribute("aria-hidden", "true");
  if (compareResults) compareResults.setAttribute("aria-hidden", "true");
  if (overlayResults) overlayResults.setAttribute("aria-hidden", "true");
  const overlayBtn = document.getElementById("stats-overlay-btn");
  if (overlayBtn) overlayBtn.style.display = "none";
  const emptyEl = document.getElementById("stats-empty");
  if (emptyEl) {
    emptyEl.textContent = "Choisissez des critères et cliquez sur Afficher.";
    emptyEl.style.display = "block";
  }
  const toast = document.getElementById("stats-period-toast");
  if (toast) {
    if (periodToastTimeout) clearTimeout(periodToastTimeout);
    toast.setAttribute("hidden", "");
    toast.removeAttribute("data-visible");
    toast.textContent = "";
  }
});

document.getElementById("stats-controls").addEventListener("keydown", (e) => {
  if (e.key !== "Enter" || !e.target.closest(".stats-form")) return;
  if (e.target.id === "stats-btn") return;
  if (e.target.matches("input, select")) {
    e.preventDefault();
    document.getElementById("stats-btn").click();
  }
});

document.querySelectorAll(".stats-tab-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    const tabId = btn.getAttribute("data-tab");
    const container = btn.closest(".stats-result-half") || btn.closest(".stats-overlay-results");
    if (container) {
      switchTabInContainer(container, tabId);
      const compareResults = document.getElementById("stats-compare-results");
      if (container.classList.contains("stats-result-half") && compareResults && compareResults.getAttribute("aria-hidden") !== "true") {
        const m = String(tabId).match(/^(prix|surface|prixm2)-(\d+)$/);
        if (m) {
          const metric = m[1];
          for (let n = 1; n <= comparePlaceCount; n++) {
            const otherContainer = document.getElementById("stats-result-" + n);
            if (otherContainer && !otherContainer.classList.contains("stats-result-hidden")) {
              switchTabInContainer(otherContainer, metric + "-" + n);
            }
          }
        }
      }
    } else {
      switchTab(tabId);
    }
  });
});

function switchTabInContainer(container, tabId) {
  if (!container) return;
  container.querySelectorAll(".stats-tab-btn").forEach((btn) => {
    const isActive = btn.getAttribute("data-tab") === tabId;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive);
  });
  container.querySelectorAll(".stats-tab-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.id === "tab-panel-" + tabId);
  });
}

document.getElementById("stats-results").addEventListener("click", (e) => {
  const btn = e.target.closest(".chart-y-axis-btn");
  if (!btn) return;
  const ctrl = btn.closest(".chart-y-axis-ctrl");
  if (!ctrl) return;
  const chartKey = ctrl.getAttribute("data-chart");
  if (!chartKey || !charts[chartKey]) return;
  e.preventDefault();
  const isCompareY = compareMode && document.getElementById("stats-compare-results")?.contains(ctrl);
  const isFixed = ctrl.classList.contains("fixed");
  if (isFixed) {
    if (isCompareY) {
      const keysSameMetric = getCompareChartKeysForMetric(chartKey);
      const { max: globalMax } = getGlobalRangeForMetric(chartKey);
      keysSameMetric.forEach((key) => applyYAxisToChart(key, 0, globalMax));
      keysSameMetric.forEach((key) => {
        const c = document.querySelector("#stats-compare-results .chart-y-axis-ctrl[data-chart=\"" + key + "\"]");
        if (c) {
          c.classList.remove("fixed");
          const b = c.querySelector(".chart-y-axis-btn");
          if (b) b.classList.remove("fixed");
          const inp = c.querySelector(".chart-y-axis-min");
          if (inp) inp.value = "0";
        }
      });
    } else {
      ctrl.classList.remove("fixed");
      btn.classList.remove("fixed");
      applyYAxisToChart(chartKey, null);
    }
  } else {
    const input = ctrl.querySelector(".chart-y-axis-min");
    if (isCompareY) {
      const { min: globalMin, max: globalMax } = getGlobalRangeForMetric(chartKey);
      const keysSameMetric = getCompareChartKeysForMetric(chartKey);
      ctrl.classList.add("fixed");
      btn.classList.add("fixed");
      keysSameMetric.forEach((key) => applyYAxisToChart(key, globalMin, globalMax));
      const effectiveMin = charts[chartKey]?.options?.scales?.y?.min ?? globalMin;
      keysSameMetric.forEach((key) => {
        const c = document.querySelector("#stats-compare-results .chart-y-axis-ctrl[data-chart=\"" + key + "\"]");
        if (c) {
          c.classList.add("fixed");
          const b = c.querySelector(".chart-y-axis-btn");
          if (b) b.classList.add("fixed");
          const inp = c.querySelector(".chart-y-axis-min");
          if (inp) inp.value = String(effectiveMin);
        }
      });
    } else {
      const v = parseFloat(input.value, 10) || 0;
      ctrl.classList.add("fixed");
      btn.classList.add("fixed");
      applyYAxisToChart(chartKey, v);
    }
  }
});

document.getElementById("stats-results").addEventListener("input", (e) => {
  if (!e.target.classList.contains("chart-y-axis-min")) return;
  const ctrl = e.target.closest(".chart-y-axis-ctrl");
  if (!ctrl || !ctrl.classList.contains("fixed")) return;
  const chartKey = ctrl.getAttribute("data-chart");
  if (!chartKey || !charts[chartKey]) return;
  const v = parseFloat(e.target.value, 10);
  const minVal = Number.isNaN(v) ? 0 : v;
  if (compareMode && document.getElementById("stats-compare-results")?.contains(ctrl)) {
    const { max: globalMax } = getGlobalRangeForMetric(chartKey);
    const keysSameMetric = getCompareChartKeysForMetric(chartKey);
    keysSameMetric.forEach((key) => applyYAxisToChart(key, minVal, globalMax));
    keysSameMetric.forEach((key) => {
      if (key === chartKey) return;
      const c = document.querySelector("#stats-compare-results .chart-y-axis-ctrl[data-chart=\"" + key + "\"]");
      const inp = c?.querySelector(".chart-y-axis-min");
      if (inp) inp.value = String(minVal);
    });
  } else {
    applyYAxisToChart(chartKey, minVal);
  }
});

loadGeo()
  .then(loadPeriod)
  .then(() => {
    setTimeout(() => loadCommunesInBackground(), 0);
  })
  .catch((e) => console.error(e));
