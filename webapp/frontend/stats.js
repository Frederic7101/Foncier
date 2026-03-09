// Vide = même origine ; si page ouverte en file:// (ancienne URL), appeler le backend sur localhost:8000
const API_BASE =
  typeof window !== "undefined" && window.location?.protocol === "file:"
    ? "http://localhost:8000"
    : "";

/** Noms des départements français (code INSEE → nom) */
const DEPT_NAMES = {
  "01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence", "05": "Hautes-Alpes",
  "06": "Alpes-Maritimes", "07": "Ardèche", "08": "Ardennes", "09": "Ariège", "10": "Aube",
  "11": "Aude", "12": "Aveyron", "13": "Bouches-du-Rhône", "14": "Calvados", "15": "Cantal",
  "16": "Charente", "17": "Charente-Maritime", "18": "Cher", "19": "Corrèze", "21": "Côte-d'Or",
  "22": "Côtes-d'Armor", "23": "Creuse", "24": "Dordogne", "25": "Doubs", "26": "Drôme",
  "27": "Eure", "28": "Eure-et-Loir", "29": "Finistère", "30": "Gard", "31": "Haute-Garonne",
  "32": "Gers", "33": "Gironde", "34": "Hérault", "35": "Ille-et-Vilaine", "36": "Indre",
  "37": "Indre-et-Loire", "38": "Isère", "39": "Jura", "40": "Landes", "41": "Loir-et-Cher",
  "42": "Loire", "43": "Haute-Loire", "44": "Loire-Atlantique", "45": "Loiret", "46": "Lot",
  "47": "Lot-et-Garonne", "48": "Lozère", "49": "Maine-et-Loire", "50": "Manche", "51": "Marne",
  "52": "Haute-Marne", "53": "Mayenne", "54": "Meurthe-et-Moselle", "55": "Meuse", "56": "Morbihan",
  "57": "Moselle", "58": "Nièvre", "59": "Nord", "60": "Oise", "61": "Orne", "62": "Pas-de-Calais",
  "63": "Puy-de-Dôme", "64": "Pyrénées-Atlantiques", "65": "Hautes-Pyrénées", "66": "Pyrénées-Orientales",
  "67": "Bas-Rhin", "68": "Haut-Rhin", "69": "Rhône", "70": "Haute-Saône", "71": "Saône-et-Loire",
  "72": "Sarthe", "73": "Savoie", "74": "Haute-Savoie", "75": "Paris", "76": "Seine-Maritime",
  "77": "Seine-et-Marne", "78": "Yvelines", "79": "Deux-Sèvres", "80": "Somme", "81": "Tarn",
  "82": "Tarn-et-Garonne", "83": "Var", "84": "Vaucluse", "85": "Vendée", "86": "Vienne",
  "87": "Haute-Vienne", "88": "Vosges", "89": "Yonne", "90": "Territoire de Belfort",
  "91": "Essonne", "92": "Hauts-de-Seine", "93": "Seine-Saint-Denis", "94": "Val-de-Marne",
  "95": "Val-d'Oise", "2A": "Corse-du-Sud", "2B": "Haute-Corse",
};

let geo = { regions: [], departements: [] };
let communesList = []; // liste des communes (lieu unique ou lieu 1)
let communesList2 = [];
let communesList3 = [];
let communesList4 = [];
const MAX_COMPARE_PLACES = 4;
let comparePlaceCount = 2;
let allCommunesList = []; // copie de la liste complète pour restaurer quand on désélectionne le département
let charts = { prix: null, surface: null, prixm2: null };
let compareMode = false;
/** Données du Lieu 1 après une comparaison, pour réafficher en vue simple sans refetch */
let lastCompareDataForSingle = null;
/** Données et titres des lieux après une comparaison (index 0 = lieu 1, pour mode superposition) */
let lastCompareData = [null, null, null, null];
let lastCompareTitre = [null, null, null, null];
/** Mode superposition des graphiques (courbes des 2 lieux sur les mêmes graphiques) */
let overlayMode = false;
/** Groupe actif pour les graphiques overlay (0 = moyenne, 1 = médiane, 2 = Q1, 3 = Q3) ; synchronisé sur les 3 onglets. */
let overlayActiveGroup = 0;
const OVERLAY_CHART_KEYS = ["overlay-prix", "overlay-surface", "overlay-prixm2"];

/** Configs datasets pour buildChartOverlayMulti (un lieu ou N lieux). group: 0 = moyenne, 1 = médiane, 2 = Q1, 3 = Q3. */
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
  const list = regionId ? getDepartmentsForRegion(regionId) : (geo.departements || []);
  deptSelect.innerHTML = "<option value=''>— Choisir un département —</option>";
  list.forEach((d) => {
    deptSelect.innerHTML += `<option value="${escapeHtml(d)}">${escapeHtml(d)}</option>`;
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
  const deptLibelle = DEPT_NAMES[code_dept] ? `${code_dept} ${DEPT_NAMES[code_dept]}` : code_dept;
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

/** Couleurs pour les 4 courbes (moyenne, médiane, Q1, Q3) — Lieu 1 = foncé, Lieu 2 = clair + pointillés */
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
      layout: { padding: { left: 14 } },
      interaction: { mode: "index", intersect: false },
      scales: {
        y: { ...yScale, ticks: { padding: 10 } },
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
      layout: { padding: { left: 14 } },
      interaction: { mode: "index", intersect: false },
      scales: { y: { ...yScale, ticks: { padding: 10 } } },
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
      layout: { padding: { left: 14 } },
      interaction: { mode: "index", intersect: false },
      scales: { y: { ...yScale, ticks: { padding: 10 } } },
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
  const res = await fetch(`${API_BASE}/api/geo`);
  if (!res.ok) throw new Error("Erreur chargement geo");
  geo = await res.json();
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
  const res = await fetch(`${API_BASE}/api/period`);
  if (!res.ok) return;
  const p = await res.json();
  document.getElementById("annee-min").value = p.annee_min ?? "";
  document.getElementById("annee-max").value = p.annee_max ?? "";
  updatePeriodConstraints();
}

const COMMUNES_CHUNK_SIZE = 1500;

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
  if (suffix === "1") return "commune-1-select";
  if (suffix === "2") return "commune-2-select";
  if (suffix === "3") return "commune-3-select";
  if (suffix === "4") return "commune-4-select";
  return "commune-select";
}

async function loadCommunes(codeDept, suffix) {
  const selectId = getCommuneSelectId(suffix);
  const communeSelect = document.getElementById(selectId);
  if (!communeSelect) return;
  communeSelect.innerHTML = "<option value=''>Chargement…</option>";
  const url = codeDept
    ? `${API_BASE}/api/communes?code_dept=${encodeURIComponent(codeDept)}`
    : `${API_BASE}/api/communes`;
  const res = await fetch(url);
  if (!res.ok) {
    communeSelect.innerHTML = "<option value=''>Erreur</option>";
    return;
  }
  const list = await res.json();
  if (suffix === "2") communesList2 = list;
  else if (suffix === "3") communesList3 = list;
  else if (suffix === "4") communesList4 = list;
  else {
    communesList = list;
    if (!codeDept) allCommunesList = list.slice(0);
  }
  communeSelect.innerHTML = "<option value=''>— Choisir une commune —</option>";
  if (list.length <= COMMUNES_CHUNK_SIZE) {
    list.forEach((c, i) => {
      const opt = document.createElement("option");
      opt.value = i;
      opt.textContent = `${c.commune} (${c.code_postal})`;
      communeSelect.appendChild(opt);
    });
  } else {
    fillCommuneOptionsChunked(communeSelect, list, 0);
  }
}

function restoreAllCommunesInDropdown(suffix) {
  const list = allCommunesList.slice(0);
  const selectId = getCommuneSelectId(suffix || "");
  const communeSelect = document.getElementById(selectId);
  if (!communeSelect) return;
  if (suffix === "2") communesList2 = list;
  else if (suffix === "3") communesList3 = list;
  else if (suffix === "4") communesList4 = list;
  else communesList = list;
  communeSelect.innerHTML = "<option value=''>— Choisir une commune —</option>";
  if (list.length <= COMMUNES_CHUNK_SIZE) {
    list.forEach((c, i) => {
      const opt = document.createElement("option");
      opt.value = i;
      opt.textContent = `${c.commune} (${c.code_postal})`;
      communeSelect.appendChild(opt);
    });
  } else {
    fillCommuneOptionsChunked(communeSelect, list, 0);
  }
}

function loadCommunesInBackground() {
  const communeSelect = document.getElementById("commune-select");
  if (allCommunesList.length > 0) return;
  communeSelect.innerHTML = "<option value=''>Chargement des communes en arrière-plan…</option>";
  fetch(`${API_BASE}/api/communes`)
    .then((res) => {
      if (!res.ok) throw new Error("Erreur chargement communes");
      return res.json();
    })
    .then((list) => {
      allCommunesList = list;
      communesList = list.slice(0);
      if (document.getElementById("dept-select").value) return;
      const firstOpt = communeSelect.options[0];
      if (!firstOpt || firstOpt.text !== "Chargement des communes en arrière-plan…") return;
      communeSelect.innerHTML = "<option value=''>— Choisir une commune —</option>";
      if (list.length <= COMMUNES_CHUNK_SIZE) {
        list.forEach((c, i) => {
          const opt = document.createElement("option");
          opt.value = i;
          opt.textContent = `${c.commune} (${c.code_postal})`;
          communeSelect.appendChild(opt);
        });
      } else {
        fillCommuneOptionsChunked(communeSelect, list, 0);
      }
    })
    .catch(() => {
      if (communeSelect.options[0]?.text === "Chargement des communes en arrière-plan…") {
        communeSelect.innerHTML = "<option value=''>Erreur chargement</option>";
      }
    });
}

function escapeHtml(s) {
  if (s == null) return "";
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

function getCommuneValue() {
  const sel = document.getElementById("commune-select");
  const v = sel?.value;
  if (v === "" || v === undefined || !communesList.length)
    return { code_dept: null, code_postal: null, commune: null };
  const i = parseInt(v, 10);
  if (Number.isNaN(i) || i < 0 || i >= communesList.length) return { code_dept: null, code_postal: null, commune: null };
  const c = communesList[i];
  return { code_dept: c.code_dept, code_postal: c.code_postal, commune: c.commune };
}

function getCommuneValueForPlace(place) {
  const selectId = getCommuneSelectId(String(place));
  const list = place === 1 ? communesList : place === 2 ? communesList2 : place === 3 ? communesList3 : communesList4;
  const sel = document.getElementById(selectId);
  const v = sel?.value;
  if (v === "" || v === undefined || !list.length) return { code_dept: null, code_postal: null, commune: null };
  const i = parseInt(v, 10);
  if (Number.isNaN(i) || i < 0 || i >= list.length) return { code_dept: null, code_postal: null, commune: null };
  const c = list[i];
  return { code_dept: c.code_dept, code_postal: c.code_postal, commune: c.commune };
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
  const sel = document.getElementById("commune-select");
  const sel1 = document.getElementById("commune-1-select");
  if (sel && sel1) {
    sel1.innerHTML = sel.innerHTML;
    sel1.value = sel.value;
  }
  communesList = allCommunesList.length ? allCommunesList.slice(0) : communesList.slice(0);
}

function syncPlace1ToSingle() {
  document.getElementById("region-select").value = document.getElementById("region-1-select").value || "";
  document.getElementById("dept-select").value = document.getElementById("dept-1-select").value || "";
  const sel1 = document.getElementById("commune-1-select");
  const sel = document.getElementById("commune-select");
  if (sel1 && sel) {
    sel.innerHTML = sel1.innerHTML;
    sel.value = sel1.value;
  }
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
    switchLieuTab(1);
    compareBtn.textContent = "Quitter la comparaison";
    content.setAttribute("aria-hidden", "true");
    compareResults.setAttribute("aria-hidden", "true");
    overlayResults.setAttribute("aria-hidden", "true");
    overlayMode = false;
    if (overlayBtn) overlayBtn.style.display = "block";
    if (overlayBtn) overlayBtn.textContent = "Superposer les graphiques";
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

function buildNiveauAndParams(p, type_local, surface_cat, pieces_cat, annee_min, annee_max) {
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
    ...(surface_cat && { surface_cat }),
    ...(pieces_cat && { pieces_cat }),
    ...(annee_min && { annee_min }),
    ...(annee_max && { annee_max }),
  });
  return { niveau, region_id_for_api, code_dept, params };
}

async function submitStats() {
  const type_local = document.getElementById("type-local").value || undefined;
  const surface_cat = document.getElementById("surface-cat").value || undefined;
  const pieces_cat = document.getElementById("pieces-cat").value || undefined;
  const annee_min = document.getElementById("annee-min").value || undefined;
  const annee_max = document.getElementById("annee-max").value || undefined;

  if (compareMode) {
    const places = [];
    const builds = [];
    for (let i = 1; i <= comparePlaceCount; i++) {
      const p = getPlaceValues(i);
      const b = buildNiveauAndParams(p, type_local, surface_cat, pieces_cat, annee_min, annee_max);
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
      const responses = await Promise.all(builds.map((b) => fetch(`${API_BASE}/api/stats?${b.params}`)));
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
        setCompareChartsYToZero();
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
  const built = buildNiveauAndParams(p, type_local, surface_cat, pieces_cat, annee_min, annee_max);
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

function applyYAxisToChart(chartKey, minValue) {
  const ch = charts[chartKey];
  if (!ch) return;
  const y = ch.options.scales.y;
  if (minValue == null) {
    y.min = undefined;
    y.beginAtZero = false;
  } else {
    const v = Number(minValue);
    y.min = Number.isNaN(v) ? 0 : v;
    y.beginAtZero = v === 0;
  }
  ch.update();
}

/** En mode comparaison : fixe Y=0 pour tous les graphiques et met à jour les contrôles (évite comparaisons biaisées). */
function setCompareChartsYToZero() {
  const keys = getCompareChartKeys();
  keys.forEach((key) => applyYAxisToChart(key, 0));
  document.querySelectorAll("#stats-compare-results .chart-y-axis-ctrl").forEach((ctrl) => {
    ctrl.classList.add("fixed");
    const btn = ctrl.querySelector(".chart-y-axis-btn");
    if (btn) btn.classList.add("fixed");
    const input = ctrl.querySelector(".chart-y-axis-min");
    if (input) input.value = "0";
  });
}

document.getElementById("region-select").addEventListener("change", () => {
  const regionId = document.getElementById("region-select").value;
  fillDepartmentSelect(regionId || null);
  document.getElementById("dept-select").value = "";
  restoreAllCommunesInDropdown();
});

document.getElementById("dept-select").addEventListener("change", () => {
  const codeDept = document.getElementById("dept-select").value;
  if (!codeDept) {
    restoreAllCommunesInDropdown();
    return;
  }
  const inferredRegion = getRegionForDepartment(codeDept);
  if (inferredRegion) document.getElementById("region-select").value = inferredRegion;
  loadCommunes(codeDept);
});

document.getElementById("commune-select").addEventListener("change", () => {
  const c = getCommuneValue();
  if (c.code_dept && (c.commune || c.code_postal)) {
    document.getElementById("dept-select").value = c.code_dept;
    const regionId = getRegionForDepartment(c.code_dept);
    if (regionId) document.getElementById("region-select").value = regionId;
  }
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
  });
  document.getElementById(deptId)?.addEventListener("change", function () {
    const codeDept = this.value;
    if (!codeDept) {
      restoreAllCommunesInDropdown(s);
      return;
    }
    const inferredRegion = getRegionForDepartment(codeDept);
    if (inferredRegion) document.getElementById(regionId).value = inferredRegion;
    loadCommunes(codeDept, s);
  });
  document.getElementById(communeId)?.addEventListener("change", function () {
    const c = getCommuneValueForPlace(place);
    if (c.code_dept && (c.commune || c.code_postal)) {
      document.getElementById(deptId).value = c.code_dept;
      const regionIdVal = getRegionForDepartment(c.code_dept);
      if (regionIdVal) document.getElementById(regionId).value = regionIdVal;
    }
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
    const selFrom = document.getElementById("commune-" + fromId + "-select");
    const selTo = document.getElementById("commune-" + i + "-select");
    if (selFrom && selTo) { selTo.innerHTML = selFrom.innerHTML; selTo.value = selFrom.value; }
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
  document.getElementById("commune-" + comparePlaceCount + "-select").innerHTML = "<option value=''>— Choisir une commune —</option>";
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
    setCompareChartsYToZero();
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
    }, 220);
  }, 4000);
}

function updatePeriodConstraints() {
  const minEl = document.getElementById("annee-min");
  const maxEl = document.getElementById("annee-max");
  const minVal = minEl.value.trim();
  const maxVal = maxEl.value.trim();
  const minNum = minVal ? parseInt(minVal, 10) : NaN;
  const maxNum = maxVal ? parseInt(maxVal, 10) : NaN;
  minEl.max = !Number.isNaN(maxNum) ? String(maxNum) : "2030";
  maxEl.min = !Number.isNaN(minNum) ? String(minNum) : "2000";
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
});

document.getElementById("annee-max").addEventListener("input", updatePeriodConstraints);
document.getElementById("annee-max").addEventListener("change", () => {
  const r = validatePeriodBounds();
  if (r) {
    document.getElementById("annee-max").value = r.min;
    showPeriodToast("La période est incohérente, elle a été corrigée automatiquement.");
  }
  updatePeriodConstraints();
});

document.getElementById("stats-reset-btn").addEventListener("click", () => {
  document.getElementById("region-select").value = "";
  document.getElementById("dept-select").value = "";
  document.getElementById("commune-select").value = "";
  fillDepartmentSelect(null);
  restoreAllCommunesInDropdown();
  if (compareMode) {
    for (let i = 1; i <= MAX_COMPARE_PLACES; i++) {
      document.getElementById("region-" + i + "-select").value = "";
      document.getElementById("dept-" + i + "-select").value = "";
      document.getElementById("commune-" + i + "-select").value = "";
      fillDepartmentSelect(null, "dept-" + i + "-select");
      restoreAllCommunesInDropdown(String(i));
    }
    comparePlaceCount = 2;
    updateComparePlaceUI();
    switchLieuTab(1);
  }
  document.getElementById("type-local").value = "";
  document.getElementById("surface-cat").value = "";
  document.getElementById("pieces-cat").value = "";
  document.getElementById("annee-min").value = "";
  document.getElementById("annee-max").value = "";
  document.querySelector(".stats-tabs")?.classList.remove("single-year");
  document.querySelectorAll("[id^='stats-result-'] .stats-tabs").forEach((el) => el?.classList.remove("single-year"));
  switchTab("prix");
  updatePeriodConstraints();
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
        const otherContainer = container.id === "stats-result-1" ? document.getElementById("stats-result-2") : document.getElementById("stats-result-1");
        if (otherContainer) {
          const otherTabId = tabId.endsWith("-1") ? tabId.replace(/-1$/, "-2") : tabId.replace(/-2$/, "-1");
          switchTabInContainer(otherContainer, otherTabId);
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
    ctrl.classList.remove("fixed");
    btn.classList.remove("fixed");
    applyYAxisToChart(chartKey, null);
    if (isCompareY) {
      const keysSameMetric = getCompareChartKeysForMetric(chartKey);
      charts[chartKey].update();
      const computedMin = charts[chartKey].scales?.y?.min;
      const minVal = typeof computedMin === "number" && !Number.isNaN(computedMin) ? computedMin : 0;
      keysSameMetric.forEach((key) => applyYAxisToChart(key, minVal));
      keysSameMetric.forEach((key) => {
        const c = document.querySelector("#stats-compare-results .chart-y-axis-ctrl[data-chart=\"" + key + "\"]");
        if (c) {
          c.classList.add("fixed");
          const b = c.querySelector(".chart-y-axis-btn");
          if (b) b.classList.add("fixed");
          const inp = c.querySelector(".chart-y-axis-min");
          if (inp) inp.value = String(minVal);
        }
      });
    }
  } else {
    const input = ctrl.querySelector(".chart-y-axis-min");
    const v = parseFloat(input.value, 10) || 0;
    ctrl.classList.add("fixed");
    btn.classList.add("fixed");
    applyYAxisToChart(chartKey, v);
    if (isCompareY) {
      const keysSameMetric = getCompareChartKeysForMetric(chartKey);
      keysSameMetric.forEach((key) => applyYAxisToChart(key, v));
      keysSameMetric.forEach((key) => {
        const c = document.querySelector("#stats-compare-results .chart-y-axis-ctrl[data-chart=\"" + key + "\"]");
        if (c) {
          c.classList.add("fixed");
          const b = c.querySelector(".chart-y-axis-btn");
          if (b) b.classList.add("fixed");
          const inp = c.querySelector(".chart-y-axis-min");
          if (inp) inp.value = String(v);
        }
      });
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
  const val = Number.isNaN(v) ? 0 : v;
  applyYAxisToChart(chartKey, val);
  if (compareMode && document.getElementById("stats-compare-results")?.contains(ctrl)) {
    const keysSameMetric = getCompareChartKeysForMetric(chartKey);
    keysSameMetric.forEach((key) => applyYAxisToChart(key, val));
    keysSameMetric.forEach((key) => {
      if (key === chartKey) return;
      const c = document.querySelector("#stats-compare-results .chart-y-axis-ctrl[data-chart=\"" + key + "\"]");
      const inp = c?.querySelector(".chart-y-axis-min");
      if (inp) inp.value = String(val);
    });
  }
});

loadGeo()
  .then(loadPeriod)
  .then(() => {
    setTimeout(() => loadCommunesInBackground(), 0);
  })
  .catch((e) => console.error(e));
