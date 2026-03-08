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
let communesList = []; // liste des communes affichées (tout ou filtrée par département)
let allCommunesList = []; // copie de la liste complète pour restaurer quand on désélectionne le département
let charts = { prix: null, surface: null, prixm2: null };

function getDepartmentsForRegion(regionId) {
  const reg = geo.regions.find((r) => r.id === regionId);
  return reg ? reg.departements || [] : [];
}

function getRegionForDepartment(codeDept) {
  const reg = geo.regions.find((r) => r.departements && r.departements.includes(codeDept));
  return reg ? reg.id : null;
}

function fillDepartmentSelect(regionId) {
  const deptSelect = document.getElementById("dept-select");
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

/** Couleurs pour les 4 courbes (moyenne, médiane, Q1, Q3) */
const CHART_COLORS = [
  { border: "#2563eb", fill: "rgba(37, 99, 235, 0.1)" },
  { border: "#059669", fill: "rgba(5, 150, 105, 0.1)" },
  { border: "#d97706", fill: "rgba(217, 119, 6, 0.1)" },
  { border: "#7c3aed", fill: "rgba(124, 58, 237, 0.1)" },
];

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
          },
        },
      },
    },
  });
}

async function loadGeo() {
  const res = await fetch(`${API_BASE}/api/geo`);
  if (!res.ok) throw new Error("Erreur chargement geo");
  geo = await res.json();
  const regionSelect = document.getElementById("region-select");
  regionSelect.innerHTML = "<option value=''>— Choisir une région —</option>";
  geo.regions.forEach((r) => {
    regionSelect.innerHTML += `<option value="${r.id}">${r.nom}</option>`;
  });
  fillDepartmentSelect(null);
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

async function loadCommunes(codeDept) {
  const communeSelect = document.getElementById("commune-select");
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
  communesList = list;
  if (!codeDept) allCommunesList = list.slice(0);
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

function restoreAllCommunesInDropdown() {
  communesList = allCommunesList.slice(0);
  const communeSelect = document.getElementById("commune-select");
  communeSelect.innerHTML = "<option value=''>— Choisir une commune —</option>";
  if (communesList.length <= COMMUNES_CHUNK_SIZE) {
    communesList.forEach((c, i) => {
      const opt = document.createElement("option");
      opt.value = i;
      opt.textContent = `${c.commune} (${c.code_postal})`;
      communeSelect.appendChild(opt);
    });
  } else {
    fillCommuneOptionsChunked(communeSelect, communesList, 0);
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
  const v = sel.value;
  if (v === "" || v === undefined || !communesList.length)
    return { code_dept: null, code_postal: null, commune: null };
  const i = parseInt(v, 10);
  if (Number.isNaN(i) || i < 0 || i >= communesList.length) return { code_dept: null, code_postal: null, commune: null };
  const c = communesList[i];
  return { code_dept: c.code_dept, code_postal: c.code_postal, commune: c.commune };
}

async function submitStats() {
  const region_id = document.getElementById("region-select").value || null;
  let code_dept = document.getElementById("dept-select").value || null;
  const c = getCommuneValue();
  const code_postal = c.code_postal || null;
  const commune = c.commune || null;
  if (c.code_dept) code_dept = c.code_dept;

  let niveau;
  if (commune && code_postal && code_dept) {
    niveau = "commune";
  } else if (code_dept) {
    niveau = "department";
  } else if (region_id) {
    niveau = "region";
  } else {
    alert("Choisissez au moins une région, un département ou une commune.");
    return;
  }

  const region_id_for_api = niveau === "region" ? region_id : (region_id || getRegionForDepartment(code_dept));
  const type_local = document.getElementById("type-local").value || undefined;
  const surface_cat = document.getElementById("surface-cat").value || undefined;
  const pieces_cat = document.getElementById("pieces-cat").value || undefined;
  const annee_min = document.getElementById("annee-min").value || undefined;
  const annee_max = document.getElementById("annee-max").value || undefined;

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
  document.getElementById("stats-loading").setAttribute("aria-hidden", "false");
  document.getElementById("stats-empty").style.display = "none";
  document.getElementById("stats-content").setAttribute("aria-hidden", "true");
  destroyCharts();
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
    const regionNom = region_id_for_api
      ? geo.regions.find((r) => r.id === region_id_for_api)?.nom
      : (code_dept ? geo.regions.find((r) => r.departements && r.departements.includes(code_dept))?.nom : null);
    const deptLibelle = DEPT_NAMES[code_dept] ? `${code_dept} ${DEPT_NAMES[code_dept]}` : code_dept;
    let titre;
    if (niveau === "region") {
      titre = regionNom || "Région";
    } else if (niveau === "department") {
      titre = regionNom ? `${regionNom} / ${deptLibelle}` : deptLibelle;
    } else {
      const postaux = communesList
        .filter((c) => c.commune === commune && c.code_dept === code_dept)
        .map((c) => c.code_postal)
        .filter((v, i, a) => a.indexOf(v) === i)
        .sort();
      const base = regionNom ? `${regionNom} / ${deptLibelle}` : deptLibelle;
      titre = `${base} / ${commune} (${postaux.join(", ")})`;
    }
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
      buildChartMulti(
        "chart-prix",
        series,
        [
          { key: "prix_moyen", label: "Prix moyen (€)" },
          { key: "prix_median", label: "Prix médian (€)" },
          { key: "prix_q1", label: "Prix Q1 (€)" },
          { key: "prix_q3", label: "Prix Q3 (€)" },
        ],
        yMinDefault
      );
      buildChartMulti(
        "chart-surface",
        series,
        [
          { key: "surface_moyenne", label: "Surface moyenne (m²)" },
          { key: "surface_mediane", label: "Surface médiane (m²)" },
        ],
        yMinDefault
      );
      buildChartMulti(
        "chart-prixm2",
        series,
        [
          { key: "prix_m2_moyenne", label: "Prix/m² moyen (€)" },
          { key: "prix_m2_mediane", label: "Prix/m² médian (€)" },
          { key: "prix_m2_q1", label: "Prix/m² Q1 (€)" },
          { key: "prix_m2_q3", label: "Prix/m² Q3 (€)" },
        ],
        yMinDefault
      );
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
  document.getElementById("type-local").value = "";
  document.getElementById("surface-cat").value = "";
  document.getElementById("pieces-cat").value = "";
  document.getElementById("annee-min").value = "";
  document.getElementById("annee-max").value = "";
  document.querySelector(".stats-tabs")?.classList.remove("single-year");
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
  btn.addEventListener("click", () => switchTab(btn.getAttribute("data-tab")));
});

document.getElementById("stats-content").addEventListener("click", (e) => {
  const btn = e.target.closest(".chart-y-axis-btn");
  if (!btn) return;
  const ctrl = btn.closest(".chart-y-axis-ctrl");
  if (!ctrl) return;
  const chartKey = ctrl.getAttribute("data-chart");
  if (!chartKey || !charts[chartKey]) return;
  e.preventDefault();
  const isFixed = ctrl.classList.contains("fixed");
  if (isFixed) {
    ctrl.classList.remove("fixed");
    btn.classList.remove("fixed");
    applyYAxisToChart(chartKey, null);
  } else {
    const input = ctrl.querySelector(".chart-y-axis-min");
    const v = parseFloat(input.value, 10) || 0;
    ctrl.classList.add("fixed");
    btn.classList.add("fixed");
    applyYAxisToChart(chartKey, v);
  }
});

document.getElementById("stats-content").addEventListener("input", (e) => {
  if (!e.target.classList.contains("chart-y-axis-min")) return;
  const ctrl = e.target.closest(".chart-y-axis-ctrl");
  if (!ctrl || !ctrl.classList.contains("fixed")) return;
  const chartKey = ctrl.getAttribute("data-chart");
  if (!chartKey || !charts[chartKey]) return;
  const v = parseFloat(e.target.value, 10);
  applyYAxisToChart(chartKey, Number.isNaN(v) ? 0 : v);
});

loadGeo()
  .then(loadPeriod)
  .then(() => {
    setTimeout(() => loadCommunesInBackground(), 0);
  })
  .catch((e) => console.error(e));
