/**
 * Tableaux comparaison : rendu HTML, tri, pagination, fetch batch POST, distances sur lignes.
 */
import { S } from "./comparaison_state.js";
import {
  escapeHtml,
  normalizeDeptCodeTwoChars,
  normalizeNameCanonical,
  normalizeCommuneNameForMapMatch,
  formatPct,
  formatNbLocaux,
  getScoreLabel,
  getRegionForDepartment
} from "./comparaison_utils.js";
import {
  rentaKeyPairForTypeLogt,
  isRentabiliteTypeSupported,
  resolveFullRentabiliteScoreKey,
  resolveNbLocauxKey
} from "./comparaison_renta_keys.js";

function getTableKeyMapForTypeCode(typeCode, cat, typeSurf, nbPieces) {
  if (cat !== "rentabilite") return S.TABLE_BLOCKS[0].keyMap;
  var surf = String(typeSurf || "TOUTES").toUpperCase();
  var pie = String(nbPieces || "TOUS").toUpperCase();
  var hasSurf = surf !== "TOUTES" && surf !== "" && /^S[1-5]$/.test(surf);
  var hasPie = pie !== "TOUS" && pie !== "" && /^T[1-5]$/.test(pie);
  if (hasSurf || hasPie) {
    return {
      renta_brute: resolveFullRentabiliteScoreKey("renta_brute", typeCode, typeSurf, nbPieces, cat),
      renta_nette: resolveFullRentabiliteScoreKey("renta_nette", typeCode, typeSurf, nbPieces, cat),
      taux_tfb: "taux_tfb",
      taux_teom: "taux_teom"
    };
  }
  var rp = rentaKeyPairForTypeLogt(typeCode);
  if (rp) {
    return {
      renta_brute: rp.rb,
      renta_nette: rp.rn,
      taux_tfb: "taux_tfb",
      taux_teom: "taux_teom"
    };
  }
  if (!isRentabiliteTypeSupported(typeCode)) {
    return {
      renta_brute: S.RENTA_UNAVAILABLE_KEY,
      renta_nette: S.RENTA_UNAVAILABLE_KEY,
      taux_tfb: "taux_tfb",
      taux_teom: "taux_teom"
    };
  }
  var u = String(typeCode || "TOUS").toUpperCase();
  if (u === "MAISON") return S.TABLE_BLOCKS[1].keyMap;
  if (u === "APPART") return S.TABLE_BLOCKS[2].keyMap;
  return S.TABLE_BLOCKS[0].keyMap;
}

function computeTableIndicatorsForJob(job, cat, displayIndicators) {
  var fkParts = (job.filterKey || "").split(S.FILTER_KEY_SEP);
  var keyMap = getTableKeyMapForTypeCode(job.typeCodeForColumns, cat, fkParts[1], fkParts[2]);
  var tableIndicators = [];
  displayIndicators.forEach(function (ind) {
    var tableKey = keyMap[ind.key];
    if (tableKey != null) {
      tableIndicators.push({ key: tableKey, label: getScoreLabel(ind.key) });
    }
  });
  if (tableIndicators.length === 0 && displayIndicators.length > 0) {
    var k0 = displayIndicators[0].key;
    var tk0 = keyMap[k0] != null ? keyMap[k0] : k0;
    tableIndicators = [{ key: tk0, label: getScoreLabel(k0) }];
  }
  return tableIndicators;
}

function rowHasNumericIndicator(row, tableIndicators) {
  if (row.nb_locaux != null && row.nb_locaux !== "" && Number.isFinite(Number(row.nb_locaux))) return true;
  if (row.nb_locaux_maisons != null && Number.isFinite(Number(row.nb_locaux_maisons))) return true;
  if (row.nb_locaux_appts != null && Number.isFinite(Number(row.nb_locaux_appts))) return true;
  for (var i = 0; i < tableIndicators.length; i++) {
    var k = tableIndicators[i].key;
    if (k === S.RENTA_UNAVAILABLE_KEY) continue;
    var v = row[k];
    if (v != null && v !== "" && Number.isFinite(Number(v))) return true;
  }
  return false;
}

/** Colonnes score « tableau » (sans raccourci nb_locaux) — pour l’éligibilité cartes. */
function rowHasNumericScoreInTableColumns(row, tableIndicators) {
  for (var i = 0; i < tableIndicators.length; i++) {
    var k = tableIndicators[i].key;
    if (k === S.RENTA_UNAVAILABLE_KEY) continue;
    var v = row[k];
    if (v != null && v !== "" && Number.isFinite(Number(v))) return true;
  }
  return false;
}

export function jobHasDisplayableData(rows, job, cat, displayIndicators) {
  if (!rows || !rows.length) return false;
  var tableIndicators = computeTableIndicatorsForJob(job, cat, displayIndicators);
  if (!tableIndicators.length) return false;
  // Au moins une ligne doit avoir un indicateur score non-NULL
  // (on ignore les raccourcis nb_locaux globaux qui sont presque toujours renseignés)
  return rows.some(function (r) {
    return rowHasNumericScoreInTableColumns(r, tableIndicators);
  });
}

/** Colonnes score pour cartes : alignées sur le tableau (pas seulement job.scoreKey). */
function getMapScoreColumnKeysForJob(job, cat, displayIndicators) {
  var keys;
  if (!displayIndicators || !displayIndicators.length) {
    keys = job.scoreKey && job.scoreKey !== S.RENTA_UNAVAILABLE_KEY ? [job.scoreKey] : [];
  } else {
    var tableIndicators = computeTableIndicatorsForJob(job, cat, displayIndicators);
    keys = tableIndicators
      .map(function (ti) {
        return ti.key;
      })
      .filter(function (k) {
        return k && k !== S.RENTA_UNAVAILABLE_KEY && k.indexOf("nb_locaux") !== 0;
      });
  }
  /* Même repli que getEffectiveMapScoreKeyForJob : si le keyMap ne donne que des colonnes
   * indisponibles pour ce job, on garde job.scoreKey pour ne pas vider toutes les cartes. */
  if (keys.length) return keys;
  return job.scoreKey && job.scoreKey !== S.RENTA_UNAVAILABLE_KEY ? [job.scoreKey] : [];
}

/**
 * Colonne utilisée pour la choroplèthe / cartes communales : 1er indicateur score du tableau pour ce job.
 */
export function getEffectiveMapScoreKeyForJob(job, cat, displayIndicators) {
  var keys = getMapScoreColumnKeysForJob(job, cat, displayIndicators);
  return keys.length ? keys[0] : job.scoreKey;
}

/**
 * Au moins une valeur numérique exploitable pour une carte : mêmes colonnes score que le tableau
 * (computeTableIndicatorsForJob), sans compter le raccourci nb_locaux de rowHasNumericIndicator.
 * Sinon repli sur getMapScoreColumnKeysForJob (ex. displayIndicators vide).
 */
export function jobHasMapDisplayableData(rows, job, cat, displayIndicators) {
  if (!job || !rows || !rows.length) return false;
  var tableIndicators = computeTableIndicatorsForJob(job, cat, displayIndicators);
  if (tableIndicators.length) {
    return rows.some(function (r) {
      return rowHasNumericScoreInTableColumns(r, tableIndicators);
    });
  }
  var keys = getMapScoreColumnKeysForJob(job, cat, displayIndicators);
  if (!keys.length) return false;
  return rows.some(function (r) {
    return keys.some(function (k) {
      var v = r[k];
      if (v == null || v === "") return false;
      return Number.isFinite(Number(v));
    });
  });
}

export function sortRows(rows, key, desc) {
  var r = rows.slice();
  r.sort(function (a, b) {
    var va = a[key];
    var vb = b[key];
    if (va == null || va === "") return 1;
    if (vb == null || vb === "") return -1;
    var na = Number.isFinite(Number(va)) ? Number(va) : null;
    var nb = Number.isFinite(Number(vb)) ? Number(vb) : null;
    if (na !== null && nb !== null) {
      if (na !== nb) return desc ? nb - na : na - nb;
      return 0;
    }
    var sa = String(va).toLowerCase();
    var sb = String(vb).toLowerCase();
    var cmp = sa.localeCompare(sb, "fr", { sensitivity: "base" });
    return desc ? -cmp : cmp;
  });
  return r;
}

export function getDisplayModeFromRows(rows) {
  if (!rows.length) return "communes";
  var r = rows[0];
  if (r.mode === "departement") return "departements";
  if (r.mode === "region") return "regions";
  return "communes";
}

/** Fusion des lignes par zone géo pour le cadrage choroplèthe / cartes. */
export function mergeRowsForBounds(rowsByFilterKey, jobs) {
  var seen = new Set();
  var out = [];
  (jobs || []).forEach(function (job) {
    (rowsByFilterKey[job.filterKey] || []).forEach(function (r) {
      var mode = getDisplayModeFromRows([r]);
      var key =
        mode === "regions"
          ? "r:" + String(r.code_region || "")
          : mode === "departements"
          ? "d:" + String(r.code_dept || "")
          : "c:" +
            normalizeDeptCodeTwoChars(r.code_dept) +
            "|" +
            String(r.code_postal || "") +
            "|" +
            normalizeCommuneNameForMapMatch(r.commune || r.nom_commune || "");
      if (seen.has(key)) return;
      seen.add(key);
      out.push(r);
    });
  });
  return out;
}

function renderTablePage(tbodyEl, sorted, indicators, displayMode, page) {
  var start = page * S.TABLE_PAGE_SIZE;
  var pageRows = sorted.slice(start, start + S.TABLE_PAGE_SIZE);
  tbodyEl.innerHTML = pageRows.map(function (row, j) {
    var rank = start + j + 1;
    var region = row.region || row.reg_nom || "—";
    var cells = indicators.map(function (ind) {
      var v = row[ind.key];
      if (ind.key === "nb_locaux" || ind.key.indexOf("nb_locaux_") === 0) {
        return "<td class=\"col-num\">" + formatNbLocaux(v) + "</td>";
      }
      if (ind.key === "_distance_km") {
        var dk = row._distance_km;
        var sk =
          dk != null && Number.isFinite(Number(dk))
            ? Number(dk).toLocaleString("fr-FR", { minimumFractionDigits: 1, maximumFractionDigits: 1 })
            : "—";
        return "<td class=\"col-num\">" + sk + "</td>";
      }
      if (ind.key === "_duree_minutes") {
        return "<td class=\"col-num\">" + formatDurationMinutesCell(row._duree_minutes) + "</td>";
      }
      var hasNum = v != null && v !== "" && Number.isFinite(Number(v));
      var s = hasNum ? formatPct(Number(v)) : "—";
      return "<td class=\"col-num\">" + s + "</td>";
    });
    if (displayMode === "departements") {
      var codeDeptR = row.code_dept || "—";
      var depNom = row.dep_nom || codeDeptR;
      return "<tr><td class=\"col-num\">" + rank + "</td><td>" + escapeHtml(region) + "</td><td class=\"col-num\">" + escapeHtml(codeDeptR) + "</td><td>" + escapeHtml(depNom) + "</td>" + cells.join("") + "</tr>";
    }
    if (displayMode === "regions") {
      var codeReg = row.code_region || "—";
      return "<tr><td class=\"col-num\">" + rank + "</td><td class=\"col-num\">" + escapeHtml(codeReg) + "</td><td>" + escapeHtml(region) + "</td>" + cells.join("") + "</tr>";
    }
    var commune = row.commune || row.nom_commune || "—";
    var codeDeptR2 = row.code_dept || "—";
    var codePostalR = row.code_postal || row.code_postal_principal || "—";
    var link = (row.code_dept && row.code_postal && row.commune)
      ? "fiche_commune.html?code_dept=" + encodeURIComponent(row.code_dept) + "&code_postal=" + encodeURIComponent(row.code_postal) + "&commune=" + encodeURIComponent(row.commune)
      : "#";
    var cellCommune = link !== "#" ? "<a class=\"commune-link\" href=\"" + escapeHtml(link) + "\">" + escapeHtml(commune) + "</a>" : escapeHtml(commune);
    return "<tr><td class=\"col-num\">" + rank + "</td><td>" + escapeHtml(region) + "</td><td class=\"col-num\">" + escapeHtml(codeDeptR2) + "</td><td class=\"col-num\">" + escapeHtml(codePostalR) + "</td><td>" + cellCommune + "</td>" + cells.join("") + "</tr>";
  }).join("");
}

function renderPaginationBar(pagBarEl, sorted, page, onPageChange) {
  var total = sorted.length;
  var nbPages = Math.ceil(total / S.TABLE_PAGE_SIZE);
  if (nbPages <= 1) { pagBarEl.hidden = true; return; }
  pagBarEl.hidden = false;
  var start = page * S.TABLE_PAGE_SIZE + 1;
  var end = Math.min((page + 1) * S.TABLE_PAGE_SIZE, total);
  var html = "";
  if (nbPages > 2) {
    html += "<button data-p=\"first\" " + (page === 0 ? "disabled" : "") + ">Page\u00a01</button>";
    html += "<button data-p=\"back10\" " + (page < 10 ? "disabled" : "") + ">&laquo;</button>";
  }
  html += "<button data-p=\"prev\" " + (page === 0 ? "disabled" : "") + ">&#8592; Préc.</button>";
  html += "<span class=\"pag-info\">" + start + "–" + end + " sur " + total + " (page " + (page + 1) + "/" + nbPages + ")</span>";
  html += "<button data-p=\"next\" " + (page >= nbPages - 1 ? "disabled" : "") + ">Suiv. &#8594;</button>";
  if (nbPages > 2) {
    html += "<button data-p=\"fwd10\" " + (page >= nbPages - 10 ? "disabled" : "") + ">&raquo;</button>";
    html += "<button data-p=\"last\" " + (page >= nbPages - 1 ? "disabled" : "") + ">Page\u00a0" + nbPages + "</button>";
  }
  pagBarEl.innerHTML = html;
  pagBarEl.onclick = function (e) {
    var btn = e.target.closest("button[data-p]");
    if (!btn || btn.disabled) return;
    var action = btn.getAttribute("data-p");
    var newPage = page;
    if (action === "first") newPage = 0;
    else if (action === "back10") newPage = Math.max(0, page - 10);
    else if (action === "prev") newPage = page - 1;
    else if (action === "next") newPage = page + 1;
    else if (action === "fwd10") newPage = Math.min(nbPages - 1, page + 10);
    else if (action === "last") newPage = nbPages - 1;
    onPageChange(newPage);
  };
}

function renderTableBody(tbodyEl, rows, indicators, sortKey, sortDesc, displayMode) {
  displayMode = displayMode || (rows.length ? getDisplayModeFromRows(rows) : "communes");
  var sorted = (sortKey && rows.length) ? sortRows(rows, sortKey, sortDesc) : rows;

  var tableEl = tbodyEl.closest("table");
  var wrapEl = tableEl ? tableEl.parentElement : null;
  var pagBarId = tbodyEl.id + "-pag";
  var pagBarEl = document.getElementById(pagBarId);
  if (!pagBarEl && wrapEl) {
    pagBarEl = document.createElement("div");
    pagBarEl.id = pagBarId;
    pagBarEl.className = "comparaison-pagination";
    wrapEl.parentElement && wrapEl.parentElement.insertBefore(pagBarEl, wrapEl.nextSibling);
  }

  var currentPage = 0;

  function goToPage(p) {
    currentPage = p;
    renderTablePage(tbodyEl, sorted, indicators, displayMode, currentPage);
    if (pagBarEl) renderPaginationBar(pagBarEl, sorted, currentPage, goToPage);
    if (tableEl) tableEl.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  goToPage(0);
}

function bindSortHandlers(blockId, theadEl, tbodyEl, rows, indicators) {
  var ths = theadEl.querySelectorAll("th.sortable");
  ths.forEach(function (th) {
    var key = th.getAttribute("data-sort");
    if (!key) return;
    th.onclick = function () {
      var st = S.sortByTable[blockId];
      if (!st) return;
      if (st.key === key) st.desc = !st.desc; else { st.key = key; st.desc = true; }
      theadEl.querySelectorAll("th.sortable .sort-icon").forEach(function (span) {
        span.textContent = "";
      });
      var activeTh = theadEl.querySelector('th.sortable[data-sort="' + key + '"]');
      if (activeTh) {
        var icon = activeTh.querySelector(".sort-icon");
        if (icon) icon.textContent = st.desc ? " ▼" : " ▲";
      }
      renderTableBody(tbodyEl, rows, indicators, st.key, st.desc);
    };
  });
}

export function uniqueFilterKeysFromJobs(jobs) {
  var seen = new Set();
  var out = [];
  (jobs || []).forEach(function (j) {
    var fk = j.filterKey;
    if (!fk || seen.has(fk)) return;
    seen.add(fk);
    out.push(fk);
  });
  return out;
}

export function fetchComparaisonScoresBatch(baseParams, uniqueKeys, scorePrincipal, cat, noStore) {
  if (!uniqueKeys.length) return Promise.resolve({});

  var arrayKeys = ["code_dept", "code_postal", "commune", "code_region", "code_insee",
    "exclude_code_insee", "exclude_code_dept", "scores_secondaires"];
  var baseObj = {};
  baseParams.forEach(function (v, k) {
    if (arrayKeys.indexOf(k) !== -1) {
      if (!baseObj[k]) baseObj[k] = [];
      baseObj[k].push(v);
    } else {
      baseObj[k] = v;
    }
  });

  return Promise.all(
    uniqueKeys.map(function (fk) {
      var parts = fk.split(S.FILTER_KEY_SEP);
      var sk = resolveFullRentabiliteScoreKey(scorePrincipal, parts[0], parts[1], parts[2], cat);
      var body = Object.assign({}, baseObj, {
        type_logt: parts[0] != null ? parts[0] : "",
        type_surf: parts[1] != null ? parts[1] : "",
        nb_pieces: parts[2] != null ? parts[2] : "",
        score_principal: sk
      });
      var fetchOpts = {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      };
      if (noStore) fetchOpts.cache = "no-store";
      return fetch(S.API_BASE + "/api/comparaison_scores", fetchOpts)
        .then(function (r) {
          if (!r.ok) {
            throw new Error(
              r.status === 404 ? "Endpoint /api/comparaison_scores non encore implémenté." : "Erreur " + r.status
            );
          }
          return r.json();
        })
        .then(function (data) {
          return { fk: fk, rows: data.rows || data.lignes || data.communes || [] };
        });
    })
  ).then(function (pairs) {
    var out = {};
    pairs.forEach(function (x) {
      out[x.fk] = x.rows;
    });
    return out;
  });
}

export function normalizeCodeInseeFr(raw) {
  if (raw == null || raw === "") return "";
  var s = String(raw).trim().replace(/\s/g, "");
  if (/^\d+$/.test(s)) {
    if (s.length <= 5) return s.padStart(5, "0");
    return s;
  }
  return s;
}

export function collectInseeListFromComparaisonResults() {
  if (!S.lastRenderTableArgs) return [];
  if (S.lastRenderTableArgs.displayMode !== "communes") return [];
  var jobs = S.lastRenderTableArgs.jobs || [];
  var rowsByFilterKey = S.lastRenderTableArgs.rowsByFilterKey || {};
  var seen = {};
  var list = [];
  jobs.forEach(function (job) {
    (rowsByFilterKey[job.filterKey] || []).forEach(function (row) {
      var raw = row.code_insee != null && row.code_insee !== "" ? row.code_insee : row.codeInsee;
      var ci = normalizeCodeInseeFr(raw);
      if (!ci || seen[ci]) return;
      seen[ci] = true;
      list.push(ci);
    });
  });
  return list;
}

export function buildDistanceOverlayFromResults(results) {
  var byInsee = {};
  var byTrip = {};
  (results || []).forEach(function (r) {
    var d = {
      distance_km: r.distance_km != null ? Number(r.distance_km) : null,
      duree_minutes: r.duree_minutes != null ? Number(r.duree_minutes) : null
    };
    var ci = normalizeCodeInseeFr(r.code_insee);
    if (ci) byInsee[ci] = d;
    var tk = tripletKeyFromParts(r.code_dept, r.code_postal, r.commune);
    if (tk) byTrip[tk] = d;
  });
  return { byInsee: byInsee, byTrip: byTrip };
}

function tripletKeyFromParts(dept, postal, commune) {
  var d = normalizeDeptCodeTwoChars(dept);
  var p = String(postal || "").trim();
  var n = normalizeNameCanonical(String(commune || ""));
  if (!d || !p || !n) return "";
  return d + "|" + p + "|" + n;
}

function lookupDistanceForRow(row, overlay) {
  if (!overlay) return null;
  var ci = normalizeCodeInseeFr(row.code_insee);
  if (ci && overlay.byInsee[ci]) return overlay.byInsee[ci];
  var tk = tripletKeyFromParts(row.code_dept, row.code_postal, row.commune || row.nom_commune);
  if (tk && overlay.byTrip[tk]) return overlay.byTrip[tk];
  return null;
}

export function parseDistanceFilterInputs() {
  var elKm = document.getElementById("distances-max-km");
  var elMin = document.getElementById("distances-max-minutes");
  var maxKm = NaN;
  var maxMin = NaN;
  if (elKm && String(elKm.value || "").trim()) {
    maxKm = parseFloat(String(elKm.value).replace(",", "."));
  }
  if (elMin && String(elMin.value || "").trim()) {
    maxMin = parseFloat(String(elMin.value).replace(",", "."));
  }
  return {
    maxKm: Number.isFinite(maxKm) && maxKm > 0 ? maxKm : null,
    maxMin: Number.isFinite(maxMin) && maxMin > 0 ? maxMin : null
  };
}

function passesDistanceFilters(r, maxKm, maxMin) {
  var hasKm = maxKm != null && Number.isFinite(maxKm) && maxKm > 0;
  var hasMin = maxMin != null && Number.isFinite(maxMin) && maxMin > 0;
  if (!hasKm && !hasMin) return true;
  if (hasKm) {
    if (r._distance_km != null && Number.isFinite(r._distance_km) && r._distance_km > maxKm) return false;
  }
  if (hasMin) {
    if (r._duree_minutes != null && Number.isFinite(r._duree_minutes) && r._duree_minutes > maxMin) return false;
  }
  return true;
}

function prepareRowsWithDistanceOverlay(displayMode, rowsRaw, overlay, distFilt) {
  if (!rowsRaw || !rowsRaw.length) return [];
  if (displayMode !== "communes" || !overlay) {
    return rowsRaw.map(function (r) {
      return Object.assign({}, r);
    });
  }
  var maxKm = distFilt ? distFilt.maxKm : null;
  var maxMin = distFilt ? distFilt.maxMin : null;
  var out = [];
  for (var i = 0; i < rowsRaw.length; i++) {
    var r = Object.assign({}, rowsRaw[i]);
    var dist = lookupDistanceForRow(r, overlay);
    r._distance_km = dist && dist.distance_km != null ? Number(dist.distance_km) : null;
    r._duree_minutes = dist && dist.duree_minutes != null ? Number(dist.duree_minutes) : null;
    if (!passesDistanceFilters(r, maxKm, maxMin)) continue;
    out.push(r);
  }
  return out;
}

export function buildFilteredRowsByFilterKeyForMaps(rowsByFilterKey, jobs, displayMode) {
  var distFilt = S.lastDistanceOverlay ? parseDistanceFilterInputs() : { maxKm: null, maxMin: null };
  var out = {};
  (jobs || []).forEach(function (job) {
    var raw = rowsByFilterKey[job.filterKey] || [];
    out[job.filterKey] = prepareRowsWithDistanceOverlay(displayMode, raw, S.lastDistanceOverlay, distFilt);
  });
  return out;
}

function formatDurationMinutesCell(minutes) {
  if (minutes == null || !Number.isFinite(Number(minutes))) return "—";
  var m = Number(minutes);
  var h = Math.floor(m / 60);
  var mm = Math.round(m % 60);
  if (h > 0) return h + " h " + (mm < 10 ? "0" : "") + mm + " min";
  return Math.round(m) + " min";
}

export function distinctDeptCodesInSelection() {
  var s = new Set();
  S.selectedCommunes.forEach(function (c) {
    var d = normalizeDeptCodeTwoChars(c.code_dept);
    if (d) s.add(d);
  });
  return s;
}

/** Régions distinctes dérivées des départements des communes sélectionnées (pour éviter scope=region trop étroit). */
export function distinctRegionIdsInSelection() {
  var s = new Set();
  S.selectedCommunes.forEach(function (c) {
    var d = normalizeDeptCodeTwoChars(c.code_dept);
    var rid = d ? getRegionForDepartment(d) : null;
    if (rid != null && String(rid).trim() !== "") s.add(String(rid));
  });
  return s;
}

function normalizeDeptKeyForCompare(d) {
  return normalizeDeptCodeTwoChars(d);
}

export function communeRowsCoverSelectedDepartments(rowsByFilterKey, jobs) {
  var sel = distinctDeptCodesInSelection();
  if (sel.size <= 1) return true;
  var rowDepts = new Set();
  (jobs || []).forEach(function (job) {
    (rowsByFilterKey[job.filterKey] || []).forEach(function (r) {
      var d = normalizeDeptKeyForCompare(r.code_dept);
      if (d) rowDepts.add(d);
    });
  });
  if (rowDepts.size === 0) return true;
  var ok = true;
  sel.forEach(function (d) {
    if (!rowDepts.has(normalizeDeptKeyForCompare(d))) ok = false;
  });
  return ok;
}

function csvField(v) {
  var s = String(v == null ? "" : v);
  if (s.indexOf(";") !== -1 || s.indexOf('"') !== -1 || s.indexOf("\n") !== -1) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function exportTableToCsv(job, rows, tableIndicators, displayMode, filterSummary, sortKey, sortDesc) {
  var sorted = (sortKey && rows.length) ? sortRows(rows, sortKey, sortDesc) : rows;
  var BOM = "\uFEFF";
  var lines = [];
  lines.push("# " + (job.groupTitle || "") + " \u2014 " + (job.cardTitle || ""));
  if (job.criteriaLine) lines.push("# Crit\u00e8res : " + job.criteriaLine);
  if (filterSummary) lines.push("# Filtres : " + filterSummary);
  lines.push("");
  var geoHeaders = displayMode === "departements"
    ? ["Rang", "R\u00e9gion", "Code d\u00e9pt", "D\u00e9partement"]
    : displayMode === "regions"
    ? ["Rang", "Code r\u00e9gion", "R\u00e9gion"]
    : ["Rang", "R\u00e9gion", "Code d\u00e9pt", "Code postal", "Commune"];
  var dataHeaders = tableIndicators.map(function (ind) { return ind.label; });
  lines.push(geoHeaders.concat(dataHeaders).map(csvField).join(";"));
  sorted.forEach(function (row, i) {
    var geo;
    if (displayMode === "departements") {
      var cd = row.code_dept || "";
      geo = [i + 1, row.region || row.reg_nom || "", cd, row.dep_nom || cd];
    } else if (displayMode === "regions") {
      geo = [i + 1, row.code_region || "", row.region || row.reg_nom || ""];
    } else {
      geo = [i + 1, row.region || row.reg_nom || "", row.code_dept || "", row.code_postal || row.code_postal_principal || "", row.commune || row.nom_commune || ""];
    }
    var data = tableIndicators.map(function (ind) {
      var v = row[ind.key];
      if (v == null || v === "") return "";
      var n = Number(v);
      return Number.isFinite(n) ? String(n).replace(".", ",") : v;
    });
    lines.push(geo.concat(data).map(csvField).join(";"));
  });
  var content = BOM + lines.join("\n");
  var blob = new Blob([content], { type: "text/csv;charset=utf-8" });
  var url = URL.createObjectURL(blob);
  var a = document.createElement("a");
  a.href = url;
  a.download = "comparaison_" + (job.cardTitle || "export").replace(/[^a-zA-Z0-9_-]/g, "_").substring(0, 50) + ".csv";
  document.body.appendChild(a);
  a.click();
  setTimeout(function () { document.body.removeChild(a); URL.revokeObjectURL(url); }, 500);
}

export function renderComparaisonTables(jobs, rowsByFilterKey, displayMode, cat, displayIndicators, theadPrefix) {
  var dyn = document.getElementById("comparaison-tables-dynamic");
  if (!dyn) return;
  dyn.innerHTML = "";
  S.sortByTable = {};

  jobs.forEach(function (job, idx) {
    var rowsRaw = rowsByFilterKey[job.filterKey] || [];
    var distFilt = S.lastDistanceOverlay ? parseDistanceFilterInputs() : { maxKm: null, maxMin: null };
    var rows = prepareRowsWithDistanceOverlay(displayMode, rowsRaw, S.lastDistanceOverlay, distFilt);
    var jobId = "comparaison-job-" + idx;
    S.sortByTable[jobId] = { key: null, desc: true };

    var tableIndicators = computeTableIndicatorsForJob(job, cat, displayIndicators);
    if (tableIndicators.length === 0) return;
    if (!jobHasDisplayableData(rowsRaw, job, cat, displayIndicators)) return;
    if (cat === "rentabilite") {
      var nbLocauxKey = resolveNbLocauxKey(job.scoreKey);
      tableIndicators = [
        { key: nbLocauxKey, label: "Nb locaux vendus" }
      ].concat(tableIndicators);
      var firstScoreIdx = -1;
      for (var si = 0; si < tableIndicators.length; si++) {
        if (tableIndicators[si].key !== nbLocauxKey) {
          firstScoreIdx = si;
          break;
        }
      }
      S.sortByTable[jobId].key = firstScoreIdx >= 0 ? tableIndicators[firstScoreIdx].key : tableIndicators[0].key;
    } else {
      S.sortByTable[jobId].key = tableIndicators[0].key;
    }
    if (S.lastDistanceOverlay && displayMode === "communes") {
      tableIndicators = tableIndicators.concat([
        { key: "_distance_km", label: "Distance (km)" },
        { key: "_duree_minutes", label: "Trajet (min)" }
      ]);
    }

    var theadRow = theadPrefix;
    tableIndicators.forEach(function (ind, i) {
      var label = escapeHtml(ind.label);
      var dir = ind.key === S.sortByTable[jobId].key ? " ▼" : "";
      theadRow +=
        '<th class="col-num sortable" data-sort="' +
        escapeHtml(ind.key) +
        '" data-label="' +
        label.replace(/"/g, "&quot;") +
        '">' +
        label +
        '<span class="sort-icon">' +
        dir +
        "</span></th>";
    });
    theadRow += "</tr>";

    var nbZones = rows.length;
    var zoneLabel = displayMode === "departements" ? "départements" : displayMode === "regions" ? "régions" : "communes";
    var titleHtml =
      "<strong>" + escapeHtml(job.groupTitle) + "</strong> (" + nbZones + " " + zoneLabel + ") — " + escapeHtml(job.cardTitle);
    var filterSummary = S.lastRenderTableArgs ? (S.lastRenderTableArgs.filterSummary || "") : "";
    var crit = job.criteriaLine
      ? '<p class="comparaison-table-criteria">' + escapeHtml(job.criteriaLine) + "</p>"
      : "";

    var section = document.createElement("section");
    section.className = "comparaison-table-block";
    section.id = jobId;
    section.setAttribute("aria-hidden", "false");
    section.innerHTML =
      '<h3 class="comparaison-table-title">' +
      '<span class="comparaison-table-title-text">' + titleHtml + '</span>' +
      (filterSummary ? '<span class="comparaison-table-filter-summary">' + escapeHtml(filterSummary) + '</span>' : '') +
      '<button type="button" class="comparaison-export-csv-btn" title="Exporter ce tableau en CSV">Exporter CSV</button>' +
      "</h3>" +
      crit +
      '<div class="comparaison-table-wrap"><table class="comparaison-table" id="' +
      jobId +
      '-table"><thead id="' +
      jobId +
      '-thead">' +
      theadRow +
      '</thead><tbody id="' +
      jobId +
      '-tbody"></tbody></table></div>';

    dyn.appendChild(section);

    (function (capturedJob, capturedRows, capturedTableIndicators, capturedDisplayMode, capturedFilterSummary, capturedJobId) {
      var csvBtn = section.querySelector(".comparaison-export-csv-btn");
      if (csvBtn) {
        csvBtn.addEventListener("click", function () {
          var st = S.sortByTable[capturedJobId] || { key: null, desc: true };
          exportTableToCsv(capturedJob, capturedRows, capturedTableIndicators, capturedDisplayMode, capturedFilterSummary, st.key, st.desc);
        });
      }
    })(job, rows, tableIndicators, displayMode, filterSummary, jobId);

    var tbodyEl = document.getElementById(jobId + "-tbody");
    var theadEl = document.getElementById(jobId + "-thead");
    var st = S.sortByTable[jobId];
    renderTableBody(tbodyEl, rows, tableIndicators, st.key, st.desc, displayMode);
    bindSortHandlers(jobId, theadEl, tbodyEl, rows, tableIndicators);
  });
}
