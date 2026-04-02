/**
 * Critères de comparaison : refs API, granularité type/surface/pièces,
 * validateurs, jobs de cartes/tableaux, visibilité onglet cartes / indicateurs.
 */
import { S } from "./comparaison_state.js";
import { getScoreLabel, makeFilterKey } from "./comparaison_utils.js";
import {
  getSelectedValuesFromMulti,
  setOnlySelected,
  bindExclusiveTousMulti,
  bindExclusiveAllRegions,
  bindExclusiveAllDepts,
  getMultiSelectEffectiveSingle
} from "./comparaison_selection.js";
import {
  isRentabiliteTypeSupported,
  resolveFullRentabiliteScoreKey
} from "./comparaison_renta_keys.js";

export function shouldShowSurfPiecesSelectors() {
  var vals = getSelectedValuesFromMulti("comparaison-type-logt").map(function (v) {
    return String(v || "").toUpperCase();
  });
  if (vals.length === 0) return true;
  if (vals.indexOf("TOUS") !== -1) return true;
  return vals.indexOf("MAISON") !== -1 || vals.indexOf("APPART") !== -1;
}

export function updateSurfPiecesVisibility() {
  var show = shouldShowSurfPiecesSelectors();
  var surfCol = document.getElementById("comparaison-surf-col");
  var pieCol = document.getElementById("comparaison-pieces-col");
  if (surfCol) surfCol.style.display = show ? "" : "none";
  if (pieCol) pieCol.style.display = show ? "" : "none";
  if (!show) {
    var sSurf = document.getElementById("comparaison-surf");
    var sPie = document.getElementById("comparaison-pieces");
    if (sSurf) setOnlySelected("comparaison-surf", "TOUTES");
    if (sPie) setOnlySelected("comparaison-pieces", "TOUS");
  }
}

export function validateComparaisonRefCriteria() {
  var block = document.getElementById("comparaison-criteria-block");
  if (!block || block.hasAttribute("hidden")) return null;
  var types = getSelectedValuesFromMulti("comparaison-type-logt");
  if (types.length === 0) {
    return "Sélectionnez au moins une valeur pour le critère « Type de local ».";
  }
  if (shouldShowSurfPiecesSelectors()) {
    var s = getSelectedValuesFromMulti("comparaison-surf");
    var p = getSelectedValuesFromMulti("comparaison-pieces");
    if (s.length === 0) return "Sélectionnez au moins une valeur pour le critère « Surface ».";
    if (p.length === 0) return "Sélectionnez au moins une valeur pour le critère « Nombre de pièces ».";
  }
  return null;
}

export function setMapTabVisible(show) {
  var tabMaps = document.getElementById("comparaison-tab-maps");
  var tabTables = document.getElementById("comparaison-tab-tables");
  var panelTables = document.getElementById("comparaison-panel-tables");
  var panelMaps = document.getElementById("comparaison-panel-maps");
  if (!tabMaps) return;
  tabMaps.hidden = !show;
  if (!show && tabMaps.classList.contains("is-active") && tabTables && panelTables && panelMaps) {
    tabTables.classList.add("is-active");
    tabMaps.classList.remove("is-active");
    tabTables.setAttribute("aria-selected", "true");
    tabMaps.setAttribute("aria-selected", "false");
    panelTables.hidden = false;
    panelMaps.hidden = true;
  }
}

export function getIndicatorCheckboxIds(category) {
  if (category === "rentabilite") return ["ind-renta-brute", "ind-renta-nette"];
  if (category === "taxes") return ["ind-taux-tfb", "ind-taux-teom"];
  return [];
}

export function updateIndicatorVisibility() {
  var cat = document.getElementById("comparaison-categorie").value;
  document.getElementById("indicators-rentabilite").style.display = cat === "rentabilite" ? "" : "none";
  document.getElementById("indicators-taxes").style.display = cat === "taxes" ? "" : "none";
  var rentaMinFields = document.getElementById("comparaison-renta-min-fields");
  if (rentaMinFields) rentaMinFields.style.display = cat === "rentabilite" ? "contents" : "none";
}

export function getSelectedIndicators() {
  var cat = document.getElementById("comparaison-categorie").value;
  var ids = getIndicatorCheckboxIds(cat);
  var order = S.CATEGORY_INDICATORS[cat] || [];
  var out = [];
  order.forEach(function (key) {
    var id = key === "renta_brute" ? "ind-renta-brute" : key === "renta_nette" ? "ind-renta-nette" : key === "taux_tfb" ? "ind-taux-tfb" : "ind-taux-teom";
    var cb = document.getElementById(id);
    if (cb && cb.checked) out.push({ key: key, label: getScoreLabel(key) });
  });
  return out;
}

export function refLibelle(arr, code) {
  var row = (arr || []).find(function (x) { return String(x.code) === String(code); });
  return row ? row.libelle : code;
}

/** Granularité « détaillée » pour un critère (type / surface / pièces), indépendamment des autres. */
export function granularityIsDetail(radioName) {
  var el = document.querySelector('input[name="' + radioName + '"]:checked');
  return el && el.value === "detail";
}

export function buildMapJobs(scorePrincipal, cat) {
  var showSurfPieces = shouldShowSurfPiecesSelectors();
  var showType = granularityIsDetail("comparaison-granularity-type");
  var showSurf = granularityIsDetail("comparaison-granularity-surf") && showSurfPieces;
  var showPieces = granularityIsDetail("comparaison-granularity-pieces") && showSurfPieces;
  var types = S.refData.type_logts.length ? S.refData.type_logts : S.REF_TYPE_LOGTS_FALLBACK;
  var surfs = S.refData.type_surf.length ? S.refData.type_surf : S.REF_TYPE_SURF_FALLBACK;
  var pieces = S.refData.nb_pieces.length ? S.refData.nb_pieces : S.REF_NB_PIECES_FALLBACK;
  var selT = getMultiSelectEffectiveSingle("comparaison-type-logt", "TOUS", "TOUS");
  var selS = getMultiSelectEffectiveSingle("comparaison-surf", "TOUTES", "TOUTES");
  var selP = getMultiSelectEffectiveSingle("comparaison-pieces", "TOUS", "TOUS");
  var jobs = [];

  if (!showType && !showSurf && !showPieces) {
    jobs.push({
      groupTitle: "Vue synthétique",
      cardTitle: refLibelle(types, selT) + " — " + refLibelle(surfs, selS) + " — " + refLibelle(pieces, selP),
      scoreKey: resolveFullRentabiliteScoreKey(scorePrincipal, selT, selS, selP, cat),
      footnote: null,
      criteriaLine: "Type : " + refLibelle(types, selT) + " · Surface : " + refLibelle(surfs, selS) + " · Pièces : " + refLibelle(pieces, selP),
      filterKey: makeFilterKey(selT, selS, selP),
      typeCodeForColumns: selT
    });
    return jobs;
  }

  if (showType) {
    types.forEach(function (t) {
      jobs.push({
        groupTitle: "Type de local",
        cardTitle: t.libelle,
        scoreKey: resolveFullRentabiliteScoreKey(scorePrincipal, t.code, selS, selP, cat),
        footnote:
          cat === "rentabilite" && !isRentabiliteTypeSupported(t.code)
            ? "Rentabilité non calculée pour ce code de type (non référencé). Pas de repli sur l’agrégat « tous types »."
            : null,
        criteriaLine: "Surface : " + refLibelle(surfs, selS) + " · Pièces : " + refLibelle(pieces, selP),
        filterKey: makeFilterKey(t.code, selS, selP),
        typeCodeForColumns: t.code
      });
    });
  }
  if (showSurf) {
    surfs.forEach(function (s) {
      jobs.push({
        groupTitle: "Surface",
        cardTitle: s.libelle,
        scoreKey: resolveFullRentabiliteScoreKey(scorePrincipal, selT, s.code, selP, cat),
        footnote:
          s.code !== "TOUTES" && cat === "rentabilite"
            ? "Indicateurs issus des colonnes tranche surface (vf_communes S1–S5), sans repli sur l’agrégat global."
            : null,
        criteriaLine: "Type : " + refLibelle(types, selT) + " · Pièces : " + refLibelle(pieces, selP),
        filterKey: makeFilterKey(selT, s.code, selP),
        typeCodeForColumns: selT
      });
    });
  }
  if (showPieces) {
    pieces.forEach(function (p) {
      jobs.push({
        groupTitle: "Nombre de pièces",
        cardTitle: p.libelle,
        scoreKey: resolveFullRentabiliteScoreKey(scorePrincipal, selT, selS, p.code, cat),
        footnote:
          p.code !== "TOUS" && cat === "rentabilite"
            ? "Indicateurs issus des colonnes tranche pièces (vf_communes T1–T5), sans repli sur l’agrégat global."
            : null,
        criteriaLine: "Type : " + refLibelle(types, selT) + " · Surface : " + refLibelle(surfs, selS),
        filterKey: makeFilterKey(selT, selS, p.code),
        typeCodeForColumns: selT
      });
    });
  }
  return jobs;
}

export function sortRefsByOrder(arr) {
  return (arr || []).slice().sort(function (a, b) {
    return (Number(a.sort_order) || 0) - (Number(b.sort_order) || 0);
  });
}

export function fillRefMultiSelect(selectId, rows, defaultCode) {
  var sel = document.getElementById(selectId);
  if (!sel) return;
  var prevSel = Array.from(sel.selectedOptions).map(function (o) {
    return o.value;
  });
  sel.innerHTML = "";
  (rows || []).forEach(function (r) {
    var opt = document.createElement("option");
    opt.value = r.code;
    opt.textContent = r.libelle || r.code;
    sel.appendChild(opt);
  });
  var codes = Array.from(sel.options).map(function (o) {
    return o.value;
  });
  if (prevSel.length && prevSel.every(function (v) { return codes.indexOf(v) !== -1; })) {
    prevSel.forEach(function (v) {
      var o = Array.from(sel.options).find(function (x) {
        return x.value === v;
      });
      if (o) o.selected = true;
    });
  } else if (defaultCode && codes.indexOf(defaultCode) !== -1) {
    setOnlySelected(selectId, defaultCode);
  }
}

export function wireRefComparaisonControls() {
  if (S.refComparaisonControlsWired) return;
  S.refComparaisonControlsWired = true;
  bindExclusiveTousMulti("comparaison-type-logt", "TOUS", "type");
  bindExclusiveTousMulti("comparaison-surf", "TOUTES", "surf");
  bindExclusiveTousMulti("comparaison-pieces", "TOUS", "pieces");
  bindExclusiveAllRegions("comparaison-region-select-dept", "regionDept");
  bindExclusiveAllRegions("comparaison-region-select-only", "regionOnly");
  bindExclusiveAllDepts();
  var t = document.getElementById("comparaison-type-logt");
  if (t) t.addEventListener("change", updateSurfPiecesVisibility);
}

export function fillRefSelects() {
  var types = sortRefsByOrder(S.refData.type_logts.length ? S.refData.type_logts : S.REF_TYPE_LOGTS_FALLBACK);
  var surfs = sortRefsByOrder(S.refData.type_surf.length ? S.refData.type_surf : S.REF_TYPE_SURF_FALLBACK);
  var pieces = sortRefsByOrder(S.refData.nb_pieces.length ? S.refData.nb_pieces : S.REF_NB_PIECES_FALLBACK);
  fillRefMultiSelect("comparaison-type-logt", types, "TOUS");
  fillRefMultiSelect("comparaison-surf", surfs, "TOUTES");
  fillRefMultiSelect("comparaison-pieces", pieces, "TOUS");
  wireRefComparaisonControls();
  updateSurfPiecesVisibility();
}

export function loadRefs() {
  return fetch(S.API_BASE + "/api/refs-comparaison-logement")
    .then(function (r) {
      if (!r.ok) throw new Error("refs");
      return r.json();
    })
    .then(function (data) {
      S.refData.type_logts = data.type_logts || [];
      S.refData.type_surf = data.type_surf || [];
      S.refData.nb_pieces = data.nb_pieces || [];
      fillRefSelects();
    })
    .catch(function () {
      S.refData.type_logts = [];
      S.refData.type_surf = [];
      S.refData.nb_pieces = [];
      fillRefSelects();
    });
}
