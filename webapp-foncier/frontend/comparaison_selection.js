/**
 * Sélection géographique et listes (communes, départements, régions).
 */
import { S } from "./comparaison_state.js";
import {
  escapeHtml,
  normalizeDeptCodeTwoChars,
  getComparaisonMode,
  getRegionForDepartment
} from "./comparaison_utils.js";

export function getSelectedValuesFromMulti(selectId) {
  var sel = document.getElementById(selectId);
  if (!sel) return [];
  return Array.from(sel.selectedOptions).map(function (o) {
    return o.value;
  });
}

export function setOnlySelected(selectId, value) {
  var sel = document.getElementById(selectId);
  if (!sel) return;
  Array.from(sel.options).forEach(function (o) {
    o.selected = o.value === value;
  });
}

export function bindExclusiveTousMulti(selectId, tousCode, lastKey) {
  var sel = document.getElementById(selectId);
  if (!sel || !sel.multiple) return;
  sel.addEventListener("mousedown", function (e) {
    var t = e.target;
    if (t && t.tagName === "OPTION") S.lastExclusiveTousClick[lastKey] = t.value;
  });
  sel.addEventListener("change", function () {
    var vals = getSelectedValuesFromMulti(selectId);
    var hasT = vals.indexOf(tousCode) !== -1;
    if (!hasT || vals.length <= 1) return;
    var last = S.lastExclusiveTousClick[lastKey];
    if (last === tousCode) {
      setOnlySelected(selectId, tousCode);
    } else {
      Array.from(sel.options).forEach(function (o) {
        if (o.value === tousCode) o.selected = false;
      });
    }
  });
}

export function bindExclusiveAllRegions(selectId, lastKey) {
  var sel = document.getElementById(selectId);
  if (!sel || !sel.multiple) return;
  sel.addEventListener("mousedown", function (e) {
    var t = e.target;
    if (t && t.tagName === "OPTION") S.lastExclusiveTousClick[lastKey] = t.value;
  });
  sel.addEventListener("change", function () {
    var vals = getSelectedValuesFromMulti(selectId);
    var hasA = vals.indexOf(S.ALL_REGIONS_SPECIAL) !== -1;
    if (!hasA || vals.length <= 1) return;
    var last = S.lastExclusiveTousClick[lastKey];
    if (last === S.ALL_REGIONS_SPECIAL) {
      setOnlySelected(selectId, S.ALL_REGIONS_SPECIAL);
    } else {
      Array.from(sel.options).forEach(function (o) {
        if (o.value === S.ALL_REGIONS_SPECIAL) o.selected = false;
      });
    }
  });
}

export function bindExclusiveAllDepts() {
  var sel = document.getElementById("comparaison-dept-select");
  if (!sel || !sel.multiple) return;
  sel.addEventListener("mousedown", function (e) {
    var t = e.target;
    if (t && t.tagName === "OPTION") S.lastExclusiveTousClick.dept = t.value;
  });
  sel.addEventListener("change", function () {
    var vals = getSelectedValuesFromMulti("comparaison-dept-select");
    var hasA = vals.indexOf(S.ALL_DEPTS_SPECIAL) !== -1;
    if (!hasA || vals.length <= 1) return;
    var last = S.lastExclusiveTousClick.dept;
    if (last === S.ALL_DEPTS_SPECIAL) {
      setOnlySelected("comparaison-dept-select", S.ALL_DEPTS_SPECIAL);
    } else {
      Array.from(sel.options).forEach(function (o) {
        if (o.value === S.ALL_DEPTS_SPECIAL) o.selected = false;
      });
    }
  });
}

export function getMultiSelectEffectiveSingle(selectId, defaultCode, tousCode) {
  var vals = getSelectedValuesFromMulti(selectId);
  if (vals.length === 0) return defaultCode;
  if (vals.length === 1) return vals[0];
  if (vals.indexOf(tousCode) !== -1) return tousCode;
  return vals[0];
}
export function fillRegionsMultiSelectDeptMode() {
  var sel = document.getElementById("comparaison-region-select-dept");
  if (!sel) return;
  sel.innerHTML = "";
  var optAll = document.createElement("option");
  optAll.value = S.ALL_REGIONS_SPECIAL;
  optAll.textContent = "Toutes les régions";
  sel.appendChild(optAll);
  (S.geo.regions || []).forEach(function (r) {
    var o = document.createElement("option");
    o.value = r.id;
    o.textContent = r.nom;
    sel.appendChild(o);
  });
  setOnlySelected("comparaison-region-select-dept", S.ALL_REGIONS_SPECIAL);
}

export function fillRegionsMultiSelectRegionsOnly() {
  var sel = document.getElementById("comparaison-region-select-only");
  if (!sel) return;
  sel.innerHTML = "";
  var optAll = document.createElement("option");
  optAll.value = S.ALL_REGIONS_SPECIAL;
  optAll.textContent = "Toutes les régions";
  sel.appendChild(optAll);
  (S.geo.regions || []).forEach(function (r) {
    var o = document.createElement("option");
    o.value = r.id;
    o.textContent = r.nom;
    sel.appendChild(o);
  });
  Array.from(sel.options).forEach(function (o) {
    o.selected = false;
  });
}

export function getDeptModeSelectedRegionIdsForFilter() {
  var vals = getSelectedValuesFromMulti("comparaison-region-select-dept");
  if (vals.indexOf(S.ALL_REGIONS_SPECIAL) !== -1) {
    return (S.geo.regions || []).map(function (r) {
      return r.id;
    });
  }
  return vals.filter(function (v) {
    return v !== S.ALL_REGIONS_SPECIAL;
  });
}

export function fillDeptsMultiSelect(preserveSelection) {
  var sel = document.getElementById("comparaison-dept-select");
  if (!sel) return;
  if (preserveSelection == null) preserveSelection = true;
  var prevSel = getSelectedValuesFromMulti("comparaison-dept-select");
  var selectedRegionIds = getDeptModeSelectedRegionIdsForFilter();
  var allDepts = S.geo.departements || [];
  var deptsToShow =
    selectedRegionIds.length === 0
      ? allDepts
      : allDepts.filter(function (d) {
          var code = typeof d === "object" ? d.code : d;
          var reg = (S.geo.regions || []).find(function (r) {
            return (r.departements || []).indexOf(code) !== -1;
          });
          return reg && selectedRegionIds.indexOf(reg.id) !== -1;
        });
  sel.innerHTML = "";
  var optAllD = document.createElement("option");
  optAllD.value = S.ALL_DEPTS_SPECIAL;
  optAllD.textContent = "Tous les départements";
  sel.appendChild(optAllD);
  deptsToShow.forEach(function (d) {
    var code = typeof d === "object" ? d.code : d;
    var nom = typeof d === "object" ? d.nom : S.geo.deptNomByCode[code] || code;
    var o = document.createElement("option");
    o.value = code;
    o.textContent = code + " " + nom;
    sel.appendChild(o);
  });
  if (preserveSelection) {
    var codes = Array.from(sel.options).map(function (o) {
      return o.value;
    });
    prevSel.forEach(function (v) {
      if (codes.indexOf(v) === -1) return;
      var opt = Array.from(sel.options).find(function (x) {
        return x.value === v;
      });
      if (opt) opt.selected = true;
    });
  }
}

export function initDepartementsModeSelectionState() {
  fillRegionsMultiSelectDeptMode();
  fillDeptsMultiSelect(false);
}

export function getSelectedRegionIds() {
  return getDeptModeSelectedRegionIdsForFilter();
}

export function getSelectedRegionIdsForRegionsMode() {
  var vals = getSelectedValuesFromMulti("comparaison-region-select-only");
  if (vals.indexOf(S.ALL_REGIONS_SPECIAL) !== -1) {
    return (S.geo.regions || []).map(function (r) {
      return r.id;
    });
  }
  return vals.filter(function (v) {
    return v !== S.ALL_REGIONS_SPECIAL;
  });
}

export function getDeptCodesForCurrentRegionFilter() {
  var regionIds = getSelectedRegionIds();
  if (regionIds.length === 0) {
    return (S.geo.departements || []).map(function (d) {
      return typeof d === "object" ? d.code : d;
    });
  }
  var codes = [];
  (S.geo.regions || []).forEach(function (r) {
    if (regionIds.indexOf(r.id) !== -1 && r.departements) codes.push.apply(codes, r.departements);
  });
  return codes.filter(function (c, i, a) {
    return a.indexOf(c) === i;
  });
}

export function getSelectedDeptCodes() {
  var vals = getSelectedValuesFromMulti("comparaison-dept-select");
  if (vals.indexOf(S.ALL_DEPTS_SPECIAL) !== -1) {
    return getDeptCodesForCurrentRegionFilter();
  }
  return vals.filter(function (v) {
    return v !== S.ALL_DEPTS_SPECIAL;
  });
}

export function updateSelectedPlacesTitle() {
  var titleEl = document.getElementById("comparaison-selected-title");
  if (!titleEl) return;
  var mode = getComparaisonMode();
  var n = 0;
  if (mode === "communes") n = S.selectedCommunes.length;
  else if (mode === "departements") n = getSelectedDeptCodes().length;
  else n = getSelectedRegionIdsForRegionsMode().length;
  var base =
    mode === "communes"
      ? "Communes sélectionnées"
      : mode === "departements"
        ? "Départements sélectionnés"
        : "Régions sélectionnées";
  titleEl.textContent = base + " (" + n + ")";
}

export function updateSelectedListForMode() {
  var block = document.getElementById("comparaison-selected-block");
  var ul = document.getElementById("comparaison-selected-ul");
  if (!block || !ul) {
    updateSelectedPlacesTitle();
    updateComparaisonCriteriaBlockVisibility();
    return;
  }
  var mode = getComparaisonMode();
  if (mode === "communes") {
    if (S.selectedCommunes.length === 0) {
      block.setAttribute("aria-hidden", "true");
      ul.innerHTML = "";
      updateSelectedPlacesTitle();
      updateComparaisonCriteriaBlockVisibility();
      return;
    }
    block.removeAttribute("aria-hidden");
    ul.innerHTML = S.selectedCommunes.map(function (c, i) {
      var label = escapeHtml(c.commune) + " (" + escapeHtml(c.code_postal) + ")";
      return "<li>" + label + " <button type=\"button\" data-i=\"" + i + "\" aria-label=\"Retirer\">×</button></li>";
    }).join("");
    ul.querySelectorAll("button").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var i = parseInt(btn.getAttribute("data-i"), 10);
        var removed = S.selectedCommunes.splice(i, 1)[0];
        if (S.selectionMassMode && removed && removed.code_insee) {
          S.excludedCommunes.push(removed);
        }
        updateSelectedListForMode();
      });
    });
    updateSelectedPlacesTitle();
    updateComparaisonCriteriaBlockVisibility();
    return;
  }
  if (mode === "departements") {
    var depts = getSelectedDeptCodes();
    var names = depts.map(function (code) { return code + " " + (S.geo.deptNomByCode[code] || ""); });
    if (names.length === 0) {
      block.setAttribute("aria-hidden", "true");
      ul.innerHTML = "";
      updateSelectedPlacesTitle();
      updateComparaisonCriteriaBlockVisibility();
      return;
    }
    block.removeAttribute("aria-hidden");
    ul.innerHTML = depts.map(function (code) {
      var label = (code + " " + (S.geo.deptNomByCode[code] || "")).trim();
      return "<li>" + escapeHtml(label) + " <button type=\"button\" data-dept=\"" + escapeHtml(code) + "\" aria-label=\"Retirer\">×</button></li>";
    }).join("");
    ul.querySelectorAll("button[data-dept]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var code = btn.getAttribute("data-dept");
        var sel = document.getElementById("comparaison-dept-select");
        if (!sel) return;
        var vals = getSelectedValuesFromMulti("comparaison-dept-select");
        if (vals.indexOf(S.ALL_DEPTS_SPECIAL) !== -1) {
          Array.from(sel.options).forEach(function (o) {
            o.selected = o.value !== S.ALL_DEPTS_SPECIAL && o.value !== code;
          });
        } else {
          var o = Array.from(sel.options).find(function (x) {
            return x.value === code;
          });
          if (o) o.selected = false;
        }
        updateSelectedListForMode();
      });
    });
    updateSelectedPlacesTitle();
    updateComparaisonCriteriaBlockVisibility();
    return;
  }
  if (mode === "regions") {
    var ids = getSelectedRegionIdsForRegionsMode();
    var names = ids.map(function (id) {
      var r = (S.geo.regions || []).find(function (x) { return x.id === id; });
      return r ? r.nom : id;
    });
    if (names.length === 0) {
      block.setAttribute("aria-hidden", "true");
      ul.innerHTML = "";
      updateSelectedPlacesTitle();
      updateComparaisonCriteriaBlockVisibility();
      return;
    }
    block.removeAttribute("aria-hidden");
    ul.innerHTML = ids.map(function (id) {
      var r = (S.geo.regions || []).find(function (x) { return x.id === id; });
      var label = r ? r.nom : id;
      return "<li>" + escapeHtml(label) + " <button type=\"button\" data-region=\"" + escapeHtml(id) + "\" aria-label=\"Retirer\">×</button></li>";
    }).join("");
    ul.querySelectorAll("button[data-region]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var id = btn.getAttribute("data-region");
        var sel = document.getElementById("comparaison-region-select-only");
        if (!sel) return;
        var vals = getSelectedValuesFromMulti("comparaison-region-select-only");
        if (vals.indexOf(S.ALL_REGIONS_SPECIAL) !== -1) {
          Array.from(sel.options).forEach(function (o) {
            o.selected = o.value !== S.ALL_REGIONS_SPECIAL && o.value !== id;
          });
        } else {
          var o = Array.from(sel.options).find(function (x) {
            return x.value === id;
          });
          if (o) o.selected = false;
        }
        updateSelectedListForMode();
      });
    });
  }
  updateSelectedPlacesTitle();
  updateComparaisonCriteriaBlockVisibility();
}
export function hasGeographicSelection() {
  var mode = getComparaisonMode();
  if (mode === "communes") return S.selectedCommunes.length > 0;
  if (mode === "departements") return getSelectedDeptCodes().length > 0;
  if (mode === "regions") return getSelectedRegionIdsForRegionsMode().length > 0;
  return false;
}

export function updateComparaisonCriteriaBlockVisibility() {
  var block = document.getElementById("comparaison-criteria-block");
  if (!block) return;
  if (hasGeographicSelection()) {
    block.removeAttribute("hidden");
  } else {
    block.setAttribute("hidden", "hidden");
  }
}
export function fillRegions() {
  const sel = document.getElementById("comparaison-region");
  if (!sel) return;
  sel.innerHTML = "<option value=''>— Choisir une région —</option>" +
    (S.geo.regions || []).map(function (r) {
      return "<option value=\"" + escapeHtml(r.id) + "\">" + escapeHtml(r.nom) + "</option>";
    }).join("");
}

export function getDepartmentsForRegion(regionId) {
  if (!regionId) return (S.geo.departements || []).map(function (d) { return typeof d === "object" ? d.code : d; });
  const reg = S.geo.regions.find(function (r) { return r.id === regionId; });
  return reg && reg.departements ? reg.departements : [];
}

export function fillDepartments() {
  const regionId = document.getElementById("comparaison-region").value;
  const deptSelect = document.getElementById("comparaison-dept");
  if (!deptSelect) return;
  const codes = getDepartmentsForRegion(regionId);
  deptSelect.innerHTML = "<option value=''>— Choisir un département —</option>";
  codes.forEach(function (code) {
    const lib = S.geo.deptNomByCode[code] ? code + " " + S.geo.deptNomByCode[code] : code;
    deptSelect.innerHTML += "<option value=\"" + escapeHtml(code) + "\">" + escapeHtml(lib) + "</option>";
  });
  deptSelect.dispatchEvent(new Event("change"));
}

export function fillCommunes() {
  const codeDept = document.getElementById("comparaison-dept").value;
  const codeRegion = document.getElementById("comparaison-region").value;
  const communeSelect = document.getElementById("comparaison-commune");
  if (!communeSelect) return;
  console.log("(fillCommunes)");
  // 1ère ligne : libellé neutre
  communeSelect.innerHTML = "<option value=''>— Choisir une commune —</option>";

  // Récupérer les communes du département si un département est sélectionné
  if (codeDept){
    console.log("codeDept", codeDept);
    // 2ème ligne : toutes les communes du département : la liste est correctement renseignée
    const optAllDept = document.createElement("option");
    optAllDept.value = "__ALL_DEPT__";
    optAllDept.textContent = "Toutes les communes du département";
    communeSelect.appendChild(optAllDept);
    fetch(S.API_BASE + "/api/communes?code_dept=" + encodeURIComponent(codeDept))
      .then(function (r) { return r.json(); })
      .then(function (data) {
        S.listCommunes = Array.isArray(data) ? data : [];
        S.listCommunes.forEach(function (c) {
          const opt = document.createElement("option");
          opt.value = JSON.stringify({ code_dept: c.code_dept || "", code_postal: c.code_postal || "", commune: c.commune || "", code_insee: c.code_insee || "" });
          opt.textContent = (c.commune || "") + " (" + (c.code_postal || "") + ")";
          communeSelect.appendChild(opt);
        });
      })
      .catch(function () { S.listCommunes = []; });
  }
  // Sinon, récupérer les communes de la région si une région est sélectionnée
  else if (codeRegion){
    console.log("codeRegion", codeRegion);
    // 2ème ligne : toutes les communes de la région : la liste est correctement renseignée
    const optAllRegion = document.createElement("option");
    optAllRegion.value = "__ALL_REGION__";
    optAllRegion.textContent = "Toutes les communes de la région";
    communeSelect.appendChild(optAllRegion);
    fetch(S.API_BASE + "/api/communes?code_region=" + encodeURIComponent(codeRegion))
      .then(function (r) { return r.json(); })
      .then(function (data) {
        S.listCommunes = Array.isArray(data) ? data : [];
        S.listCommunes.forEach(function (c) {
          const opt = document.createElement("option");
          opt.value = JSON.stringify({ code_dept: c.code_dept || "", code_postal: c.code_postal || "", commune: c.commune || "", code_insee: c.code_insee || "" });
          opt.textContent = (c.commune || "") + " (" + (c.code_postal || "") + ")";
          communeSelect.appendChild(opt);
        });
      })
      .catch(function () { S.listCommunes = []; });
  }
  // Sinon (ni département ni région sélectionnés), récupérer les communes de la France
  else {
    console.log("else");
    // 2ème ligne : toutes les communes de la France : la liste est correctement renseignée
    const optAllFrance = document.createElement("option");
    optAllFrance.value = "__ALL_FRANCE__";
    optAllFrance.textContent = "Toutes les communes de la France";
    communeSelect.appendChild(optAllFrance);
    fetch(S.API_BASE + "/api/communes?all_France=true")
      .then(function (r) { return r.json(); })
      .then(function (data) {
        S.listCommunes = Array.isArray(data) ? data : [];
        S.listCommunes.forEach(function (c) {
          const opt = document.createElement("option");
          opt.value = JSON.stringify({ code_dept: c.code_dept || "", code_postal: c.code_postal || "", commune: c.commune || "", code_insee: c.code_insee || "" });
          opt.textContent = (c.commune || "") + " (" + (c.code_postal || "") + ")";
          communeSelect.appendChild(opt);
        });
      })
      .catch(function () { S.listCommunes = []; });
  }
}



export function loadGeo() {
  return fetch(S.API_BASE + "/api/geo")
    .then(function (r) { return r.json(); })
    .then(function (data) {
      S.geo.regions = data.regions || [];
      S.geo.departements = data.departements || [];
      S.geo.deptNomByCode = Object.fromEntries((S.geo.departements || []).map(function (d) { return [d.code, d.nom]; }));
      fillRegions();
      fillDepartments();
      fillRegionsMultiSelectRegionsOnly();
      initDepartementsModeSelectionState();
    })
    .catch(function () {});
}
export function addSelectedCommuneItem(item) {
  var deptNorm = normalizeDeptCodeTwoChars(item.code_dept) || String(item.code_dept || "").trim();
  var key = deptNorm + "|" + (item.code_postal || "") + "|" + (item.commune || "");

  if (S.selectedCommunesSet.has(key)) return;

  S.selectedCommunesSet.add(key);
  S.selectedCommunes.push({
    code_dept: deptNorm,
    code_postal: item.code_postal || "",
    commune: item.commune || "",
    code_insee: item.code_insee || ""
  });
}
// Fin nouveau code : Claude.ia

export function renderSelectedList() {
  updateSelectedListForMode();
}

export function initFromUrlParams() {
  var sp = new URLSearchParams(window.location.search || "");
  var code_dept = normalizeDeptCodeTwoChars(sp.get("code_dept") || "") || (sp.get("code_dept") || "").trim();
  var code_postal = (sp.get("code_postal") || "").trim();
  var commune = (sp.get("commune") || "").trim();
  if (!code_dept || !code_postal || !commune) return;
  var regionId = getRegionForDepartment(code_dept);
  var regionSelect = document.getElementById("comparaison-region");
  var deptSelect = document.getElementById("comparaison-dept");
  var searchInput = document.getElementById("comparaison-search");
  if (regionSelect && regionId) regionSelect.value = regionId;
  fillDepartments();
  if (deptSelect) deptSelect.value = code_dept;
  fillCommunes();
  addSelectedCommuneItem({ code_dept: code_dept, code_postal: code_postal, commune: commune });
  renderSelectedList();
  if (searchInput) searchInput.value = code_postal + " " + commune;
}

// Autocomplete recherche commune (nom ou code postal) — après renderSelectedList (référence dans le callback)
(function initSearch() {
  initCommuneAutocomplete({
    inputId: "comparaison-search",
    suggestionsId: "comparaison-suggestions",
    minChars: 2,
    onSelect: function (item) {
      S.selectionMassMode = null;
      S.excludedCommunes = [];
      addSelectedCommuneItem(item);
      renderSelectedList();
    }
  });
})();
