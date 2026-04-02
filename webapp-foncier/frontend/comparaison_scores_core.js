import { initDistancesSection } from './comparaison_filters.js';
import {
  updateSurfPiecesVisibility,
  validateComparaisonRefCriteria,
  setMapTabVisible,
  updateIndicatorVisibility,
  getSelectedIndicators,
  buildMapJobs,
  fillRefSelects,
  loadRefs
} from './comparaison_criteria.js';
import { S } from './comparaison_state.js';
import {
  normalizeDeptCodeTwoChars,
  getScoreLabel,
  getComparaisonMode,
  getNiveauLabel
} from './comparaison_utils.js';
import {
  fillDepartments,
  fillCommunes,
  fillDeptsMultiSelect,
  initDepartementsModeSelectionState,
  fillRegionsMultiSelectRegionsOnly,
  getSelectedDeptCodes,
  getSelectedRegionIdsForRegionsMode,
  updateSelectedListForMode,
  loadGeo,
  initFromUrlParams,
  addSelectedCommuneItem,
  renderSelectedList,
  updateComparaisonCriteriaBlockVisibility
} from './comparaison_selection.js';
import {
  renderComparaisonTables,
  fetchComparaisonScoresBatch,
  uniqueFilterKeysFromJobs,
  buildFilteredRowsByFilterKeyForMaps,
  collectInseeListFromComparaisonResults,
  buildDistanceOverlayFromResults,
  distinctDeptCodesInSelection,
  distinctRegionIdsInSelection,
  communeRowsCoverSelectedDepartments,
  parseDistanceFilterInputs,
  jobHasDisplayableData,
  getDisplayModeFromRows,
  normalizeCodeInseeFr
} from './comparaison_table.js';
import {
  renderComparaisonMap,
  clearComparaisonMap,
  refreshCommuneMapsIfCurrentTab,
  setCommuneMapSubTab,
  syncCommuneMapSubtabButtons,
  jobHasMapDisplayableData
} from './comparaison_maps.js';

(function () {
        const isLocal = (typeof window !== "undefined" && (window.location?.hostname === "localhost" || window.location?.hostname === "127.0.0.1" || window.location?.protocol === "file:"));
        S.API_BASE = isLocal && window.location?.port !== "8000" ? "http://localhost:8000" : "";




        function appendCommunesScopeParamsByInsee(baseParams) {
          baseParams.set("scope", "communes");
          var excluded = {};
          S.excludedCommunes.forEach(function (c) {
            var ex = normalizeCodeInseeFr(c.code_insee != null ? c.code_insee : c.codeInsee);
            if (ex) excluded[ex] = true;
          });
          var seen = {};
          S.selectedCommunes.forEach(function (c) {
            var ci = normalizeCodeInseeFr(c.code_insee != null ? c.code_insee : c.codeInsee);
            if (!ci || excluded[ci] || seen[ci]) return;
            seen[ci] = true;
            baseParams.append("code_insee", ci);
          });
        }

        function everySelectedCommuneHasInsee() {
          return S.selectedCommunes.length > 0 && S.selectedCommunes.every(function (c) {
            return normalizeCodeInseeFr(c.code_insee != null ? c.code_insee : c.codeInsee);
          });
        }

        function buildComparaisonDistanceScopeKey() {
          var mode = getComparaisonMode();
          if (mode === "departements") {
            return "M:dep:" + getSelectedDeptCodes().slice().sort().join(",");
          }
          if (mode === "regions") {
            return "M:reg:" + getSelectedRegionIdsForRegionsMode().slice().sort().join(",");
          }
          var exSig = S.excludedCommunes
            .map(function (c) {
              return String(c.code_insee || "").trim();
            })
            .filter(Boolean)
            .sort()
            .join("|");
          if (S.selectionMassMode === "france") {
            return "CF:france;ex:" + exSig;
          }
          if (S.selectionMassMode === "dept") {
            var dEl = document.getElementById("comparaison-dept");
            var d0 = dEl ? normalizeDeptCodeTwoChars(dEl.value) : "";
            return "CF:dept:" + d0 + ";ex:" + exSig;
          }
          if (S.selectionMassMode === "region") {
            var rEl = document.getElementById("comparaison-region");
            var r0 = rEl ? String(rEl.value || "").trim() : "";
            return "CF:region:" + r0 + ";ex:" + exSig;
          }
          return "C:manual";
        }
        function refreshTableAndMapsAfterDistanceThresholdChange() {
          if (!S.lastDistanceOverlay || !S.lastRenderTableArgs) return;
          renderComparaisonTables(
            S.lastRenderTableArgs.jobs,
            S.lastRenderTableArgs.rowsByFilterKey,
            S.lastRenderTableArgs.displayMode,
            S.lastRenderTableArgs.cat,
            S.lastRenderTableArgs.displayIndicators,
            S.lastRenderTableArgs.theadPrefix
          );
          if (S.mapViz.mode === "communes" && S.mapViz.lastCommuneMapScorePrincipal != null && S.lastComparaisonJobs.length) {
            renderComparaisonMap(S.lastComparaisonJobs, S.lastRowsByFilterKey, "communes", S.mapViz.lastCommuneMapScorePrincipal, {
              preserveLegendBounds: true
            });
          }
        }

        function resetComparaisonResultsOnModeChange(mode) {
          var emptyEl = document.getElementById("comparaison-empty");
          var loadingEl = document.getElementById("comparaison-loading");
          var errorEl = document.getElementById("comparaison-error");
          if (loadingEl) loadingEl.classList.remove("visible");
          if (errorEl) { errorEl.hidden = true; errorEl.textContent = ""; }
          if (emptyEl) {
            if (mode === "communes") {
              emptyEl.textContent = "Ajoutez au moins une commune à la liste ci‑dessus (recherche ou sélecteur), choisissez les indicateurs et cliquez sur Comparer.";
            } else if (mode === "departements") {
              emptyEl.textContent = "Sélectionnez un ou plusieurs départements puis cliquez sur Comparer.";
            } else {
              emptyEl.textContent = "Sélectionnez une ou plusieurs régions puis cliquez sur Comparer.";
            }
            emptyEl.style.display = "block";
          }
          var dynClear = document.getElementById("comparaison-tables-dynamic");
          if (dynClear) dynClear.innerHTML = "";
          S.lastComparaisonRows = [];
          S.lastComparaisonJobs = [];
          S.lastRowsByFilterKey = {};
          S.lastTableConfigs = [];
          S.sortByTable = {};
          clearComparaisonMap();
        }


        function switchModeUI() {
          var mode = getComparaisonMode();
          S.lastComparaisonMode = mode;
          document.getElementById("comparaison-mode-communes").style.display = mode === "communes" ? "" : "none";
          document.getElementById("comparaison-mode-departements").style.display = mode === "departements" ? "" : "none";
          document.getElementById("comparaison-mode-regions").style.display = mode === "regions" ? "" : "none";
          /* Titre « … sélectionné(e)s (n) » mis à jour par updateSelectedPlacesTitle() via updateSelectedListForMode(). */
          var nMaxLabel = document.getElementById("comparaison-n-max-label");
          var nMaxInput = document.getElementById("comparaison-n-max");
          var niveauText = "Nb max de " + getNiveauLabel();
          if (nMaxLabel) nMaxLabel.textContent = niveauText;
          if (nMaxInput) nMaxInput.setAttribute("aria-label", niveauText);
          resetComparaisonResultsOnModeChange(mode);
          updateSelectedListForMode();
        }


        document.getElementById("comparaison-region").addEventListener("change", function () {
          if (getComparaisonMode() === "communes" && S.selectionMassMode === "region") {
            S.selectionMassMode = null;
            S.excludedCommunes = [];
          }
          fillDepartments();
        });
        document.getElementById("comparaison-dept").addEventListener("change", function () {
          if (getComparaisonMode() === "communes" && S.selectionMassMode === "dept") {
            S.selectionMassMode = null;
            S.excludedCommunes = [];
          }
          fillCommunes();
        });
        document.getElementById("comparaison-categorie").addEventListener("change", updateIndicatorVisibility);
        [].forEach.call(document.querySelectorAll('input[name="comparaison-mode"]'), function (radio) {
          radio.addEventListener("change", switchModeUI);
        });
        (function bindMultiSelectListeners() {
          var regDept = document.getElementById("comparaison-region-select-dept");
          if (regDept) {
            regDept.addEventListener("change", function () {
              fillDeptsMultiSelect(false);
              updateSelectedListForMode();
            });
          }
          var deptSel = document.getElementById("comparaison-dept-select");
          if (deptSel) {
            deptSel.addEventListener("change", function () {
              updateSelectedListForMode();
            });
          }
          var regOnly = document.getElementById("comparaison-region-select-only");
          if (regOnly) {
            regOnly.addEventListener("change", function () {
              updateSelectedListForMode();
            });
          }
        })();

        document.getElementById("comparaison-vider-liste").addEventListener("click", function () {
          var mode = getComparaisonMode();
          S.selectedCommunes.length = 0;
          S.selectedCommunesSet.clear();
          if (mode === "departements") {
            initDepartementsModeSelectionState();
          } else if (mode === "regions") {
            var selOnly = document.getElementById("comparaison-region-select-only");
            if (selOnly) {
              Array.from(selOnly.options).forEach(function (o) {
                o.selected = false;
              });
            }
          }
          updateSelectedListForMode();
        });

        // Sélection via le select de communes
        (function initCommuneSelect() {
          const sel = document.getElementById("comparaison-commune");
          if (!sel) return;
          sel.addEventListener("change", function () {
            const v = sel.value;
            console.log("(initCommuneSelect) v : ", v);
            if (!v) return; // « Choisir une commune »
            console.log("(initCommuneSelect) S.listCommunes : ", S.listCommunes);
            // si sélection en masse (Toutes les communes du département, Toutes les communes de la région, Toutes les communes de France)
            if (v === "__ALL_DEPT__" || v === "__ALL_REGION__" || v === "__ALL_FRANCE__") {
              console.log("(initCommuneSelect) sélection en masse");
              // Mémoriser le mode de sélection en masse pour orienter le rendu cartographique
              if (v === "__ALL_FRANCE__") S.selectionMassMode = "france";
              else if (v === "__ALL_REGION__") S.selectionMassMode = "region";
              else S.selectionMassMode = "dept";
              S.excludedCommunes = []; // Reset des exclusions pour la nouvelle sélection
              // Remplir la liste avec toutes les communes sélectionnées en masse
              (S.listCommunes || []).forEach(function (c) { addSelectedCommuneItem(c); });
              renderSelectedList(); // ← une seule fois après la boucle (optimisation conseillée par Claude.ia)
              sel.value = "";
              return;
            }
            else {
              console.log("(initCommuneSelect) sélection individuelle");
            }
            try {
              const c = JSON.parse(v);
              console.log("(initCommuneSelect) c : ", c);
              S.selectionMassMode = null;
              S.excludedCommunes = [];
              addSelectedCommuneItem(c);
              renderSelectedList();
            } catch (e) {}
            sel.value = "";
          });
        })();
        

        function initComparaisonViewTabs() {
          var tabTables = document.getElementById("comparaison-tab-tables");
          var tabMaps = document.getElementById("comparaison-tab-maps");
          var panelTables = document.getElementById("comparaison-panel-tables");
          var panelMaps = document.getElementById("comparaison-panel-maps");
          if (!tabTables || !tabMaps || !panelTables || !panelMaps) return;
          function showTables() {
            tabTables.classList.add("is-active");
            tabMaps.classList.remove("is-active");
            tabTables.setAttribute("aria-selected", "true");
            tabMaps.setAttribute("aria-selected", "false");
            panelTables.hidden = false;
            panelMaps.hidden = true;
          }
          function showMaps() {
            tabMaps.classList.add("is-active");
            tabTables.classList.remove("is-active");
            tabMaps.setAttribute("aria-selected", "true");
            tabTables.setAttribute("aria-selected", "false");
            panelMaps.hidden = false;
            panelTables.hidden = true;

            // Fonction unique qui invalide et refait le fitBounds pour TOUS les types de carte
            function _refitAllMaps() {
              if (typeof L === "undefined") return;

              // 1. Cartes choroplèthes (mode département / région)
              var choroMaps = (S.mapViz.multiChoroMaps || []).map(function (x) { return x.map; }).filter(Boolean);
              choroMaps.forEach(function (m) {
                try { m.invalidateSize({ animate: false }); } catch (e) {}
              });
              (S.mapViz.multiChoroMaps || []).forEach(function (entry) {
                if (!entry || !entry.map) return;
                try {
                  entry.map.invalidateSize({ animate: false });
                  var b = entry.savedBounds;
                  if (!b || !b.isValid || !b.isValid()) {
                    b = entry.layer && entry.layer.getBounds ? entry.layer.getBounds() : null;
                  }
                  var fo = entry.fitOpts || { padding: [12, 12], maxZoom: 11 };
                  if (b && b.isValid && b.isValid()) entry.map.fitBounds(b, fo);
                } catch (e) {}
              });

              // 2. Cartes communes
              (S.mapViz.communeMaps || []).forEach(function (entry) {
                if (!entry || !entry.map) return;
                try {
                  entry.map.invalidateSize({ animate: false });
                  if (entry.franceMetroFixedBounds && entry.franceMetroFixedBounds.isValid && entry.franceMetroFixedBounds.isValid()) {
                    var ffo = entry.franceMetroFitOpts || { padding: [12, 12], maxZoom: 6 };
                    entry.map.fitBounds(entry.franceMetroFixedBounds, ffo);
                    return;
                  }
                  var b = null;
                  if (entry.preferFullLayerBounds && entry.layer && entry.layer.getBounds) {
                    b = entry.layer.getBounds();
                  } else if (entry.selBounds && entry.selBounds.isValid && entry.selBounds.isValid()) {
                    b = entry.selBounds;
                  } else if (entry.layer && entry.layer.getBounds) {
                    b = entry.layer.getBounds();
                  }
                  var fo = entry.fitOpts || { padding: [10, 10], maxZoom: 15 };
                  if (b && b.isValid && b.isValid()) entry.map.fitBounds(b, fo);
                } catch (e) {}
              });

              // 3. Carte principale unique (S.mapViz.map)
              if (S.mapViz.map) {
                try { S.mapViz.map.invalidateSize({ animate: false }); } catch (e) {}
              }
            }

            // 3 passes progressives : le navigateur a besoin de temps pour calculer
            // les dimensions du panneau après hidden=false
            setTimeout(_refitAllMaps, 80);
            setTimeout(_refitAllMaps, 300);
            setTimeout(_refitAllMaps, 700);
          }
          tabTables.addEventListener("click", showTables);
          tabMaps.addEventListener("click", showMaps);
        }

        function initCommuneMapSubtabsWire() {
          var root = document.getElementById("comparaison-commune-map-subtabs");
          if (!root) return;
          root.addEventListener("click", function (ev) {
            var t = ev.target && ev.target.closest ? ev.target.closest("button[data-commune-map-tab]") : null;
            if (!t) return;
            var tab = t.getAttribute("data-commune-map-tab");
            if (!tab) return;
            setCommuneMapSubTab(tab);
            syncCommuneMapSubtabButtons(tab);
            refreshCommuneMapsIfCurrentTab();
          });
        }

        function runComparaison() {
          var mode = getComparaisonMode();
          var cat = document.getElementById("comparaison-categorie").value;
          var indicators = getSelectedIndicators();
          var nMax = parseInt(document.getElementById("comparaison-n-max").value, 10) || 20;
          var scorePrincipal = indicators.length ? indicators[0].key : (S.CATEGORY_INDICATORS[cat] || [])[0] || "renta_brute";
          var scoresSecondaires = indicators.map(function (i) { return i.key; });
          if (scoresSecondaires.length === 0) scoresSecondaires = S.CATEGORY_INDICATORS[cat] ? S.CATEGORY_INDICATORS[cat].slice(0, 1) : ["renta_brute"];

          var emptyEl = document.getElementById("comparaison-empty");
          var loadingEl = document.getElementById("comparaison-loading");
          var errorEl = document.getElementById("comparaison-error");

          if (mode === "communes" && S.selectedCommunes.length === 0) {
            errorEl.textContent = "Ajoutez au moins une commune à la liste (recherche rapide ou sélecteur) puis cliquez sur Comparer.";
            errorEl.hidden = false;
            emptyEl.style.display = "none";
            var dynErr0 = document.getElementById("comparaison-tables-dynamic");
            if (dynErr0) dynErr0.innerHTML = "";
            return;
          }
          if (mode === "departements" && getSelectedDeptCodes().length === 0) {
            errorEl.textContent = "Sélectionnez au moins un département (ou « Tous les départements » dans la liste) puis cliquez sur Comparer.";
            errorEl.hidden = false;
            emptyEl.style.display = "none";
            var dynErr1 = document.getElementById("comparaison-tables-dynamic");
            if (dynErr1) dynErr1.innerHTML = "";
            return;
          }
          if (mode === "regions" && getSelectedRegionIdsForRegionsMode().length === 0) {
            errorEl.textContent = "Sélectionnez au moins une région (ou « Toutes les régions » dans la liste) puis cliquez sur Comparer.";
            errorEl.hidden = false;
            emptyEl.style.display = "none";
            var dynErr2 = document.getElementById("comparaison-tables-dynamic");
            if (dynErr2) dynErr2.innerHTML = "";
            return;
          }
          var refCritErr = validateComparaisonRefCriteria();
          if (refCritErr) {
            errorEl.textContent = refCritErr;
            errorEl.hidden = false;
            emptyEl.style.display = "none";
            var dynRef = document.getElementById("comparaison-tables-dynamic");
            if (dynRef) dynRef.innerHTML = "";
            return;
          }
          errorEl.hidden = true;
          emptyEl.style.display = "none";
          loadingEl.classList.add("visible");
          var dynLoading = document.getElementById("comparaison-tables-dynamic");
          if (dynLoading) dynLoading.innerHTML = "";

          var jobs = buildMapJobs(scorePrincipal, cat);
          var uniqueKeys = uniqueFilterKeysFromJobs(jobs);

          var baseParams = new URLSearchParams();
          baseParams.set("mode", mode);
          baseParams.set("score_principal", scorePrincipal);
          baseParams.set("n_max", String(nMax));
          var periodeDvfEl = document.getElementById("comparaison-periode-dvf");
          var periodeDvf = periodeDvfEl ? parseInt(String(periodeDvfEl.value || "1"), 10) : 1;
          if (Number.isFinite(periodeDvf) && [1, 2, 3, 5].indexOf(periodeDvf) >= 0) {
            baseParams.set("periode_annees", String(periodeDvf));
          }
          var nbLocauxMinEl = document.getElementById("comparaison-nb-locaux-min");
          var nbLocauxMinRaw = nbLocauxMinEl ? String(nbLocauxMinEl.value || "").trim() : "";
          var nbLocauxMin = nbLocauxMinRaw === "" ? NaN : parseInt(nbLocauxMinRaw, 10);
          if (Number.isFinite(nbLocauxMin) && nbLocauxMin >= 0) {
            baseParams.set("nb_locaux_min", String(nbLocauxMin));
          }
          var rbMinEl = document.getElementById("comparaison-renta-brute-min");
          var rnMinEl = document.getElementById("comparaison-renta-nette-min");
          var rbMinRaw = rbMinEl ? String(rbMinEl.value || "").trim().replace(",", ".") : "";
          var rnMinRaw = rnMinEl ? String(rnMinEl.value || "").trim().replace(",", ".") : "";
          var rbMin = rbMinRaw === "" ? NaN : parseFloat(rbMinRaw);
          var rnMin = rnMinRaw === "" ? NaN : parseFloat(rnMinRaw);
          if (Number.isFinite(rbMin)) {
            baseParams.set("renta_brute_min", String(rbMin));
          }
          if (Number.isFinite(rnMin)) {
            baseParams.set("renta_nette_min", String(rnMin));
          }
          scoresSecondaires.forEach(function (s) {
            baseParams.append("scores_secondaires", s);
          });
          if (mode === "communes") {
            // Liste d’INSEE unifiée (manuel + masse dépt/région + mélanges) ; France entière très volumineuse → scope all_france
            var MAX_FRANCE_DIRECT_INSEE = 12000;
            var allInsee = everySelectedCommuneHasInsee();
            var franceListTooBig = S.selectionMassMode === "france" && S.selectedCommunes.length > MAX_FRANCE_DIRECT_INSEE;
            /** scope=department / region ne renvoie qu’un seul dépt/région : interdit si la liste cumule plusieurs périmètres. */
            var singleDeptSelection = distinctDeptCodesInSelection().size <= 1;
            var singleRegionSelection = distinctRegionIdsInSelection().size <= 1;
            if (allInsee && !franceListTooBig) {
              appendCommunesScopeParamsByInsee(baseParams);
            } else if (S.selectionMassMode === "france") {
              baseParams.set("scope", "all_france");
              S.excludedCommunes.forEach(function (c) {
                if (c.code_insee) baseParams.append("exclude_code_insee", c.code_insee);
              });
            } else if (!allInsee && S.selectionMassMode === "dept" && singleDeptSelection) {
              baseParams.set("scope", "department");
              var deptVal = document.getElementById("comparaison-dept").value;
              if (deptVal) baseParams.append("code_dept", normalizeDeptCodeTwoChars(deptVal) || deptVal);
              S.excludedCommunes.forEach(function (c) {
                if (c.code_insee) baseParams.append("exclude_code_insee", c.code_insee);
              });
            } else if (!allInsee && S.selectionMassMode === "region" && singleRegionSelection) {
              baseParams.set("scope", "region");
              var regVal = document.getElementById("comparaison-region").value;
              if (regVal) baseParams.append("code_region", regVal);
              S.excludedCommunes.forEach(function (c) {
                if (c.code_insee) baseParams.append("exclude_code_insee", c.code_insee);
              });
            } else {
              S.selectedCommunes.forEach(function (c) {
                baseParams.append("code_dept", normalizeDeptCodeTwoChars(c.code_dept) || String(c.code_dept || "").trim());
                baseParams.append("code_postal", c.code_postal || "");
                baseParams.append("commune", c.commune || "");
              });
            }
          } else if (mode === "departements") {
            getSelectedDeptCodes().forEach(function (code) {
              baseParams.append("code_dept", code);
            });
          } else {
            getSelectedRegionIdsForRegionsMode().forEach(function (id) {
              baseParams.append("code_region", id);
            });
          }

          fetchComparaisonScoresBatch(baseParams, uniqueKeys, scorePrincipal, cat)
            .then(function (rowsByFilterKey) {
              if (
                mode === "communes" &&
                !S.selectionMassMode &&
                distinctDeptCodesInSelection().size > 1 &&
                !communeRowsCoverSelectedDepartments(rowsByFilterKey, jobs)
              ) {
                return fetchComparaisonScoresBatch(baseParams, uniqueKeys, scorePrincipal, cat, true);
              }
              return rowsByFilterKey;
            })
            .then(function (rowsByFilterKey) {
              loadingEl.classList.remove("visible");
              var displayIndicators = indicators.length
                ? indicators
                : [{ key: scorePrincipal, label: S.CATEGORY_LABELS[cat] || getScoreLabel(scorePrincipal) }];

              var newScopeKey = buildComparaisonDistanceScopeKey();
              if (S.lastDistanceOverlay && S.lastComparaisonDistanceScopeKey != null && newScopeKey !== S.lastComparaisonDistanceScopeKey) {
                S.lastDistanceOverlay = null;
              }
              S.lastComparaisonDistanceScopeKey = newScopeKey;

              S.lastComparaisonJobs = jobs;
              S.lastRowsByFilterKey = rowsByFilterKey;
              var firstKeys = uniqueKeys.slice();
              var firstRows = [];
              for (var fi = 0; fi < firstKeys.length; fi++) {
                var rr = rowsByFilterKey[firstKeys[fi]];
                if (rr && rr.length) {
                  firstRows = rr;
                  break;
                }
              }
              S.lastComparaisonRows = firstRows.length ? firstRows : rowsByFilterKey[firstKeys[0]] || [];

              var displayMode = getDisplayModeFromRows(firstRows.length ? firstRows : S.lastComparaisonRows);
              /* ancien code : Cursor
              var theadPrefix =
                displayMode === "departements"
                  ? "<tr><th class=\"col-num\">Rang</th><th>Région</th><th class=\"col-num\">Code dépt</th><th>Département</th>"
                  : displayMode === "regions"
                  ? "<tr><th class=\"col-num\">Rang</th><th class=\"col-num\">Code région</th><th>Région</th>"
                  : "<tr><th class=\"col-num\">Rang</th><th>Région</th><th class=\"col-num\">Code dépt</th><th class=\"col-num\">Code postal</th><th>Commune</th>";
              */
              // nouveau code : Claude.ia
              var theadPrefix =
                displayMode === "departements"
                  ? "<tr><th class=\"col-num\">Rang</th><th class=\"sortable\" data-sort=\"region\">Région<span class=\"sort-icon\"></span></th><th class=\"col-num sortable\" data-sort=\"code_dept\">Code dépt<span class=\"sort-icon\"></span></th><th class=\"sortable\" data-sort=\"dep_nom\">Département<span class=\"sort-icon\"></span></th>"
                  : displayMode === "regions"
                  ? "<tr><th class=\"col-num\">Rang</th><th class=\"col-num sortable\" data-sort=\"code_region\">Code région<span class=\"sort-icon\"></span></th><th class=\"sortable\" data-sort=\"region\">Région<span class=\"sort-icon\"></span></th>"
                  : "<tr><th class=\"col-num\">Rang</th><th class=\"sortable\" data-sort=\"region\">Région<span class=\"sort-icon\"></span></th><th class=\"col-num sortable\" data-sort=\"code_dept\">Code dépt<span class=\"sort-icon\"></span></th><th class=\"col-num sortable\" data-sort=\"code_postal\">Code postal<span class=\"sort-icon\"></span></th><th class=\"sortable\" data-sort=\"commune\">Commune<span class=\"sort-icon\"></span></th>";
              // fin nouveau code : Claude.ia
              var hasAnyRows = jobs.some(function (j) {
                return (rowsByFilterKey[j.filterKey] || []).length > 0;
              });
              var hasRenderableTable = jobs.some(function (j) {
                return jobHasDisplayableData(rowsByFilterKey[j.filterKey] || [], j, cat, displayIndicators);
              });
              var hasRenderableMapValues = jobs.some(function (j) {
                return jobHasMapDisplayableData(rowsByFilterKey[j.filterKey] || [], j);
              });
              if (!hasAnyRows) {
                S.lastRenderTableArgs = null;
                S.lastDistanceOverlay = null;
                S.lastComparaisonDistanceScopeKey = null;
                emptyEl.textContent = "Aucune donnée retournée pour la sélection et les filtres demandés.";
                emptyEl.style.display = "block";
                var dynEmpty = document.getElementById("comparaison-tables-dynamic");
                if (dynEmpty) dynEmpty.innerHTML = "";
                clearComparaisonMap();
              } else if (!hasRenderableTable && !hasRenderableMapValues) {
                S.lastRenderTableArgs = null;
                S.lastDistanceOverlay = null;
                S.lastComparaisonDistanceScopeKey = null;
                emptyEl.textContent =
                  "Aucune donnée exploitable (valeurs nulles) pour les tableaux et les cartes avec les filtres demandés.";
                emptyEl.style.display = "block";
                var dynEmpty2 = document.getElementById("comparaison-tables-dynamic");
                if (dynEmpty2) dynEmpty2.innerHTML = "";
                clearComparaisonMap();
              } else {
                emptyEl.style.display = "none";
                S.lastRenderTableArgs = {
                  jobs: jobs,
                  rowsByFilterKey: rowsByFilterKey,
                  displayMode: displayMode,
                  cat: cat,
                  displayIndicators: displayIndicators,
                  theadPrefix: theadPrefix
                };
                if (hasRenderableTable) {
                  renderComparaisonTables(jobs, rowsByFilterKey, displayMode, cat, displayIndicators, theadPrefix);
                } else {
                  var dynNoTab = document.getElementById("comparaison-tables-dynamic");
                  if (dynNoTab) dynNoTab.innerHTML = "";
                }
                if (hasRenderableMapValues) {
                  renderComparaisonMap(jobs, rowsByFilterKey, displayMode, scorePrincipal);
                  // Si l'onglet cartes est déjà actif, invalider la taille immédiatement
                  var _tabMaps = document.getElementById("comparaison-tab-maps");
                  if (_tabMaps && _tabMaps.classList.contains("is-active")) {
                    [150, 500, 1000].forEach(function (d) {
                      setTimeout(function () {
                        (S.mapViz.communeMaps || []).concat(S.mapViz.multiChoroMaps || []).forEach(function (e) {
                          if (!e || !e.map) return;
                          try {
                            e.map.invalidateSize({ animate: false });
                          } catch (_) {}
                          try {
                            if (e.franceMetroFixedBounds && e.franceMetroFixedBounds.isValid && e.franceMetroFixedBounds.isValid()) {
                              e.map.fitBounds(e.franceMetroFixedBounds, e.franceMetroFitOpts || { padding: [12, 12], maxZoom: 6 });
                            } else if (e.savedBounds && e.savedBounds.isValid && e.savedBounds.isValid()) {
                              e.map.fitBounds(e.savedBounds, e.fitOpts || { padding: [12, 12], maxZoom: 11 });
                            } else if (e.layer && e.layer.getBounds) {
                              var b = e.layer.getBounds();
                              if (b && b.isValid()) e.map.fitBounds(b, { padding: [10, 10] });
                            }
                          } catch (_) {}
                        });
                      }, d);
                    });
                  }
                } else {
                  clearComparaisonMap();
                }
              }
              setMapTabVisible(hasRenderableMapValues);
            })
            .catch(function (err) {
              loadingEl.classList.remove("visible");
              S.lastRenderTableArgs = null;
              S.lastDistanceOverlay = null;
              S.lastComparaisonDistanceScopeKey = null;
              errorEl.textContent = err.message || "Erreur lors du chargement.";
              errorEl.hidden = false;
              emptyEl.style.display = "none";
              var dynCatch = document.getElementById("comparaison-tables-dynamic");
              if (dynCatch) dynCatch.innerHTML = "";
              clearComparaisonMap();
              setMapTabVisible(false);
            });
        }

        document.getElementById("comparaison-btn").addEventListener("click", runComparaison);

        function forceRecalculForMode() {
          var mode = getComparaisonMode();
          var emptyEl = document.getElementById("comparaison-empty");
          var loadingEl = document.getElementById("comparaison-loading");
          var errorEl = document.getElementById("comparaison-error");

          var params = new URLSearchParams();
          params.set("mode", mode);
          if (mode === "communes") {
            if (S.selectedCommunes.length === 0) {
              errorEl.textContent = "Ajoutez au moins une commune à la liste puis cliquez sur Forcer le recalcul.";
              errorEl.hidden = false;
              return;
            }
            S.selectedCommunes.forEach(function (c) {
              params.append("code_dept", normalizeDeptCodeTwoChars(c.code_dept) || String(c.code_dept || "").trim());
              params.append("code_postal", c.code_postal || "");
              params.append("commune", c.commune || "");
            });
          } else if (mode === "departements") {
            var depts = getSelectedDeptCodes();
            if (!depts.length) {
              errorEl.textContent = "Sélectionnez au moins un département puis cliquez sur Forcer le recalcul.";
              errorEl.hidden = false;
              return;
            }
            depts.forEach(function (d) { params.append("code_dept", d); });
          } else {
            var regs = getSelectedRegionIdsForRegionsMode();
            if (!regs.length) {
              errorEl.textContent = "Sélectionnez au moins une région puis cliquez sur Forcer le recalcul.";
              errorEl.hidden = false;
              return;
            }
            regs.forEach(function (r) { params.append("code_region", r); });
          }

          var refErrForce = validateComparaisonRefCriteria();
          if (refErrForce) {
            errorEl.textContent = refErrForce;
            errorEl.hidden = false;
            return;
          }

          errorEl.hidden = true;
          emptyEl.style.display = "none";
          loadingEl.classList.add("visible");

          fetch(S.API_BASE + "/api/force-recalcul-indicateurs?" + params.toString(), { method: "POST" })
            .then(function (r) {
              if (!r.ok) throw new Error("Erreur " + r.status + " lors du recalcul.");
              return r.json();
            })
            .then(function () {
              loadingEl.classList.remove("visible");
              runComparaison();
            })
            .catch(function (err) {
              loadingEl.classList.remove("visible");
              errorEl.textContent = err.message || "Erreur lors du recalcul.";
              errorEl.hidden = false;
            });
        }

        document.getElementById("comparaison-force-btn").addEventListener("click", forceRecalculForMode);

        function resetComparaisonUI() {
          // 1) Nettoyer l'URL (supprime les query params) sans rechargement.
          try {
            var cleanPath = window.location.pathname || "comparaison_scores.html";
            window.history.replaceState({}, document.title, cleanPath);
          } catch (e) {}

          // 2) Réinitialiser les états métier / sélections.
          S.selectedCommunes.length = 0;
          S.selectedCommunesSet.clear();
          S.listCommunes = [];
          S.lastRenderTableArgs = null;
          S.lastDistanceOverlay = null;
          S.lastComparaisonDistanceScopeKey = null;
          try {
            document.dispatchEvent(new Event("comparaison-distances-reset"));
          } catch (eEv) {}
          var dKmR = document.getElementById("distances-max-km");
          var dMinR = document.getElementById("distances-max-minutes");
          if (dKmR) dKmR.value = "";
          if (dMinR) dMinR.value = "";

          // Mode par défaut: communes.
          var modeDefault = document.querySelector('input[name="comparaison-mode"][value="communes"]');
          if (modeDefault) modeDefault.checked = true;
          switchModeUI();

          // Filtres de base.
          var regionSel = document.getElementById("comparaison-region");
          var deptSel = document.getElementById("comparaison-dept");
          var communeSel = document.getElementById("comparaison-commune");
          if (regionSel) regionSel.value = "";
          fillDepartments(); // remet aussi le select commune à l'état initial
          if (deptSel) deptSel.value = "";
          fillCommunes();
          if (communeSel) communeSel.value = "";

          // Recherche rapide + suggestions.
          var searchInput = document.getElementById("comparaison-search");
          var suggestions = document.getElementById("comparaison-suggestions");
          if (searchInput) searchInput.value = "";
          if (suggestions) {
            suggestions.innerHTML = "";
            suggestions.hidden = true;
            suggestions.setAttribute("aria-expanded", "false");
          }

          // Indicateurs / catégorie / n_max.
          var catSel = document.getElementById("comparaison-categorie");
          if (catSel) catSel.value = "rentabilite";
          var nMaxInput = document.getElementById("comparaison-n-max");
          if (nMaxInput) nMaxInput.value = "20";
          var periodeDvfReset = document.getElementById("comparaison-periode-dvf");
          if (periodeDvfReset) periodeDvfReset.value = "1";
          var nbLocauxMinReset = document.getElementById("comparaison-nb-locaux-min");
          if (nbLocauxMinReset) nbLocauxMinReset.value = "";
          var rbMinReset = document.getElementById("comparaison-renta-brute-min");
          var rnMinReset = document.getElementById("comparaison-renta-nette-min");
          if (rbMinReset) rbMinReset.value = "";
          if (rnMinReset) rnMinReset.value = "";
          S.legendCustomMin = null;
          S.legendCustomMax = null;
          ["ind-renta-brute", "ind-renta-nette", "ind-taux-tfb", "ind-taux-teom"].forEach(function (id) {
            var cb = document.getElementById(id);
            if (cb) cb.checked = true;
          });
          updateIndicatorVisibility();

          ["comparaison-granularity-type", "comparaison-granularity-surf", "comparaison-granularity-pieces"].forEach(function (nm) {
            var agg = document.querySelector('input[name="' + nm + '"][value="agg"]');
            if (agg) agg.checked = true;
          });
          fillRefSelects();
          updateSurfPiecesVisibility();
          var pt = document.getElementById("comparaison-panel-tables");
          var pm = document.getElementById("comparaison-panel-maps");
          var tt = document.getElementById("comparaison-tab-tables");
          var tm = document.getElementById("comparaison-tab-maps");
          if (pt && pm && tt && tm) {
            pt.hidden = false;
            pm.hidden = true;
            tt.classList.add("is-active");
            tm.classList.remove("is-active");
            tt.setAttribute("aria-selected", "true");
            tm.setAttribute("aria-selected", "false");
          }
          setMapTabVisible(true);

          // Mode départements : toutes les régions cochées, tous les départements décochés.
          initDepartementsModeSelectionState();

          // Mode régions : aucune sélection.
          fillRegionsMultiSelectRegionsOnly();

          // 3) Réinitialiser affichages résultats/cartes/messages.
          resetComparaisonResultsOnModeChange("communes");
          updateSelectedListForMode();
          var err = document.getElementById("comparaison-error");
          if (err) { err.hidden = true; err.textContent = ""; }

          // 4) Recalculer les liens de navigation avec URL désormais vide.
          initNavFromUrl();
        }

        document.getElementById("comparaison-reset-btn").addEventListener("click", resetComparaisonUI);

        function initNavFromUrl() {
          var sp = new URLSearchParams(window.location.search || "");
          var code_dept = sp.get("code_dept");
          var code_postal = sp.get("code_postal");
          var commune = sp.get("commune");
          if (typeof window.updateNavLinksFromCommune === "function") {
            window.updateNavLinksFromCommune(code_dept, code_postal, commune);
          }
        }

        function wireComparaisonDistancesUi() {
          initDistancesSection({
            apiBase: S.API_BASE,
            getInseeListForDistance: collectInseeListFromComparaisonResults,
            hasComparaisonResults: function () {
              return !!S.lastRenderTableArgs;
            },
            onDistanceComputed: function (results) {
              S.lastDistanceOverlay = buildDistanceOverlayFromResults(results);
              S.lastComparaisonDistanceScopeKey = buildComparaisonDistanceScopeKey();
              if (!S.lastRenderTableArgs) return;
              renderComparaisonTables(
                S.lastRenderTableArgs.jobs,
                S.lastRenderTableArgs.rowsByFilterKey,
                S.lastRenderTableArgs.displayMode,
                S.lastRenderTableArgs.cat,
                S.lastRenderTableArgs.displayIndicators,
                S.lastRenderTableArgs.theadPrefix
              );
              if (
                S.mapViz.mode === "communes" &&
                S.mapViz.lastCommuneMapScorePrincipal != null &&
                S.lastComparaisonJobs &&
                S.lastComparaisonJobs.length
              ) {
                renderComparaisonMap(S.lastComparaisonJobs, S.lastRowsByFilterKey, "communes", S.mapViz.lastCommuneMapScorePrincipal, {
                  preserveLegendBounds: true
                });
              }
            },
            onDistanceCleared: function () {
              S.lastDistanceOverlay = null;
              if (!S.lastRenderTableArgs) return;
              renderComparaisonTables(
                S.lastRenderTableArgs.jobs,
                S.lastRenderTableArgs.rowsByFilterKey,
                S.lastRenderTableArgs.displayMode,
                S.lastRenderTableArgs.cat,
                S.lastRenderTableArgs.displayIndicators,
                S.lastRenderTableArgs.theadPrefix
              );
              if (
                S.mapViz.mode === "communes" &&
                S.mapViz.lastCommuneMapScorePrincipal != null &&
                S.lastComparaisonJobs &&
                S.lastComparaisonJobs.length
              ) {
                renderComparaisonMap(S.lastComparaisonJobs, S.lastRowsByFilterKey, "communes", S.mapViz.lastCommuneMapScorePrincipal, {
                  preserveLegendBounds: true
                });
              }
            },
            onDistanceCalcBusy: function () {}
          });
          ["distances-max-km", "distances-max-minutes"].forEach(function (id) {
            var el = document.getElementById(id);
            if (!el) return;
            ["change", "input"].forEach(function (evName) {
              el.addEventListener(evName, function () {
                if (S.lastDistanceOverlay) refreshTableAndMapsAfterDistanceThresholdChange();
              });
            });
          });
        }

        fillRefSelects();

        if (document.readyState === "loading") {
          document.addEventListener("DOMContentLoaded", function () {
            initComparaisonViewTabs();
            initCommuneMapSubtabsWire();
            updateIndicatorVisibility();
            switchModeUI();
            loadGeo()
              .then(function () {
                return loadRefs();
              })
              .then(function () {
                initFromUrlParams();
                initNavFromUrl();
                switchModeUI();
                updateComparaisonCriteriaBlockVisibility();
                wireComparaisonDistancesUi();
              });
          });
        } else {
          initComparaisonViewTabs();
          initCommuneMapSubtabsWire();
          updateIndicatorVisibility();
          switchModeUI();
          loadGeo()
            .then(function () {
              return loadRefs();
            })
            .then(function () {
              initFromUrlParams();
              initNavFromUrl();
              switchModeUI();
              updateComparaisonCriteriaBlockVisibility();
              wireComparaisonDistancesUi();
            });
        }
      })();
