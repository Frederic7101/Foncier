export var DISTANCES_ADDR_STORAGE_KEY = "foncier_comparaison_distances_adresses_v1";
/** Cache navigateur : origine (lat|lon arrondis) → { code_insee → ligne résultat } */
export var DISTANCES_RESULT_CACHE_KEY = "foncier_distances_origin_insee_v1";

function normalizeInseeForCache(raw) {
  var t = String(raw || "").trim().replace(/\s/g, "");
  if (!/^\d+$/.test(t)) return t;
  while (t.length < 5) t = "0" + t;
  return t.length > 5 ? t.slice(-5) : t;
}

function makeDistanceOrigKey(lat, lon) {
  var la = Math.round(Number(lat) * 1e5) / 1e5;
  var lo = Math.round(Number(lon) * 1e5) / 1e5;
  return la.toFixed(5) + "|" + lo.toFixed(5);
}

function loadDistancesResultCache() {
  try {
    var raw = localStorage.getItem(DISTANCES_RESULT_CACHE_KEY);
    if (!raw) return {};
    var o = JSON.parse(raw);
    return typeof o === "object" && o !== null ? o : {};
  } catch (e) {
    return {};
  }
}

function mergeResultsIntoOriginCache(origKey, results) {
  if (!origKey || !results || !results.length) return;
  var all = loadDistancesResultCache();
  var bucket = all[origKey] || {};
  results.forEach(function (r) {
    if (!r || r.code_insee == null) return;
    bucket[normalizeInseeForCache(r.code_insee)] = r;
  });
  all[origKey] = bucket;
  try {
    localStorage.setItem(DISTANCES_RESULT_CACHE_KEY, JSON.stringify(all));
  } catch (e) {
    console.log("[distances] cache merge", e);
  }
}

export function initDistancesSection(opts) {
  opts = opts || {};
  var getInseeListForDistance = opts.getInseeListForDistance || function () {
    return [];
  };
  var apiBase = opts.apiBase || "";
  var onDistanceComputed = opts.onDistanceComputed || function () {};
  var onDistanceCleared = opts.onDistanceCleared || function () {};
  var onDistanceCalcBusy = opts.onDistanceCalcBusy || function () {};
  var hasComparaisonResults = opts.hasComparaisonResults || function () {
    return false;
  };
  function log() {
    var a = ["[distances]"];
    for (var i = 0; i < arguments.length; i++) a.push(arguments[i]);
    console.log.apply(console, a);
  }
  var BAN_SEARCH = "https://api-adresse.data.gouv.fr/search/";
  var toggleBtn = document.getElementById("distances-toggle-btn");
  var body = document.getElementById("distances-body");
  var addrInput = document.getElementById("distances-address");
  var suggestionsUl = document.getElementById("distances-suggestions");
  var calcBtn = document.getElementById("distances-btn-calc");
  var statusEl = document.getElementById("distances-status");
  var recentEl = document.getElementById("distances-recent");
  var maxKmEl = document.getElementById("distances-max-km");
  var maxMinEl = document.getElementById("distances-max-minutes");
  if (!toggleBtn || !body || !addrInput || !calcBtn) {
    log("init ignoré : éléments DOM manquants");
    return;
  }

  var distAddrCoords = null; // {lat, lon, label}

  function loadRecentAddresses() {
    try {
      var raw = localStorage.getItem(DISTANCES_ADDR_STORAGE_KEY);
      if (!raw) return [];
      var arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr : [];
    } catch (e) {
      log("localStorage lecture", e);
      return [];
    }
  }
  function saveRecentAddress(entry) {
    if (!entry || entry.lat == null || entry.lon == null) return;
    var list = loadRecentAddresses().filter(function (x) {
      return !(x && x.lat === entry.lat && x.lon === entry.lon);
    });
    list.unshift({ label: entry.label || "", lat: entry.lat, lon: entry.lon });
    list = list.slice(0, 8);
    try {
      localStorage.setItem(DISTANCES_ADDR_STORAGE_KEY, JSON.stringify(list));
    } catch (e) {
      log("localStorage écriture", e);
    }
    renderRecentAddresses();
  }
  function renderRecentAddresses() {
    if (!recentEl) return;
    recentEl.innerHTML = "";
    var list = loadRecentAddresses();
    if (!list.length) {
      recentEl.hidden = true;
      return;
    }
    recentEl.hidden = false;
    var lab = document.createElement("span");
    lab.className = "distances-recent-label";
    lab.textContent = "Récentes :";
    recentEl.appendChild(lab);
    list.forEach(function (item) {
      var b = document.createElement("button");
      b.type = "button";
      b.title = item.label || "";
      b.textContent = item.label || "(sans libellé)";
      b.addEventListener("click", function () {
        distAddrCoords = { lat: item.lat, lon: item.lon, label: item.label || "" };
        addrInput.value = item.label || "";
        calcBtn.disabled = false;
        log("adresse récente appliquée", { lat: item.lat, lon: item.lon });
      });
      recentEl.appendChild(b);
    });
  }

  toggleBtn.addEventListener("click", function () {
    var isOpen = !body.hidden;
    body.hidden = isOpen;
    toggleBtn.classList.toggle("is-open", !isOpen);
    if (!body.hidden) {
      var _insee = getInseeListForDistance() || [];
      log("section ouverte", {
        inseeCountTableau: _insee.length,
        apiBase: apiBase || "(relatif)",
        sampleInsee: _insee.slice(0, 5)
      });
      renderRecentAddresses();
    }
  });

  function cachedFeaturesMatching(q, maxN) {
    var ql = q.toLowerCase();
    var list = loadRecentAddresses();
    var out = [];
    for (var i = 0; i < list.length && out.length < maxN; i++) {
      var item = list[i];
      var lab = (item.label || "").toLowerCase();
      if (lab.indexOf(ql) !== -1) {
        out.push({
          type: "Feature",
          geometry: { type: "Point", coordinates: [item.lon, item.lat] },
          properties: { label: item.label || "", _fromCache: true }
        });
      }
    }
    return out;
  }
  function renderSuggestionList(feats) {
    if (!feats.length) {
      suggestionsUl.hidden = true;
      return;
    }
    suggestionsUl.innerHTML = feats
      .map(function (f, i) {
        var label = (f.properties && f.properties.label) || "";
        var isC = f.properties && f.properties._fromCache;
        return (
          '<li data-idx="' +
          i +
          '"' +
          (isC ? ' class="distances-suggestion-cached"' : "") +
          ">" +
          label +
          "</li>"
        );
      })
      .join("");
    suggestionsUl._features = feats;
    suggestionsUl.hidden = false;
  }

  // Autocomplétion : adresses en cache en tête, puis résultats BAN
  var banDebounce = null;
  addrInput.addEventListener("input", function () {
    clearTimeout(banDebounce);
    var q = addrInput.value.trim();
    if (q.length < 1) {
      suggestionsUl.hidden = true;
      return;
    }
    if (q.length < 3) {
      renderSuggestionList(cachedFeaturesMatching(q, 10));
      return;
    }
    banDebounce = setTimeout(function () {
      var cachedFirst = cachedFeaturesMatching(q, 5);
      var seen = {};
      cachedFirst.forEach(function (f) {
        var lab = (f.properties && f.properties.label) || "";
        var coords = f.geometry && f.geometry.coordinates;
        seen[lab + "|" + (coords ? coords.join(",") : "")] = true;
      });
      fetch(BAN_SEARCH + "?q=" + encodeURIComponent(q) + "&limit=8")
        .then(function (r) {
          return r.json();
        })
        .then(function (data) {
          var feats = (data && data.features) || [];
          var merged = cachedFirst.slice();
          feats.forEach(function (f) {
            if (merged.length >= 12) return;
            var lab = (f.properties && f.properties.label) || "";
            var coords = f.geometry && f.geometry.coordinates;
            var key = lab + "|" + (coords ? coords.join(",") : "");
            if (seen[key]) return;
            seen[key] = true;
            merged.push(f);
          });
          if (!merged.length) {
            suggestionsUl.hidden = true;
            return;
          }
          renderSuggestionList(merged);
        })
        .catch(function (e) {
          log("BAN fetch erreur", e);
          renderSuggestionList(cachedFirst);
        });
    }, 300);
  });

  suggestionsUl.addEventListener("click", function (e) {
    var li = e.target.closest("li");
    if (!li) return;
    var idx = parseInt(li.getAttribute("data-idx"), 10);
    var feats = suggestionsUl._features || [];
    var feat = feats[idx];
    if (!feat) return;
    var coords = feat.geometry && feat.geometry.coordinates;
    var label = (feat.properties && feat.properties.label) || "";
    if (coords && coords.length >= 2) {
      distAddrCoords = { lon: coords[0], lat: coords[1], label: label };
      addrInput.value = label;
      calcBtn.disabled = false;
      saveRecentAddress(distAddrCoords);
      log("BAN sélection", { label: label, lat: distAddrCoords.lat, lon: distAddrCoords.lon });
    }
    suggestionsUl.hidden = true;
  });

  document.addEventListener("click", function (e) {
    if (!suggestionsUl.contains(e.target) && e.target !== addrInput) {
      suggestionsUl.hidden = true;
    }
  });

  addrInput.addEventListener("change", function () {
    if (!addrInput.value.trim()) {
      distAddrCoords = null;
      calcBtn.disabled = true;
      onDistanceCleared();
    }
  });

  calcBtn.addEventListener("click", function () {
    if (!distAddrCoords) {
      log("calcul refusé : pas de coordonnées adresse");
      return;
    }

    var codeInseeList = getInseeListForDistance() || [];
    log("clic calcul", {
      codeInseeCount: codeInseeList.length,
      sampleInsee: codeInseeList.slice(0, 8),
      adresse: { label: distAddrCoords.label, lat: distAddrCoords.lat, lon: distAddrCoords.lon }
    });
    if (!codeInseeList.length) {
      statusEl.textContent =
        "Aucun code INSEE issu du tableau de comparaison. Lancez « Comparer » avec des communes affichées dans les résultats, puis réessayez.";
      statusEl.className = "distances-status error";
      statusEl.hidden = false;
      return;
    }

    var origKeyCache = makeDistanceOrigKey(distAddrCoords.lat, distAddrCoords.lon);
    var cacheAll = loadDistancesResultCache();
    var bucketCache = cacheAll[origKeyCache] || {};
    var allHitCache = codeInseeList.every(function (ci) {
      return bucketCache[normalizeInseeForCache(ci)];
    });
    if (allHitCache) {
      var fromCacheOnly = codeInseeList.map(function (ci) {
        return bucketCache[normalizeInseeForCache(ci)];
      });
      try {
        onDistanceCalcBusy(true);
      } catch (eBusyC) {}
      calcBtn.disabled = true;
      statusEl.textContent = "Chargement depuis le cache…";
      statusEl.className = "distances-status";
      statusEl.hidden = false;
      setTimeout(function () {
        try {
          onDistanceComputed(fromCacheOnly);
        } catch (eCb0) {
          log("onDistanceComputed erreur (cache)", eCb0);
        }
        if (hasComparaisonResults()) {
          statusEl.textContent =
            fromCacheOnly.length +
            " commune(s) traitée(s). Les colonnes distance et trajet ont été ajoutées au tableau des résultats.";
        } else {
          statusEl.textContent =
            fromCacheOnly.length +
            " commune(s) traitée(s). Les résultats seront reliés au tableau après un « Comparer ».";
        }
        calcBtn.disabled = false;
        try {
          onDistanceCalcBusy(false);
        } catch (eBusyC2) {}
        log("calcul terminé (cache navigateur)", { resultCount: fromCacheOnly.length });
      }, 20);
      return;
    }

    try {
      onDistanceCalcBusy(true);
    } catch (eBusy0) {}
    calcBtn.disabled = true;
    statusEl.textContent = "Calcul en cours pour " + codeInseeList.length + " commune(s) du tableau...";
    statusEl.className = "distances-status";
    statusEl.hidden = false;

    var BATCH = 1000;
    var allResults = [];
    var batches = [];
    for (var bi = 0; bi < codeInseeList.length; bi += BATCH) {
      batches.push(codeInseeList.slice(bi, bi + BATCH));
    }

    var done = 0;
    var total = codeInseeList.length;
    function runBatch(idx) {
      if (idx >= batches.length) {
        statusEl.className = "distances-status";
        try {
          onDistanceComputed(allResults);
        } catch (eCb) {
          log("onDistanceComputed erreur", eCb);
        }
        if (hasComparaisonResults()) {
          statusEl.textContent =
            allResults.length +
            " commune(s) traitée(s). Les colonnes distance et trajet ont été ajoutées au tableau des résultats.";
        } else {
          statusEl.textContent =
            allResults.length +
            " commune(s) traitée(s). Les résultats seront reliés au tableau après un « Comparer ».";
        }
        calcBtn.disabled = false;
        try {
          onDistanceCalcBusy(false);
        } catch (eBusy1) {}
        log("calcul terminé", { resultCount: allResults.length });
        return;
      }
      var batch = batches[idx];
      /* force_recalcul : réservé côté API (recalcul explicite si un cache serveur est ajouté plus tard). */
      var payload = {
        adresse_label: distAddrCoords.label,
        adresse_lat: distAddrCoords.lat,
        adresse_lon: distAddrCoords.lon,
        code_insee_list: batch,
        force_recalcul: false
      };

      var url = (apiBase || "") + "/api/distances-communes";
      log("fetch batch", { idx: idx, batchSize: batch.length, url: url });
      fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      })
        .then(function (r) {
          log("réponse HTTP", r.status, r.ok);
          if (!r.ok) {
            return r.text().then(function (t) {
              log("corps erreur brut", t);
              var detail = t;
              try {
                var j = JSON.parse(t);
                if (j && j.detail !== undefined) detail = j.detail;
              } catch (parseErr) {
                log("parse JSON erreur", parseErr);
              }
              throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
            });
          }
          return r.json();
        })
        .then(function (data) {
          var results = data.results || [];
          mergeResultsIntoOriginCache(origKeyCache, results);
          results.forEach(function (r) {
            allResults.push(r);
          });
          done += batch.length;
          statusEl.textContent = "Calcul en cours... " + done + " / " + total;
          runBatch(idx + 1);
        })
        .catch(function (err) {
          log("fetch catch", err);
          statusEl.textContent = "Erreur : " + (err.message || err);
          statusEl.className = "distances-status error";
          calcBtn.disabled = false;
          try {
            onDistanceCalcBusy(false);
          } catch (eBusy2) {}
        });
    }
    runBatch(0);
  });

  document.addEventListener("comparaison-distances-reset", function () {
    distAddrCoords = null;
    calcBtn.disabled = true;
  });

  renderRecentAddresses();
  log("initDistancesSection OK", {
    inseeTableau: (getInseeListForDistance() || []).length,
    apiBase: apiBase || "(relatif)"
  });
}