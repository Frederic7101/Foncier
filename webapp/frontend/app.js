const BAN_URL = "https://api-adresse.data.gouv.fr/search/";
const API_BASE_URL = "http://localhost:8000";
const ADDRESS_CACHE_KEY = "foncier_address_cache";
const ADDRESS_CACHE_MAX = 15;

// France métropolitaine : Dunkerque (N) → Perpignan (S), Brest (O) → Strasbourg (E)
const FRANCE_BOUNDS = L.latLngBounds(
  [42.70, -4.49],   // sud-ouest (Perpignan / Brest)
  [51.04, 7.75]     // nord-est (Dunkerque / Strasbourg)
);

let map;
let marker;
let searchCircle;
let lastCenter = null;
let saleMarkersLayer = null;
let selectedSaleMarker = null;
let currentVentes = [];
let sortColumn = "date_mutation";
let sortDir = -1; // 1 = croissant, -1 = décroissant
let selectedVente = null;

function initMap() {
  map = L.map("map", {
    maxBounds: FRANCE_BOUNDS,
    maxBoundsViscosity: 1,
    minZoom: 5,
    maxZoom: 19,
  });

  L.tileLayer(
    "https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&LAYER=GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2&STYLE=normal&TILEMATRIXSET=PM&FORMAT=image/png&TILEMATRIX={z}&TILECOL={x}&TILEROW={y}",
    {
      attribution:
        '&copy; <a href="https://www.ign.fr/">IGN</a> &amp; ' +
        '<a href="https://cartes.gouv.fr/">Géoplateforme</a>',
      minZoom: 2,
      maxZoom: 19,
      tms: false,
    }
  ).addTo(map);

  map.fitBounds(FRANCE_BOUNDS, { padding: [8, 8] });
}

function setMapCenter(lat, lon, rayonKm) {
  lastCenter = { lat, lon };

  if (!marker) {
    marker = L.marker([lat, lon]).addTo(map);
  } else {
    marker.setLatLng([lat, lon]);
  }

  if (!searchCircle) {
    searchCircle = L.circle([lat, lon], {
      radius: rayonKm * 1000,
      color: "#0077cc",
      fillColor: "#0077cc",
      fillOpacity: 0.1,
    }).addTo(map);
  } else {
    searchCircle.setLatLng([lat, lon]);
    searchCircle.setRadius(rayonKm * 1000);
  }

  const bounds = searchCircle.getBounds();
  map.fitBounds(bounds, { padding: [20, 20] });
}

function renderDetails(v) {
  if (!v) return "";
  const cpCommune = [padCodePostal(v.code_postal), v.commune].filter(Boolean).join(" ");
  const adresse = cpCommune ? adresseLine(v) + ", " + cpCommune : adresseLine(v);
  return `
    <table class="details-table">
      <tr><td class="details-label">Date de vente</td><td class="details-value">${formatDateDMY(v.date_mutation)}</td></tr>
      <tr><td class="details-label">Nature</td><td class="details-value">${v.nature_mutation ?? "—"}</td></tr>
      <tr><td class="details-label">Type de local</td><td class="details-value">${v.type_local ?? "—"}</td></tr>
      <tr><td class="details-label">Surface bâtie</td><td class="details-value">${v.surface_reelle_bati != null ? v.surface_reelle_bati + " m²" : "—"}</td></tr>
      <tr><td class="details-label">Surface terrain</td><td class="details-value">${v.surface_terrain != null ? v.surface_terrain + " m²" : "—"}</td></tr>
      <tr><td class="details-label">Prix de vente fiscal</td><td class="details-value">${v.valeur_fonciere.toLocaleString("fr-FR", { maximumFractionDigits: 0 })} €</td></tr>
      <tr><td class="details-label">Adresse</td><td class="details-value">${adresse}</td></tr>
      <tr><td class="details-label">Distance</td><td class="details-value">${v.distance_km.toFixed(2)} km</td></tr>
    </table>
  `;
}

function formatDateDMY(dateStr) {
  if (!dateStr) return "";
  const s = dateStr.toString().split("T")[0];
  const [y, m, d] = s.split("-");
  return d && m && y ? `${d}/${m}/${y}` : s;
}

function padCodePostal(cp) {
  if (cp == null || cp === "") return "";
  const s = String(cp).trim();
  return s.length < 5 ? s.padStart(5, "0") : s;
}

function adresseLine(v) {
  return [v.no_voie, v.type_de_voie, v.voie].filter(Boolean).join(" ") || "—";
}

function getAddressCache() {
  try {
    const raw = localStorage.getItem(ADDRESS_CACHE_KEY);
    const arr = raw ? JSON.parse(raw) : [];
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function setAddressCache(arr) {
  try {
    localStorage.setItem(ADDRESS_CACHE_KEY, JSON.stringify(arr.slice(0, ADDRESS_CACHE_MAX)));
  } catch (_) {}
}

function addAddressToCache(label, lon, lat) {
  if (!label || lon == null || lat == null) return;
  const trimmed = label.trim();
  if (!trimmed) return;
  let cache = getAddressCache();
  cache = cache.filter((e) => (e.label || "").trim().toLowerCase() !== trimmed.toLowerCase());
  cache.unshift({ label: trimmed, coordinates: [Number(lon), Number(lat)] });
  setAddressCache(cache);
}

async function searchBanAddresses(query) {
  const url = new URL(BAN_URL);
  url.searchParams.set("q", query);
  url.searchParams.set("limit", "10");

  const res = await fetch(url.toString());
  if (!res.ok) return [];
  const data = await res.json();
  const features = data.features;
  return Array.isArray(features) ? features : [];
}

function setupAddressAutocomplete() {
  const input = document.getElementById("address-input");
  const suggestions = document.getElementById("address-suggestions");
  if (!input || !suggestions) return;

  let timeoutId = null;

  function hideSuggestions() {
    suggestions.innerHTML = "";
  }

  function selectFeature(feature) {
    const label = feature?.properties?.label;
    const coords = feature?.geometry?.coordinates;
    if (label) input.value = label;
    hideSuggestions();
    if (coords && coords.length >= 2) {
      const [lon, lat] = coords;
      addAddressToCache(label, lon, lat);
      const rayonKm = parseFloat(
        document.getElementById("rayon-km").value || "1"
      );
      setMapCenter(lat, lon, rayonKm);
    }
  }

  input.addEventListener("input", () => {
    const value = input.value.trim();
    hideSuggestions();
    if (timeoutId) clearTimeout(timeoutId);
    if (value.length < 3) return;

    timeoutId = setTimeout(async () => {
      try {
        const valueLower = value.toLowerCase();
        const cached = getAddressCache().filter((e) =>
          (e.label || "").toLowerCase().includes(valueLower)
        );
        const features = await searchBanAddresses(value);
        const seenLabels = new Set();
        const toShow = [];
        cached.forEach((e) => {
          const label = (e.label || "").trim();
          if (label && !seenLabels.has(label.toLowerCase())) {
            seenLabels.add(label.toLowerCase());
            toShow.push({
              properties: { label },
              geometry: { coordinates: e.coordinates && e.coordinates.length >= 2 ? e.coordinates : [0, 0] },
            });
          }
        });
        (features || []).forEach((f) => {
          const label = (f.properties?.label ?? "").trim();
          if (label && !seenLabels.has(label.toLowerCase())) {
            seenLabels.add(label.toLowerCase());
            toShow.push(f);
          }
        });
        hideSuggestions();
        if (!toShow.length) return;
        toShow.forEach((f) => {
          const li = document.createElement("li");
          li.textContent = f.properties?.label ?? "";
          li.setAttribute("role", "option");
          li.addEventListener("mousedown", (e) => {
            e.preventDefault();
            e.stopPropagation();
            selectFeature(f);
          });
          li.addEventListener("click", (e) => {
            e.preventDefault();
            selectFeature(f);
          });
          suggestions.appendChild(li);
        });
      } catch (e) {
        console.error("Erreur autocomplete BAN", e);
      }
    }, 350);
  });

  document.addEventListener("click", (e) => {
    if (
      input.contains(e.target) ||
      suggestions.contains(e.target)
    )
      return;
    hideSuggestions();
  });
}

async function searchVentes() {
  const rayonKm = parseFloat(
    document.getElementById("rayon-km").value || "1"
  );
  const typeLocal = document.getElementById("type-local").value || "";
  const surfMin = document.getElementById("surf-min").value || "";
  const surfMax = document.getElementById("surf-max").value || "";
  const dateMin = document.getElementById("date-min").value || "";
  const dateMax = document.getElementById("date-max").value || "";
  const limit = parseInt(document.getElementById("limit-ventes").value || "50", 10);

  if (!lastCenter) {
    alert("Veuillez d'abord choisir une adresse.");
    return;
  }

  const params = new URLSearchParams();
  params.set("lat", lastCenter.lat.toString());
  params.set("lon", lastCenter.lon.toString());
  params.set("rayon_km", rayonKm.toString());
  params.set("limit", Math.min(250, Math.max(1, limit)).toString());

  if (typeLocal) params.set("type_local", typeLocal);
  if (surfMin) params.set("surf_min", surfMin);
  if (surfMax) params.set("surf_max", surfMax);
  if (dateMin) params.set("date_min", dateMin);
  if (dateMax) params.set("date_max", dateMax);

  const url = `${API_BASE_URL}/api/ventes?${params.toString()}`;

  const loadingEl = document.getElementById("results-loading");
  const searchBtn = document.getElementById("search-btn");
  if (loadingEl) loadingEl.classList.add("visible");
  if (searchBtn) searchBtn.disabled = true;

  try {
    const res = await fetch(url);
    const body = await res.json().catch(() => res.text());
    if (!res.ok) {
      const msg = typeof body === "string" ? body : (body?.detail || JSON.stringify(body));
      console.error("Erreur API", res.status, msg);
      alert("Erreur API " + res.status + " : " + msg);
      return;
    }
    updateResults(body);
  } catch (e) {
    console.error("Erreur lors de la recherche de ventes", e);
    alert("Erreur réseau ou serveur : " + (e.message || String(e)));
  } finally {
    if (loadingEl) loadingEl.classList.remove("visible");
    if (searchBtn) searchBtn.disabled = false;
  }
}

function getSortedVentes() {
  if (!currentVentes.length) return [];
  const key = sortColumn;
  const dir = sortDir;
  const cmp = (a, b) => {
    let va, vb;
    switch (key) {
      case "date_mutation":
        va = new Date(a.date_mutation).getTime();
        vb = new Date(b.date_mutation).getTime();
        return dir === 1 ? va - vb : vb - va;
      case "type_local":
        va = (a.type_local || "").toLowerCase();
        vb = (b.type_local || "").toLowerCase();
        return dir * (va < vb ? -1 : va > vb ? 1 : 0);
      case "surface_reelle_bati":
        va = a.surface_reelle_bati != null ? Number(a.surface_reelle_bati) : -Infinity;
        vb = b.surface_reelle_bati != null ? Number(b.surface_reelle_bati) : -Infinity;
        return dir === 1 ? va - vb : vb - va;
      case "valeur_fonciere":
        va = Number(a.valeur_fonciere);
        vb = Number(b.valeur_fonciere);
        return dir === 1 ? va - vb : vb - va;
      case "adresse":
        va = (adresseLine(a) || "").toLowerCase();
        vb = (adresseLine(b) || "").toLowerCase();
        return dir * (va < vb ? -1 : va > vb ? 1 : 0);
      case "commune":
        va = (a.commune || "").toLowerCase();
        vb = (b.commune || "").toLowerCase();
        return dir * (va < vb ? -1 : va > vb ? 1 : 0);
      case "distance_km":
        va = Number(a.distance_km);
        vb = Number(b.distance_km);
        return dir === 1 ? va - vb : vb - va;
      default:
        return 0;
    }
  };
  return [...currentVentes].sort(cmp);
}

function fillTableAndMarkers(ventes) {
  const tbody = document.getElementById("results-tbody");
  const details = document.getElementById("results-details");

  tbody.innerHTML = "";
  if (saleMarkersLayer && map) {
    map.removeLayer(saleMarkersLayer);
    saleMarkersLayer = null;
  }
  if (selectedSaleMarker && map) {
    map.removeLayer(selectedSaleMarker);
    selectedSaleMarker = null;
  }

  function updateSelectedMarkerOnMap(v) {
    if (!v || v.latitude == null || v.longitude == null || !map) return;
    if (selectedSaleMarker && map.hasLayer(selectedSaleMarker)) {
      selectedSaleMarker.setLatLng([v.latitude, v.longitude]);
    } else {
      selectedSaleMarker = L.circleMarker([v.latitude, v.longitude], {
        radius: 10,
        color: "#dc2626",
        fillColor: "#dc2626",
        fillOpacity: 1,
        weight: 2,
      });
      selectedSaleMarker.addTo(map);
    }
    map.setView([v.latitude, v.longitude], map.getZoom());
  }

  function selectRow(idx) {
    selectedVente = ventes[idx];
    tbody.querySelectorAll("tr").forEach((row) => row.classList.remove("selected"));
    const row = tbody.querySelector(`tr[data-idx="${idx}"]`);
    if (row) {
      row.classList.add("selected");
      row.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
    const v = ventes[idx];
    details.innerHTML = renderDetails(v);
    updateSelectedMarkerOnMap(v);
  }

  ventes.forEach((v, idx) => {
    const tr = document.createElement("tr");
    tr.setAttribute("data-idx", idx.toString());
    if (v === selectedVente) tr.classList.add("selected");
    tr.innerHTML = `
      <td>${formatDateDMY(v.date_mutation)}</td>
      <td>${v.type_local || "—"}</td>
      <td class="col-right">${v.surface_reelle_bati != null ? v.surface_reelle_bati : "—"}</td>
      <td class="col-right">${v.valeur_fonciere.toLocaleString("fr-FR", { maximumFractionDigits: 0 })} €</td>
      <td>${adresseLine(v)}</td>
      <td>${v.commune || "—"}</td>
      <td>${v.distance_km.toFixed(2)} km</td>
    `;
    tr.addEventListener("click", () => selectRow(idx));
    tbody.appendChild(tr);
  });

  saleMarkersLayer = L.layerGroup();
  ventes.forEach((v, idx) => {
    if (v.latitude == null || v.longitude == null) return;
    const circle = L.circleMarker([v.latitude, v.longitude], {
      radius: 5,
      color: "#2563eb",
      fillColor: "#2563eb",
      fillOpacity: 1,
      weight: 1,
    });
    circle.on("click", () => selectRow(idx));
    saleMarkersLayer.addLayer(circle);
  });
  if (map && saleMarkersLayer) map.addLayer(saleMarkersLayer);

  if (selectedVente && ventes.includes(selectedVente)) {
    const idx = ventes.indexOf(selectedVente);
    details.innerHTML = renderDetails(selectedVente);
    updateSelectedMarkerOnMap(selectedVente);
    const row = tbody.querySelector(`tr[data-idx="${idx}"]`);
    if (row) row.classList.add("selected");
  } else {
    details.innerHTML = "";
  }
}

function refreshResultsTable() {
  if (!currentVentes.length) return;
  fillTableAndMarkers(getSortedVentes());
}

function updateResults(ventes) {
  const summary = document.getElementById("results-summary");
  const tbody = document.getElementById("results-tbody");
  const details = document.getElementById("results-details");

  tbody.innerHTML = "";
  details.innerHTML = "";
  currentVentes = ventes || [];
  selectedVente = null;

  if (saleMarkersLayer && map) {
    map.removeLayer(saleMarkersLayer);
    saleMarkersLayer = null;
  }
  if (selectedSaleMarker && map) {
    map.removeLayer(selectedSaleMarker);
    selectedSaleMarker = null;
  }

  if (!ventes || ventes.length === 0) {
    summary.textContent = "Aucune vente trouvée pour ces critères.";
    return;
  }

  summary.textContent = `${ventes.length} vente(s) trouvée(s).`;
  updateSortButtonsActive();
  fillTableAndMarkers(getSortedVentes());
}

function updateSortButtonsActive() {
  document.querySelectorAll("#results-table th .sort-btn").forEach((b) => b.classList.remove("active"));
  const th = document.querySelector(`#results-table th[data-sort="${sortColumn}"]`);
  if (th) {
    const btn = sortDir === 1 ? th.querySelector(".sort-asc") : th.querySelector(".sort-desc");
    if (btn) btn.classList.add("active");
  }
}

function init() {
  initMap();
  setTimeout(() => {
    if (map) map.invalidateSize();
  }, 100);
  setupAddressAutocomplete();

  const searchBtn = document.getElementById("search-btn");
  searchBtn.addEventListener("click", searchVentes);

  document.getElementById("results-table").addEventListener("click", (e) => {
    const btn = e.target.closest(".sort-btn");
    if (!btn || !currentVentes.length) return;
    const th = btn.closest("th");
    const col = th && th.dataset.sort;
    if (!col) return;
    sortColumn = col;
    sortDir = btn.classList.contains("sort-asc") ? 1 : -1;
    updateSortButtonsActive();
    refreshResultsTable();
  });

  const rayonInput = document.getElementById("rayon-km");
  rayonInput.addEventListener("change", () => {
    if (!lastCenter) return;
    const rayonKm = parseFloat(rayonInput.value || "1");
    setMapCenter(lastCenter.lat, lastCenter.lon, rayonKm);
  });

  const titleEl = document.getElementById("page-title");
  fetch(`${API_BASE_URL}/api/period`)
    .then((r) => r.json())
    .then((data) => {
      const min = data.annee_min ?? 2020;
      const max = data.annee_max ?? 2025;
      titleEl.textContent = `Historique des ventes immobilières de ${min} à ${max}`;
      const dateMinInput = document.getElementById("date-min");
      const dateMaxInput = document.getElementById("date-max");
      if (dateMinInput && !dateMinInput.value) dateMinInput.value = `${min}-01-01`;
      if (dateMaxInput && !dateMaxInput.value) dateMaxInput.value = `${max}-12-31`;
    })
    .catch(() => {
      titleEl.textContent = "Historique des ventes immobilières";
    });
}

window.addEventListener("DOMContentLoaded", init);

