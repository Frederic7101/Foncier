// =============================================================================
// Variables Globales — À modifier ici pour changer de fournisseur ou de config
// =============================================================================

// — API & adresse
const BAN_URL = "https://api-adresse.data.gouv.fr/search/";
const BAN_REVERSE_URL = "https://api-adresse.data.gouv.fr/reverse/";
const BAN_SUGGESTIONS_LIMIT = 10;
// Backend API : utiliser localhost:8000 si page en file:// ou servie depuis un autre port (ex. 8765)
const API_BASE_URL = (function () {
  if (typeof window === "undefined" || !window.location) return "";
  const p = window.location.protocol;
  const host = window.location.hostname || "";
  const port = window.location.port || (p === "https:" ? "443" : "80");
  const isLocal = host === "localhost" || host === "127.0.0.1";
  if (p === "file:" || (isLocal && port !== "8000")) return "http://localhost:8000";
  return "";
})();

// — Cache adresses
const ADDRESS_CACHE_KEY = "foncier_address_cache";
const ADDRESS_CACHE_MAX = 15;

// — Cartographie (changer TILE_LAYER_* pour un autre fournisseur, ex. OSM, Carto, etc.)
const TILE_LAYER_URL =
  "https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&LAYER=GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2&STYLE=normal&TILEMATRIXSET=PM&FORMAT=image/png&TILEMATRIX={z}&TILECOL={x}&TILEROW={y}";
const TILE_LAYER_ATTRIBUTION =
  '&copy; <a href="https://www.ign.fr/">IGN</a> &amp; <a href="https://cartes.gouv.fr/">Géoplateforme</a>';
const TILE_LAYER_MIN_ZOOM = 2;
const TILE_LAYER_MAX_ZOOM = 19;
const MAP_MIN_ZOOM = 5;
const MAP_MAX_ZOOM = 19;
const MAP_FIT_PADDING = [8, 8];
const MAP_FIT_PADDING_CIRCLE = [20, 20];

// — Cercle de recherche (couleur et opacité)
const SEARCH_CIRCLE_COLOR = "#0077cc";
const SEARCH_CIRCLE_FILL_OPACITY = 0.1;

// — Marqueurs ventes sur la carte
const SALE_MARKER_RADIUS = 5;
const SALE_MARKER_COLOR = "#2563eb";
const SELECTED_MARKER_RADIUS = 10;
const SELECTED_MARKER_COLOR = "#dc2626";

// — Limites requêtes
const VENTES_LIMIT_MAX = 250;
const VENTES_LIMIT_DEFAULT = 50;

// — UI / timing
const DEFAULT_RAYON_KM = 1;
const BAN_DEBOUNCE_MS = 350;
const MAP_INVALIDATE_DELAY_MS = 100;

// — Carte : France métropolitaine (Dunkerque → Perpignan, Brest → Strasbourg)
const FRANCE_BOUNDS = L.latLngBounds(
  [42.70, -4.49],
  [51.04, 7.75]
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
let hasSearched = false; // true après un clic sur "Rechercher" (mode recherche)

function initMap() {
  map = L.map("map", {
    maxBounds: FRANCE_BOUNDS,
    maxBoundsViscosity: 1,
    minZoom: MAP_MIN_ZOOM,
    maxZoom: MAP_MAX_ZOOM,
  });

  L.tileLayer(TILE_LAYER_URL, {
    attribution: TILE_LAYER_ATTRIBUTION,
    minZoom: TILE_LAYER_MIN_ZOOM,
    maxZoom: TILE_LAYER_MAX_ZOOM,
    tms: false,
  }).addTo(map);

  map.fitBounds(FRANCE_BOUNDS, { padding: MAP_FIT_PADDING });
}

/** Géocodage inverse BAN : retourne { commune, postcode, lat, lon } ou null. */
async function reverseGeocode(lat, lon) {
  const url = `${BAN_REVERSE_URL}?lon=${lon}&lat=${lat}`;
  const res = await fetch(url);
  if (!res.ok) return null;
  const data = await res.json();
  const features = data.features;
  if (!Array.isArray(features) || features.length === 0) return null;
  const f = features[0];
  const city = f.properties?.city;
  const coords = f.geometry?.coordinates;
  if (!city || !coords || coords.length < 2) return null;
  const postcode = f.properties?.postcode ? String(f.properties.postcode).trim() : "";
  return { commune: city, postcode, lon: coords[0], lat: coords[1] };
}

function setCommuneOverlay(text) {
  const el = document.getElementById("map-commune-overlay");
  if (!el) return;
  el.textContent = text || "";
  el.classList.toggle("visible", !!text);
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
      color: SEARCH_CIRCLE_COLOR,
      fillColor: SEARCH_CIRCLE_COLOR,
      fillOpacity: SEARCH_CIRCLE_FILL_OPACITY,
    }).addTo(map);
  } else {
    searchCircle.setLatLng([lat, lon]);
    searchCircle.setRadius(rayonKm * 1000);
  }

  const bounds = searchCircle.getBounds();
  map.fitBounds(bounds, { padding: MAP_FIT_PADDING_CIRCLE });
}

function renderDetails(v) {
  if (!v) return "";
  const cpCommune = [padCodePostal(v.code_postal), v.commune].filter(Boolean).join(" ");
  const adresse = cpCommune ? adresseLine(v) + ", " + cpCommune : adresseLine(v);
  return `
    <table class="details-table">
      <tr><td class="details-label">Date de vente</td><td class="details-value">${formatDateDMY(v.date_mutation)}</td></tr>
      <tr><td class="details-label">Nature</td><td class="details-value">${v.nature_mutation ?? "—"}</td></tr>
      <tr><td class="details-label">Type de local</td><td class="details-value">${formatTypeLocal(v.type_local) ?? "—"}</td></tr>
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

/** Libellé affiché pour le type de local (remplace l’ancien libellé DVF). */
function formatTypeLocal(typeLocal) {
  if (!typeLocal) return typeLocal;
  const s = String(typeLocal).trim();
  if (s === "Local industriel. commercial ou assimilé" || s === "Local industriel, commercial ou assimilé") {
    return "Local industriel ou commercial";
  }
  return s;
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
  url.searchParams.set("limit", String(BAN_SUGGESTIONS_LIMIT));

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

  // Met à jour l'URL et les boutons de navigation (délégué à nav_links.js)
  function setCommuneContextFromPostalAndName(code_postal, commune) {
    if (!code_postal || !commune) return;
    const code_dept = code_postal.slice(0, 2);
    if (typeof window.updateNavLinksFromCommune === "function") {
      window.updateNavLinksFromCommune(code_dept, code_postal, commune);
    }
  }

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
        document.getElementById("rayon-km").value || String(DEFAULT_RAYON_KM)
      );
      setMapCenter(lat, lon, rayonKm);
      // Tenter de déduire code postal / commune à partir des propriétés BAN ou du label
      let cp = feature?.properties?.postcode || "";
      let city = feature?.properties?.city || "";
      if ((!cp || !city) && typeof label === "string") {
        const m = label.match(/(\d{5})\s+(.+)$/);
        if (m) {
          cp = cp || m[1];
          city = city || m[2];
        }
      }
      if (cp && city) setCommuneContextFromPostalAndName(cp, city);
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
    document.getElementById("rayon-km").value || String(DEFAULT_RAYON_KM)
  );
  const typeLocal = document.getElementById("type-local").value || "";
  const surfMin = document.getElementById("surf-min").value || "";
  const surfMax = document.getElementById("surf-max").value || "";
  const dateMin = document.getElementById("date-min").value || "";
  const dateMax = document.getElementById("date-max").value || "";
  const limit = parseInt(document.getElementById("limit-ventes").value || String(VENTES_LIMIT_DEFAULT), 10);

  if (!lastCenter) {
    alert("Veuillez d'abord choisir une adresse.");
    return;
  }

  const params = new URLSearchParams();
  params.set("lat", lastCenter.lat.toString());
  params.set("lon", lastCenter.lon.toString());
  params.set("rayon_km", rayonKm.toString());
  params.set("limit", Math.min(VENTES_LIMIT_MAX, Math.max(1, limit)).toString());

  if (typeLocal) params.set("type_local", typeLocal);
  if (surfMin) params.set("surf_min", surfMin);
  if (surfMax) params.set("surf_max", surfMax);
  if (dateMin) params.set("date_min", dateMin);
  if (dateMax) params.set("date_max", dateMax);

  hasSearched = true;
  setCommuneOverlay("");

  const url = `${API_BASE_URL}/api/ventes?${params.toString()}`;

  const loadingEl = document.getElementById("results-loading");
  const searchBtn = document.getElementById("search-btn");
  if (loadingEl) loadingEl.classList.add("visible");
  if (searchBtn) searchBtn.disabled = true;

  try {
    const res = await fetch(url);
    const text = await res.text();
    let body;
    try {
      body = text ? JSON.parse(text) : null;
    } catch (_) {
      body = text;
    }
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
    const tip = `${formatTypeLocal(v.type_local) || "—"} · ${(v.valeur_fonciere != null ? v.valeur_fonciere.toLocaleString("fr-FR", { maximumFractionDigits: 0 }) + " €" : "—")} · ${adresseLine(v)}`;
    if (selectedSaleMarker && map.hasLayer(selectedSaleMarker)) {
      selectedSaleMarker.setLatLng([v.latitude, v.longitude]);
      selectedSaleMarker.setTooltipContent(tip);
    } else {
      selectedSaleMarker = L.circleMarker([v.latitude, v.longitude], {
        radius: SELECTED_MARKER_RADIUS,
        color: SELECTED_MARKER_COLOR,
        fillColor: SELECTED_MARKER_COLOR,
        fillOpacity: 1,
        weight: 2,
      });
      selectedSaleMarker.bindTooltip(tip, {
        permanent: false,
        direction: "top",
        className: "sale-tooltip",
        offset: [0, -SELECTED_MARKER_RADIUS - 2],
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
      <td>${formatTypeLocal(v.type_local) || "—"}</td>
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
    const tip = `${formatTypeLocal(v.type_local) || "—"} · ${(v.valeur_fonciere != null ? v.valeur_fonciere.toLocaleString("fr-FR", { maximumFractionDigits: 0 }) + " €" : "—")} · ${adresseLine(v)}`;
    const circle = L.circleMarker([v.latitude, v.longitude], {
      radius: SALE_MARKER_RADIUS,
      color: SALE_MARKER_COLOR,
      fillColor: SALE_MARKER_COLOR,
      fillOpacity: 1,
      weight: 1,
    });
    circle.bindTooltip(tip, {
      permanent: false,
      direction: "top",
      className: "sale-tooltip",
      offset: [0, -SALE_MARKER_RADIUS - 2],
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

  // Adapter le rayon : unité = arrondi à l'entier de (distance max × 110 %), rayon = entier supérieur
  const maxDist = Math.max(...ventes.map((v) => Number(v.distance_km) || 0));
  const rayonAdapte = Math.max(1, Math.ceil(maxDist * 1.1));
  if (lastCenter && searchCircle && rayonAdapte > 0) {
    searchCircle.setRadius(rayonAdapte * 1000);
    const rayonInput = document.getElementById("rayon-km");
    if (rayonInput) rayonInput.value = String(rayonAdapte);
    const bounds = searchCircle.getBounds();
    map.fitBounds(bounds, { padding: MAP_FIT_PADDING_CIRCLE });
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
  }, MAP_INVALIDATE_DELAY_MS);
  setupAddressAutocomplete();

  // Pré-remplir l'adresse et lien fiche commune depuis l'URL (ex. depuis fiche : ?commune=Lyon&code_postal=69001&code_dept=69)
  const urlParams = new URLSearchParams(window.location.search || "");
  const commune = urlParams.get("commune")?.trim();
  const code_postal = urlParams.get("code_postal")?.trim();
  const code_dept = urlParams.get("code_dept")?.trim();
  if (typeof window.updateNavLinksFromCommune === "function") {
    window.updateNavLinksFromCommune(code_dept, code_postal, commune);
  }
  if (commune && code_postal) {
    const addressInput = document.getElementById("address-input");
    if (addressInput) {
      addressInput.value = `${code_postal} ${commune}`;
      // Géocoder pour que lastCenter soit défini et "Rechercher" fonctionne sans resaisie
      (async () => {
        try {
          const features = await searchBanAddresses(addressInput.value.trim());
          if (features && features.length > 0) {
            const coords = features[0].geometry?.coordinates;
            if (coords && coords.length >= 2) {
              const [lon, lat] = coords;
              const rayonKm = parseFloat(document.getElementById("rayon-km")?.value || String(DEFAULT_RAYON_KM));
              setMapCenter(lat, lon, rayonKm);
              if (!marker) marker = L.marker([lat, lon]).addTo(map);
              else marker.setLatLng([lat, lon]);
            }
          }
        } catch (e) {
          console.error("Géocodage adresse pré-remplie", e);
        }
      })();
    }
    // Si l'URL contient déjà la commune / code postal, conserver ce contexte (navs déjà mis à jour plus haut).
  }

  // Mode exploration : clic sur la carte → commune la plus proche, affichage discret, zoom 2 km
  map.on("click", async (e) => {
    // En mode recherche : n'appliquer l'exploration que si le clic est en dehors du cercle
    if (hasSearched && searchCircle) {
      const center = searchCircle.getLatLng();
      const radiusM = searchCircle.getRadius();
      const distM = map.distance(center, e.latlng);
      if (distM <= radiusM) return;
    }
    const { lat, lng: lon } = e.latlng;
    setCommuneOverlay("…");
    try {
      const result = await reverseGeocode(lat, lon);
      if (result) {
        setCommuneOverlay(result.commune);
        const addressLabel = result.postcode
          ? `${padCodePostal(result.postcode)} ${result.commune}`
          : result.commune;
        const addressInput = document.getElementById("address-input");
        if (addressInput) addressInput.value = addressLabel;
        lastCenter = { lat: result.lat, lon: result.lon };
        if (!marker) {
          marker = L.marker([result.lat, result.lon]).addTo(map);
        } else {
          marker.setLatLng([result.lat, result.lon]);
        }
        const rayonCommune = 2;
        if (!searchCircle) {
          searchCircle = L.circle([result.lat, result.lon], {
            radius: rayonCommune * 1000,
            color: SEARCH_CIRCLE_COLOR,
            fillColor: SEARCH_CIRCLE_COLOR,
            fillOpacity: SEARCH_CIRCLE_FILL_OPACITY,
          }).addTo(map);
        } else {
          searchCircle.setLatLng([result.lat, result.lon]);
          searchCircle.setRadius(rayonCommune * 1000);
        }
        const rayonInput = document.getElementById("rayon-km");
        if (rayonInput) rayonInput.value = String(rayonCommune);
        const bounds = searchCircle.getBounds();
        map.fitBounds(bounds, { padding: MAP_FIT_PADDING_CIRCLE });
      } else {
        setCommuneOverlay("");
      }
    } catch (err) {
      console.error("Erreur géocodage inverse", err);
      setCommuneOverlay("");
    }
  });

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
    const rayonKm = parseFloat(rayonInput.value || String(DEFAULT_RAYON_KM));
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

  // Bouton Réinitialiser : remettre à zéro les champs, les résultats, l'URL et les boutons de nav
  const resetBtn = document.getElementById("reset-btn");
  if (resetBtn) {
      resetBtn.addEventListener("click", () => {
        // 1) Réinitialiser tous les champs
      const addressInput = document.getElementById("address-input");
      const typeLocal = document.getElementById("type-local");
      const surfMin = document.getElementById("surf-min");
      const surfMax = document.getElementById("surf-max");
      const rayonKm = document.getElementById("rayon-km");
      const dateMin = document.getElementById("date-min");
      const dateMax = document.getElementById("date-max");
      const limitVentes = document.getElementById("limit-ventes");
      if (addressInput) addressInput.value = "";
      if (typeLocal) typeLocal.value = "";
      if (surfMin) surfMin.value = "";
      if (surfMax) surfMax.value = "";
      if (rayonKm) rayonKm.value = String(DEFAULT_RAYON_KM);
      if (dateMin) dateMin.value = "";
      if (dateMax) dateMax.value = "";
      if (limitVentes) limitVentes.value = "50";

      // 2) Nettoyer carte + résultats
      hasSearched = false;
      lastCenter = null;
      if (marker) {
        map.removeLayer(marker);
        marker = null;
      }
      if (searchCircle) {
        map.removeLayer(searchCircle);
        searchCircle = null;
      }
      if (saleMarkersLayer && map) {
        map.removeLayer(saleMarkersLayer);
        saleMarkersLayer = null;
      }
      if (map) map.fitBounds(FRANCE_BOUNDS, { padding: MAP_FIT_PADDING });
      const tbody = document.getElementById("results-tbody");
      if (tbody) tbody.innerHTML = "";
      const summary = document.getElementById("results-summary");
      if (summary) summary.textContent = "";
      const loading = document.getElementById("results-loading");
      if (loading) loading.setAttribute("aria-hidden", "true");

      // 3) Réinitialiser l’URL
      // 3) Réinitialiser URL et boutons nav (délégué à nav_links.js)
      if (typeof window.updateNavLinksFromCommune === "function") {
        window.updateNavLinksFromCommune();
      }
    });
  }
}

window.addEventListener("DOMContentLoaded", init);

