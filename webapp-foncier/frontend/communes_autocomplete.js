// Autocomplete mutualisé pour la recherche de communes (fiche_commune, comparaison_scores, etc.)
// Utilise l'endpoint /api/communes?q= :
// - Saisie uniquement numérique → communes dont le code postal COMMENCE par q (ex. 9 → 9xxx, 91 → 91xxx)
// - Saisie avec lettres → communes dont le nom contient q ou le code postal contient q
// Options : { inputId, suggestionsId, minChars?, onSelect(item) }

(function (global) {
  function getApiBase() {
    if (typeof API_BASE !== "undefined") return API_BASE;
    if (typeof window === "undefined") return "";
    const isLocal =
      window.location.hostname === "localhost" ||
      window.location.hostname === "127.0.0.1" ||
      window.location.protocol === "file:";
    return isLocal && window.location.port !== "8000" ? "http://localhost:8000" : "";
  }

  function initCommuneAutocomplete(options) {
    const input = document.getElementById(options.inputId);
    const listEl = document.getElementById(options.suggestionsId);
    if (!input || !listEl || typeof options.onSelect !== "function") return;

    const minChars = options.minChars != null ? options.minChars : 2;
    const API = getApiBase();

    let suggestions = [];
    let selectedIndex = -1;
    let debounceTimer = null;

    function hide() {
      listEl.innerHTML = "";
      listEl.hidden = true;
      listEl.setAttribute("aria-expanded", "false");
      selectedIndex = -1;
    }

    function render() {
      listEl.innerHTML = "";
      if (!suggestions.length) {
        hide();
        return;
      }
      suggestions.forEach(function (item, i) {
        const li = document.createElement("li");
        li.role = "option";
        li.id = options.suggestionsId + "-opt-" + i;
        const libCommune = item.commune || "";
        const libCp = item.code_postal || "";
        li.textContent = libCommune + (libCp ? " (" + libCp + ")" : "");
        li.dataset.index = String(i);
        li.addEventListener("click", function (e) {
          e.preventDefault();
          options.onSelect(item);
          input.value = "";
          hide();
        });
        listEl.appendChild(li);
      });
      listEl.hidden = false;
      listEl.setAttribute("aria-expanded", "true");
    }

    function fetchSuggestions(q) {
      const qq = (q || "").trim();
      if (qq.length < minChars) {
        hide();
        return;
      }
      var url = API + "/api/communes?q=" + encodeURIComponent(qq);
      fetch(url)
        .then(function (r) {
          return r.json();
        })
        .then(function (data) {
          suggestions = Array.isArray(data) ? data : [];
          selectedIndex = -1;
          render();
        })
        .catch(function () {
          hide();
        });
    }

    function trySelect() {
      const q = (input.value || "").trim();
      if (!q) return;
      if (selectedIndex >= 0 && suggestions[selectedIndex]) {
        options.onSelect(suggestions[selectedIndex]);
        input.value = "";
        hide();
        return;
      }
      if (suggestions.length === 1) {
        options.onSelect(suggestions[0]);
        input.value = "";
        hide();
        return;
      }
      if (suggestions.length > 0) {
        options.onSelect(suggestions[0]);
        input.value = "";
        hide();
        return;
      }
      if (q.length >= minChars) {
        fetchSuggestions(q);
      }
    }

    input.addEventListener("input", function () {
      clearTimeout(debounceTimer);
      const q = input.value || "";
      debounceTimer = setTimeout(function () {
        fetchSuggestions(q);
      }, 200);
    });

    input.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        hide();
        input.blur();
        return;
      }
      if (e.key === "Enter") {
        e.preventDefault();
        trySelect();
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        if (!suggestions.length) return;
        selectedIndex = (selectedIndex + 1) % suggestions.length;
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        if (!suggestions.length) return;
        selectedIndex = (selectedIndex - 1 + suggestions.length) % suggestions.length;
      } else {
        return;
      }
      Array.prototype.forEach.call(listEl.querySelectorAll("li"), function (el, i) {
        el.setAttribute("aria-selected", i === selectedIndex);
      });
      const opt = document.getElementById(options.suggestionsId + "-opt-" + selectedIndex);
      if (opt) opt.scrollIntoView({ block: "nearest" });
    });

    input.addEventListener("blur", function () {
      setTimeout(hide, 150);
    });
  }

  global.initCommuneAutocomplete = initCommuneAutocomplete;
})(typeof window !== "undefined" ? window : this);

