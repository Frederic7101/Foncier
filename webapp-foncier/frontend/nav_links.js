/**
 * Mise à jour des boutons de navigation (Fiche commune, Stats des ventes, Ventes, Comparaison communes)
 * selon le contexte commune. Utilisé par recherche_ventes.html, stats_ventes.html et fiche_commune.html.
 * Les liens sont ciblés par id pour rester valides après la première mise à jour du href.
 * Libellés homogènes : nav-stats = "Stats des ventes", nav-recherche / nav-ventes = "Ventes", nav-comparaison-scores = "Comparaison communes".
 */
(function (global) {
  function updateNavLinksFromCommune(code_dept, code_postal, commune) {
    const baseUrl = window.location.origin + window.location.pathname;
    const navFiche = document.getElementById("nav-fiche-commune");
    const navStats = document.getElementById("nav-stats");
    const navRecherche = document.getElementById("nav-recherche");
    const navVentes = document.getElementById("nav-ventes");
    const navComparaison = document.getElementById("nav-comparaison-scores");

    if (code_dept && code_postal && commune) {
      const q = "code_dept=" + encodeURIComponent(code_dept) + "&code_postal=" + encodeURIComponent(code_postal) + "&commune=" + encodeURIComponent(commune);
      window.history.replaceState({}, document.title, baseUrl + "?" + q);
      if (navFiche) {
        navFiche.href = "fiche_commune.html?" + q;
        navFiche.textContent = "Fiche commune →";
      }
      if (navStats) {
        navStats.href = "stats_ventes.html?" + q;
        navStats.textContent = "Stats des ventes →";
      }
      if (navRecherche) {
        navRecherche.href = "recherche_ventes.html?" + q;
        navRecherche.textContent = "Ventes →";
      }
      if (navVentes) {
        navVentes.href = "recherche_ventes.html?" + q;
        navVentes.textContent = "Ventes →";
      }
      if (navComparaison) {
        navComparaison.href = "comparaison_scores.html?" + q;
        navComparaison.textContent = "Comparaison communes →";
      }
    } else {
      window.history.replaceState({}, document.title, baseUrl);
      if (navFiche) {
        navFiche.href = "fiche_commune.html";
        navFiche.textContent = "Fiche commune";
      }
      if (navStats) {
        navStats.href = "stats_ventes.html";
        navStats.textContent = "Stats des ventes";
      }
      if (navRecherche) {
        navRecherche.href = "recherche_ventes.html";
        navRecherche.textContent = "Ventes";
      }
      if (navVentes) {
        navVentes.href = "recherche_ventes.html";
        navVentes.textContent = "Ventes";
      }
      if (navComparaison) {
        navComparaison.href = "comparaison_scores.html";
        navComparaison.textContent = "Comparaison communes";
      }
    }
  }

  global.updateNavLinksFromCommune = updateNavLinksFromCommune;
})(typeof window !== "undefined" ? window : this);
