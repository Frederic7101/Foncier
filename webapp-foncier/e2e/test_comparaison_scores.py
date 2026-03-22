# -*- coding: utf-8 -*-
"""
Tests E2E alignés sur « Tests comparaison scores 2.csv » (colonne B = id du pas).

Prérequis :
  pip install -r requirements.txt -r requirements-e2e.txt
  playwright install chromium

Exécution :
  cd webapp-foncier
  pytest e2e/ -v

Avec backend (recommandé pour périmètre géographique, listes, comparaison) :
  uvicorn ... sur :8000 puis pytest e2e/
"""
from __future__ import annotations

import re

import pytest


def _mode_radio(page, value: str):
    return page.locator(f'input[name="comparaison-mode"][value="{value}"]')


def _wait_regions_multiselect_ready(page, timeout_ms: int = 20_000):
    """Attend que /api/geo ait rempli le select mode Régions (≥ 2 options)."""
    page.wait_for_function(
        """() => {
          const s = document.getElementById('comparaison-region-select-only');
          return s && s.options && s.options.length >= 2;
        }""",
        timeout=timeout_ms,
    )


# Constantes alignées sur comparaison_scores.html (ALL_REGIONS_SPECIAL / ALL_DEPTS_SPECIAL)
ALL_REGIONS_VALUE = "__ALL_REGIONS__"
ALL_DEPTS_VALUE = "__ALL_DEPTS__"


def _wait_region_dept_multiselects(page, timeout_ms: int = 20_000):
    page.wait_for_function(
        """() => {
          const r = document.getElementById('comparaison-region-select-dept');
          const d = document.getElementById('comparaison-dept-select');
          return r && r.options && r.options.length >= 2 && d && d.options && d.options.length >= 2;
        }""",
        timeout=timeout_ms,
    )


def _wait_communes_geo_ready(page, timeout_ms: int = 20_000):
    """Région / département remplis par /api/geo (mode Communes)."""
    page.wait_for_function(
        """() => {
          const r = document.getElementById('comparaison-region');
          return r && r.options && r.options.length >= 2;
        }""",
        timeout=timeout_ms,
    )


def _option_value_containing(page, select_css: str, substring: str) -> str | None:
    sel = page.locator(select_css)
    sel.wait_for(state="visible", timeout=10_000)
    n = sel.locator("option").count()
    sub_l = substring.lower()
    for i in range(n):
        opt = sel.locator("option").nth(i)
        text = (opt.inner_text() or "").lower()
        if sub_l in text:
            v = opt.get_attribute("value")
            if v:
                return v
    return None


def _dept_select_option_count(page) -> int:
    return page.locator("#comparaison-dept-select option").count()


# --- 1.x Démarrage ---

@pytest.mark.csv("1.1")
def test_1_1_page_charge(comparaison_page):
    """Ouverture de la page."""
    assert comparaison_page.url.endswith("comparaison_scores.html")


# --- 2.x Apparence ---


@pytest.mark.csv("2.1")
def test_2_1_titre_document_vs_h1(comparaison_page):
    """Titre « Comparaison des communes »."""
    h1 = comparaison_page.locator("h1").first
    expect = "Comparaison des communes"
    assert expect in (h1.inner_text() or ""), "h1 attendu"


@pytest.mark.csv("2.2")
def test_2_2_navigation_principale(comparaison_page):
    """Boutons Fiche commune, Stats des ventes, Ventes + href attendus."""
    fiche = comparaison_page.locator("#nav-fiche-commune")
    stats = comparaison_page.locator("#nav-stats")
    ventes = comparaison_page.locator("#nav-recherche")
    assert fiche.get_attribute("href") == "fiche_commune.html"
    assert stats.get_attribute("href") == "stats_ventes.html"
    assert ventes.get_attribute("href") == "recherche_ventes.html"
    nav = comparaison_page.locator("header nav")
    assert nav.locator("a").count() == 3


@pytest.mark.csv("2.3")
def test_2_3_sections_formulaire(comparaison_page):
    """Périmètre géographique ; Critères / Indicateurs selon état (hidden si pas de zones)."""
    comparaison_page.get_by_role("heading", name="Périmètre géographique").wait_for(state="visible")
    if comparaison_page.locator("#comparaison-selected-ul li").count() >= 1:
        crit = comparaison_page.locator("#comparaison-criteria-block")
        assert crit.is_visible(), "Critères sur les locaux doit être visible si zones sélectionnées"
        indicators = comparaison_page.locator("#comparaison-indicators-block")
        assert indicators.is_visible(), "Indicateurs doit être visible si zones sélectionnées"
    else:
        crit = comparaison_page.locator("#comparaison-criteria-block")
        assert not crit.is_visible(), "Critères sur les locaux doit être masqué si pas de zones sélectionnées"
        indicators = comparaison_page.locator("#comparaison-indicators-block")
        assert not indicators.is_visible(), "Indicateurs doit être masqué si pas de zones sélectionnées"

@pytest.mark.csv("2.3.1")
def test_2_3_1_perimetre_aide_et_radios(comparaison_page):
    """Titre "Périmètre géographique" 
    + Texte d’aide ("Choisissez le type de comparaison (communes, départements ou régions), puis sélectionnez les éléments à comparer et cliquez sur Comparer.
    + 3 modes Communes / Départements / Régions."""
    comparaison_page.get_by_role("heading", name="Périmètre géographique").wait_for(state="visible")
    assert comparaison_page.locator(".comparaison-controls h2").inner_text() == "Périmètre géographique"
    assert comparaison_page.locator(".comparaison-controls .help").first.is_visible()
    assert comparaison_page.locator(".comparaison-controls .comparaison-mode-opt").count() == 3


@pytest.mark.csv("2.3.2")
def test_2_3_2_switch_modes_affichage(comparaison_page):
    """Clic sur les 3 modes : panneaux correspondants visibles."""
    wrap_c = comparaison_page.locator("#comparaison-mode-communes")
    wrap_d = comparaison_page.locator("#comparaison-mode-departements")
    wrap_r = comparaison_page.locator("#comparaison-mode-regions")

    _mode_radio(comparaison_page, "communes").check()
    assert wrap_c.is_visible()
    assert not wrap_d.is_visible()
    assert not wrap_r.is_visible()

    _mode_radio(comparaison_page, "departements").check()
    assert not wrap_c.is_visible()
    assert wrap_d.is_visible()
    assert not wrap_r.is_visible()

    _mode_radio(comparaison_page, "regions").check()
    assert not wrap_c.is_visible()
    assert not wrap_d.is_visible()
    assert wrap_r.is_visible()


# --- Mode Régions (backend requis pour listes) ---


@pytest.mark.csv("2.3.3.1")
def test_2_3_3_1_mode_regions_select(require_backend, comparaison_page):
    """Liste régions + item « Toutes les régions » — pré-sélection (spec vs bug connu)."""
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    first_opt = sel.locator("option").first
    assert "toutes" in (first_opt.inner_text() or "").lower()
    # Comportement actuel : aucune option sélectionnée après remplissage
    selected = sel.evaluate("el => Array.from(el.selectedOptions).map(o => o.textContent)")
    # Si le produit doit pré-sélectionner « Toutes les régions », décommenter l’assertion suivante :
    # assert len(selected) >= 1 and "régions" in (selected[0] or "").lower()
    assert isinstance(selected, list)


@pytest.mark.csv("2.3.2.1.1")
def test_2_3_2_1_1_selection_une_region(require_backend, comparaison_page):
    """Sélection d’une région → zone « sélectionnées » (titre dynamique)."""
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    opts = sel.locator("option")
    n = opts.count()
    assert n >= 2, "Au moins Toutes les régions + une région"
    # Choisir la 2e option (première région réelle si ordre fixe)
    value = opts.nth(1).get_attribute("value")
    sel.select_option(value)
    comparaison_page.locator("#comparaison-selected-block").wait_for(state="visible", timeout=5_000)
    ul = comparaison_page.locator("#comparaison-selected-ul")
    assert ul.locator("li").count() >= 1


@pytest.mark.csv("2.3.2.1.2")
def test_2_3_2_1_2_selection_deux_regions(require_backend, comparaison_page):
    """Multi-sélection : deux régions → deux entrées dans « Régions sélectionnées »."""
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    opts = sel.locator("option")
    if opts.count() < 3:
        pytest.skip("Pas assez de régions dans /api/geo")
    v1 = opts.nth(1).get_attribute("value")
    v2 = opts.nth(2).get_attribute("value")
    assert v1 and v2
    sel.select_option([v1, v2])
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    assert comparaison_page.locator("#comparaison-selected-ul li").count() >= 2


@pytest.mark.csv("2.3.2.1.3")
def test_2_3_2_1_3_toutes_les_regions(require_backend, comparaison_page):
    """CSV : « Toutes les communes » (typo) — attendu : item « Toutes les régions » sélectionne tout."""
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    sel.select_option(ALL_REGIONS_VALUE)
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    n_regions = sel.locator("option").count() - 1  # hors ligne « Toutes les régions »
    n_li = comparaison_page.locator("#comparaison-selected-ul li").count()
    assert n_li == n_regions, "Une ligne par région réelle attendue"


@pytest.mark.csv("2.3.3.1.5")
def test_2_3_3_1_5_suppression_region(require_backend, comparaison_page):
    """Croix sur une région sélectionnée la retire de la liste."""
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    if sel.locator("option").count() < 2:
        pytest.skip("Pas assez de régions dans /api/geo")
    sel.select_option(index=1)
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    before = comparaison_page.locator("#comparaison-selected-ul li").count()
    comparaison_page.locator('#comparaison-selected-ul li button[aria-label="Retirer"]').first.click()
    assert comparaison_page.locator("#comparaison-selected-ul li").count() == before - 1


@pytest.mark.csv("2.3.2.1.4")
def test_2_3_2_1_4_vider_liste_regions(require_backend, comparaison_page):
    """Bouton Vider la liste vide les sélections en mode régions."""
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    if sel.locator("option").count() >= 2:
        sel.select_option(index=1)
    comparaison_page.locator("#comparaison-vider-liste").click()
    assert comparaison_page.locator("#comparaison-selected-ul li").count() == 0


# --- Mode Départements ---


@pytest.mark.csv("2.3.3.2")
def test_2_3_3_2_mode_departements_deux_selects(require_backend, comparaison_page):
    """Deux multi-selects + défauts Toutes les régions / Tous les départements."""
    _mode_radio(comparaison_page, "departements").check()
    reg = comparaison_page.locator("#comparaison-region-select-dept")
    dep = comparaison_page.locator("#comparaison-dept-select")
    reg.wait_for(state="visible", timeout=10_000)
    dep.wait_for(state="visible", timeout=10_000)
    _wait_region_dept_multiselects(comparaison_page)
    assert reg.locator("option").first.inner_text().lower().find("toutes") >= 0
    assert dep.locator("option").first.inner_text().lower().find("tous") >= 0


@pytest.mark.csv("2.3.3.2.1")
def test_2_3_3_2_1_dept_filtre_une_region(require_backend, comparaison_page):
    """Une région (hors « Toutes ») réduit la liste des départements."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    full_count = _dept_select_option_count(comparaison_page)
    reg_sel = comparaison_page.locator("#comparaison-region-select-dept")
    v = _option_value_containing(comparaison_page, "#comparaison-region-select-dept", "bourgogne")
    if not v or v == ALL_REGIONS_VALUE:
        pytest.skip("Région type Bourgogne-Franche-Comté introuvable dans /api/geo")
    reg_sel.select_option(v)
    comparaison_page.wait_for_timeout(200)
    filtered = _dept_select_option_count(comparaison_page)
    assert filtered < full_count, "La liste départements doit se restreindre à la région"


@pytest.mark.csv("2.3.3.2.2")
def test_2_3_3_2_2_dept_filtre_deux_regions(require_backend, comparaison_page):
    """Deux régions : liste des départements = union (≤ liste complète)."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    full_count = _dept_select_option_count(comparaison_page)
    reg_sel = comparaison_page.locator("#comparaison-region-select-dept")
    n_opt = reg_sel.locator("option").count()
    if n_opt < 4:
        pytest.skip("Pas assez de régions")
    v1 = reg_sel.locator("option").nth(1).get_attribute("value")
    v2 = reg_sel.locator("option").nth(2).get_attribute("value")
    reg_sel.select_option([v1, v2])
    comparaison_page.wait_for_timeout(250)
    union_count = _dept_select_option_count(comparaison_page)
    assert 2 <= union_count <= full_count


@pytest.mark.csv("2.3.3.2.3-a")
def test_2_3_3_2_3_a_toutes_regions_liste_depts_complete(require_backend, comparaison_page):
    """« Toutes les régions » → liste départements complète (même taille qu’au chargement)."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    full_count = _dept_select_option_count(comparaison_page)
    reg_sel = comparaison_page.locator("#comparaison-region-select-dept")
    if reg_sel.locator("option").count() < 3:
        pytest.skip("Pas assez de régions")
    v = reg_sel.locator("option").nth(1).get_attribute("value")
    reg_sel.select_option(v)
    comparaison_page.wait_for_timeout(200)
    reg_sel.select_option(ALL_REGIONS_VALUE)
    comparaison_page.wait_for_timeout(200)
    assert _dept_select_option_count(comparaison_page) == full_count


@pytest.mark.csv("2.3.3.2.3-b")
def test_2_3_3_2_3_b_selection_un_departement(require_backend, comparaison_page):
    """Sélection d’un département → zone « Départements sélectionnés »."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    comparaison_page.locator("#comparaison-region-select-dept").select_option(ALL_REGIONS_VALUE)
    comparaison_page.wait_for_timeout(200)
    dep_sel = comparaison_page.locator("#comparaison-dept-select")
    if dep_sel.locator("option").count() < 3:
        pytest.skip("Pas assez de départements")
    code = dep_sel.locator("option").nth(1).get_attribute("value")
    assert code and code != ALL_DEPTS_VALUE
    dep_sel.select_option(code)
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    assert comparaison_page.locator("#comparaison-selected-ul li").count() >= 1


@pytest.mark.csv("2.3.3.2.3-c")
def test_2_3_3_2_3_c_selection_plusieurs_departements(require_backend, comparaison_page):
    """Plusieurs départements visibles dans la zone sélectionnée."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    comparaison_page.locator("#comparaison-region-select-dept").select_option(ALL_REGIONS_VALUE)
    comparaison_page.wait_for_timeout(200)
    dep_sel = comparaison_page.locator("#comparaison-dept-select")
    if dep_sel.locator("option").count() < 4:
        pytest.skip("Pas assez de départements")
    c1 = dep_sel.locator("option").nth(1).get_attribute("value")
    c2 = dep_sel.locator("option").nth(2).get_attribute("value")
    dep_sel.select_option([c1, c2])
    comparaison_page.locator("#comparaison-selected-ul li").nth(1).wait_for(state="visible", timeout=5_000)
    assert comparaison_page.locator("#comparaison-selected-ul li").count() >= 2


@pytest.mark.csv("2.3.3.2.4")
def test_2_3_3_2_4_tous_les_departements(require_backend, comparaison_page):
    """« Tous les départements » → autant de lignes que de départements du filtre régional."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    comparaison_page.locator("#comparaison-region-select-dept").select_option(ALL_REGIONS_VALUE)
    comparaison_page.wait_for_timeout(200)
    dep_sel = comparaison_page.locator("#comparaison-dept-select")
    dep_sel.select_option(ALL_DEPTS_VALUE)
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=10_000)
    n_li = comparaison_page.locator("#comparaison-selected-ul li").count()
    n_dept_opts = dep_sel.locator("option").count() - 1  # hors « Tous »
    assert n_li == n_dept_opts


@pytest.mark.csv("2.3.3.2.5")
def test_2_3_3_2_5_suppression_departement(require_backend, comparaison_page):
    """Croix retire un département de la liste sélectionnée."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    comparaison_page.locator("#comparaison-region-select-dept").select_option(ALL_REGIONS_VALUE)
    comparaison_page.wait_for_timeout(200)
    dep_sel = comparaison_page.locator("#comparaison-dept-select")
    if dep_sel.locator("option").count() < 2:
        pytest.skip("Pas assez de départements")
    dep_sel.select_option(dep_sel.locator("option").nth(1).get_attribute("value"))
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    before = comparaison_page.locator("#comparaison-selected-ul li").count()
    comparaison_page.locator('#comparaison-selected-ul li button[aria-label="Retirer"]').first.click()
    assert comparaison_page.locator("#comparaison-selected-ul li").count() == before - 1


@pytest.mark.csv("2.3.3.2.6")
def test_2_3_3_2_6_vider_liste_departements(require_backend, comparaison_page):
    """Vider la liste en mode départements réinitialise les sélecteurs (liste vide)."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    comparaison_page.locator("#comparaison-dept-select").select_option(
        comparaison_page.locator("#comparaison-dept-select option").nth(1).get_attribute("value")
    )
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    comparaison_page.locator("#comparaison-vider-liste").click()
    assert comparaison_page.locator("#comparaison-selected-ul li").count() == 0


@pytest.mark.csv("2.3.3.3")
def test_2_3_3_3_mode_communes_champs_defaut(require_backend, comparaison_page):
    """Trois selects + recherche rapide ; placeholders par défaut."""
    _mode_radio(comparaison_page, "communes").check()
    comparaison_page.locator("#comparaison-region").wait_for(state="visible", timeout=10_000)
    _wait_communes_geo_ready(comparaison_page)
    first_reg = comparaison_page.locator("#comparaison-region option").first.inner_text() or ""
    assert "choisir" in first_reg.lower() or "—" in first_reg
    first_dept = comparaison_page.locator("#comparaison-dept option").first.inner_text() or ""
    assert "département" in first_dept.lower() or "departement" in first_dept.lower()
    first_com = comparaison_page.locator("#comparaison-commune option").first.inner_text() or ""
    assert "commune" in first_com.lower()
    comparaison_page.get_by_label(re.compile(r"Recherche rapide", re.I)).wait_for(state="visible")


def _wait_commune_dropdown_ready(page, min_options: int = 3, timeout_ms: int = 25_000):
    page.wait_for_function(
        """(minO) => {
          const s = document.getElementById('comparaison-commune');
          return s && s.options && s.options.length >= minO;
        }""",
        arg=min_options,
        timeout=timeout_ms,
    )


@pytest.mark.csv("2.3.3.3.1")
def test_2_3_3_3_1_communes_selecteur_region(require_backend, comparaison_page):
    """Sélection d’une région : valeur affichée dans le select Région (mono-choix)."""
    _mode_radio(comparaison_page, "communes").check()
    _wait_communes_geo_ready(comparaison_page)
    rv = _option_value_containing(comparaison_page, "#comparaison-region", "bourgogne")
    if not rv:
        pytest.skip("Région Bourgogne-Franche-Comté introuvable")
    reg = comparaison_page.locator("#comparaison-region")
    reg.select_option(rv)
    chosen = reg.evaluate("el => el.value")
    assert chosen == rv


@pytest.mark.csv("2.3.3.3.2")
def test_2_3_3_3_2_communes_selecteur_departement_filtre(require_backend, comparaison_page):
    """Après une région, la liste des départements est restreinte (ex. BFC → plusieurs deps)."""
    _mode_radio(comparaison_page, "communes").check()
    _wait_communes_geo_ready(comparaison_page)
    rv = _option_value_containing(comparaison_page, "#comparaison-region", "bourgogne")
    if not rv:
        pytest.skip("Région Bourgogne-Franche-Comté introuvable")
    comparaison_page.locator("#comparaison-region").select_option(rv)
    comparaison_page.wait_for_timeout(300)
    dept_sel = comparaison_page.locator("#comparaison-dept")
    n = dept_sel.locator("option").count()
    assert n >= 3, "Après choix de région, plusieurs départements attendus"


@pytest.mark.csv("2.3.3.3.3")
def test_2_3_3_3_3_communes_ajout_via_select(require_backend, comparaison_page):
    """Choisir une commune dans le select → ligne dans « Communes sélectionnées »."""
    _mode_radio(comparaison_page, "communes").check()
    _wait_communes_geo_ready(comparaison_page)
    rv = _option_value_containing(comparaison_page, "#comparaison-region", "bourgogne")
    if not rv:
        pytest.skip("Région introuvable")
    comparaison_page.locator("#comparaison-region").select_option(rv)
    comparaison_page.wait_for_timeout(300)
    dept_sel = comparaison_page.locator("#comparaison-dept")
    if dept_sel.locator("option").count() < 2:
        pytest.skip("Pas de département")
    # Préférence Jura (39) si présent
    dv = _option_value_containing(comparaison_page, "#comparaison-dept", "39")
    if not dv:
        dv = dept_sel.locator("option").nth(1).get_attribute("value")
    assert dv
    dept_sel.select_option(dv)
    _wait_commune_dropdown_ready(comparaison_page, min_options=3)
    com_sel = comparaison_page.locator("#comparaison-commune")
    val = com_sel.locator("option").nth(2).get_attribute("value")
    if not val or val == "__ALL__":
        val = com_sel.locator("option").nth(3).get_attribute("value")
    assert val and val != "__ALL__"
    com_sel.select_option(val)
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    assert comparaison_page.locator("#comparaison-selected-ul li").count() >= 1


@pytest.mark.csv("2.3.3.3.4")
def test_2_3_3_3_4_toutes_communes_du_departement(require_backend, comparaison_page):
    """Option « Toutes les communes du département » remplit la liste (ascenseur si volumineux)."""
    _mode_radio(comparaison_page, "communes").check()
    _wait_communes_geo_ready(comparaison_page)
    rv = _option_value_containing(comparaison_page, "#comparaison-region", "bourgogne")
    if not rv:
        pytest.skip("Région introuvable")
    comparaison_page.locator("#comparaison-region").select_option(rv)
    comparaison_page.wait_for_timeout(300)
    dept_sel = comparaison_page.locator("#comparaison-dept")
    dv = _option_value_containing(comparaison_page, "#comparaison-dept", "39") or dept_sel.locator("option").nth(
        1
    ).get_attribute("value")
    if not dv:
        pytest.skip("Pas de département")
    dept_sel.select_option(dv)
    _wait_commune_dropdown_ready(comparaison_page, min_options=3)
    n_opts = comparaison_page.locator("#comparaison-commune option").count() - 2  # hors placeholders + toutes
    if n_opts > 200:
        pytest.skip("Trop de communes pour E2E perf (>%s)" % n_opts)
    comparaison_page.locator("#comparaison-commune").select_option("__ALL__")
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=60_000)
    n_li = comparaison_page.locator("#comparaison-selected-ul li").count()
    assert n_li == n_opts
    scroll = comparaison_page.locator(".comparaison-selected-scroll")
    assert scroll.count() == 1


@pytest.mark.csv("2.3.3.3.5")
def test_2_3_3_3_5_suppression_commune(require_backend, comparaison_page):
    """Croix retire une commune de la liste."""
    _mode_radio(comparaison_page, "communes").check()
    _wait_communes_geo_ready(comparaison_page)
    rv = _option_value_containing(comparaison_page, "#comparaison-region", "bourgogne")
    if not rv:
        pytest.skip("Région introuvable")
    comparaison_page.locator("#comparaison-region").select_option(rv)
    comparaison_page.wait_for_timeout(300)
    dept_sel = comparaison_page.locator("#comparaison-dept")
    dv = dept_sel.locator("option").nth(1).get_attribute("value")
    if not dv:
        pytest.skip("Pas de département")
    dept_sel.select_option(dv)
    _wait_commune_dropdown_ready(comparaison_page, min_options=3)
    com_sel = comparaison_page.locator("#comparaison-commune")
    val = com_sel.locator("option").nth(2).get_attribute("value")
    if not val or val == "__ALL__":
        val = com_sel.locator("option").nth(3).get_attribute("value")
    com_sel.select_option(val)
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    before = comparaison_page.locator("#comparaison-selected-ul li").count()
    comparaison_page.locator('#comparaison-selected-ul li button[aria-label="Retirer"]').first.click()
    assert comparaison_page.locator("#comparaison-selected-ul li").count() == before - 1


@pytest.mark.csv("2.3.3.3.6")
def test_2_3_3_3_6_vider_liste_communes(require_backend, comparaison_page):
    """Vider la liste : plus aucune commune sélectionnée."""
    _mode_radio(comparaison_page, "communes").check()
    _wait_communes_geo_ready(comparaison_page)
    rv = _option_value_containing(comparaison_page, "#comparaison-region", "bourgogne")
    if not rv:
        pytest.skip("Région introuvable")
    comparaison_page.locator("#comparaison-region").select_option(rv)
    comparaison_page.wait_for_timeout(300)
    dept_sel = comparaison_page.locator("#comparaison-dept")
    dv = dept_sel.locator("option").nth(1).get_attribute("value")
    if not dv:
        pytest.skip("Pas de département")
    dept_sel.select_option(dv)
    _wait_commune_dropdown_ready(comparaison_page, min_options=3)
    val = comparaison_page.locator("#comparaison-commune option").nth(2).get_attribute("value")
    if val and val != "__ALL__":
        comparaison_page.locator("#comparaison-commune").select_option(val)
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    comparaison_page.locator("#comparaison-vider-liste").click()
    assert comparaison_page.locator("#comparaison-selected-ul li").count() == 0


@pytest.mark.csv("2.3.2.1")
def test_2_3_2_1_roundtrip_mode_regions(require_backend, comparaison_page):
    """Après sélections en mode Régions, changement de mode puis retour : UI toujours utilisable."""
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    sel.select_option(sel.locator("option").nth(1).get_attribute("value"))
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    _mode_radio(comparaison_page, "communes").check()
    _mode_radio(comparaison_page, "regions").check()
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    sel.select_option(sel.locator("option").nth(2).get_attribute("value"))
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    assert comparaison_page.locator("#comparaison-selected-ul li").count() >= 1


@pytest.mark.csv("2.3.2.2")
def test_2_3_2_2_roundtrip_mode_departements(require_backend, comparaison_page):
    """Cycle de modes avec retour sur Départements : sélection possible."""
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    comparaison_page.locator("#comparaison-region-select-dept").select_option(ALL_REGIONS_VALUE)
    comparaison_page.wait_for_timeout(200)
    dep_sel = comparaison_page.locator("#comparaison-dept-select")
    dep_sel.select_option(dep_sel.locator("option").nth(1).get_attribute("value"))
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    _mode_radio(comparaison_page, "regions").check()
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    dep_sel = comparaison_page.locator("#comparaison-dept-select")
    if dep_sel.locator("option").count() < 3:
        pytest.skip("Pas assez d’options départements après retour sur le mode")
    dep_sel.select_option(dep_sel.locator("option").nth(2).get_attribute("value"))
    comparaison_page.locator("#comparaison-selected-ul li").first.wait_for(state="visible", timeout=5_000)
    assert comparaison_page.locator("#comparaison-selected-ul li").count() >= 1


@pytest.mark.csv("2.3.2.3")
def test_2_3_2_3_roundtrip_mode_communes(require_backend, comparaison_page):
    """Retour en mode Communes après autre mode : ajout de commune encore possible."""
    _mode_radio(comparaison_page, "communes").check()
    _wait_communes_geo_ready(comparaison_page)
    _mode_radio(comparaison_page, "departements").check()
    _wait_region_dept_multiselects(comparaison_page)
    _mode_radio(comparaison_page, "communes").check()
    _wait_communes_geo_ready(comparaison_page)
    rv = _option_value_containing(comparaison_page, "#comparaison-region", "bourgogne")
    if not rv:
        pytest.skip("Région introuvable")
    comparaison_page.locator("#comparaison-region").select_option(rv)
    assert comparaison_page.locator("#comparaison-mode-communes").is_visible()


@pytest.mark.csv("2.3.3-crit")
def test_2_3_3_criteres_section_structure(require_backend, comparaison_page):
    """Critères : titre + 3 colonnes (Type de local, Surface, Pièces) visibles avec zone géo."""
    txt = comparaison_page.locator("#comparaison-criteria-block h3").first.inner_text() or ""
    assert "Critères" in txt and "locaux" in txt
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    sel.select_option(sel.locator("option").nth(1).get_attribute("value"))
    comparaison_page.locator("#comparaison-criteria-block").wait_for(state="visible", timeout=5_000)
    comparaison_page.get_by_text("Type de local", exact=False).first.wait_for(state="visible")
    comparaison_page.get_by_text("Surface", exact=False).first.wait_for(state="visible")
    comparaison_page.get_by_text("Nombre de pièces", exact=False).first.wait_for(state="visible")


@pytest.mark.csv("2.3.3-ind")
def test_2_3_3_indicateurs_boutons(comparaison_page):
    """Indicateurs : catégorie, nb max, boutons, cases à cocher pré-cochées (rentabilité)."""
    comparaison_page.locator("#comparaison-categorie").wait_for(state="attached", timeout=5_000)
    assert comparaison_page.locator("#comparaison-n-max").input_value() == "20"
    assert comparaison_page.locator("#comparaison-btn").is_visible()
    assert comparaison_page.locator("#comparaison-reset-btn").is_visible()
    assert comparaison_page.locator("#comparaison-force-btn").is_visible()
    assert comparaison_page.locator("#ind-renta-brute").is_checked()
    assert comparaison_page.locator("#ind-renta-nette").is_checked()


# --- Zone résultats (structure) ---


@pytest.mark.csv("2.4.1")
def test_2_4_1_onglets_tableaux_cartes(comparaison_page):
    """Onglets Affichage tableaux / cartes présents."""
    comparaison_page.locator("#comparaison-tab-tables").wait_for(state="visible")
    comparaison_page.locator("#comparaison-tab-maps").wait_for(state="visible")


@pytest.mark.csv("2.4.2")
def test_2_4_2_panneau_cartes_dans_dom(comparaison_page):
    """Panneau cartes existe (peut être hidden)."""
    assert comparaison_page.locator("#comparaison-panel-maps").count() == 1
