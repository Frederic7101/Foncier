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


# --- Mode Régions (backend requis pour listes) ---


@pytest.mark.csv("2.3.3.1")
def test_2_3_3_1_mode_regions_select(require_backend, comparaison_page):
    """Liste régions précédée de l'item « Toutes les régions » et pré-sélection de "Toutes les régions"."""
    _mode_radio(comparaison_page, "regions").check()
    sel = comparaison_page.locator("#comparaison-region-select-only")
    sel.wait_for(state="visible", timeout=10_000)
    _wait_regions_multiselect_ready(comparaison_page)
    first_opt = sel.locator("option").first
    assert "toutes les régions" in (first_opt.inner_text() or "").lower()
    # Comportement actuel : aucune option sélectionnée après remplissage
    selected = sel.evaluate("el => Array.from(el.selectedOptions).map(o => o.textContent)")
    # Si le produit doit pré-sélectionner « Toutes les régions », décommenter l’assertion suivante :
    assert len(selected) >= 1 and "régions" in (selected[0] or "").lower()
    assert isinstance(selected, list)


@pytest.mark.csv("2.3.2.1.1")
def test_2_3_2_1_1_selection_une_region(require_backend, comparaison_page):
    """Sélection d’une région (la première) → affichage de la région dans la zone « sélectionnées » (titre dynamique)."""
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
    # Vérifier que la région est bien affichée dans la zone "sélectionnées"
    comparaison_page.locator("#comparaison-selected-block").wait_for(state="visible", timeout=5_000)
    ul = comparaison_page.locator("#comparaison-selected-ul")
    assert ul.locator("li").count() >= 1 and ul.locator("li").first.inner_text() == value
    # Vérifier que le titre de la zone "sélectionnées" est bien "Régions sélectionnées"
    assert comparaison_page.locator("#comparaison-selected-block h3").inner_text() == "Régions sélectionnées"

