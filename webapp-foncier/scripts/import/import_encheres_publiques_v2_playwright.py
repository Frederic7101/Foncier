#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
import_encheres_publiques.py

Script d'import des annonces immobilières depuis encheres-publiques.com,
en scrolant jusqu'en bas avec Playwright.

Il parcourt 3 pages :
- /evenements/immobilier
- /encheres/immobilier
- /resultats/immobilier

et stocke toutes les annonces (cards) dans un CSV unique, avec un champ "source"
indiquant la page d'origine (evenements / encheres / resultats).

Usage :
    python import_encheres_publiques.py --csv encheres_publiques.csv --headless

Prérequis :
    pip install playwright
    playwright install
"""

import argparse
import csv
import sys
import time
from typing import Dict, List

from playwright.sync_api import sync_playwright, Page


def debug_print(enabled: bool, *args, **kwargs) -> None:
    if enabled:
        print(*args, file=sys.stderr, **kwargs)


def auto_scroll(page: Page, debug: bool = False, max_scrolls: int = 100, pause: float = 1.0) -> None:
    """
    Scrolle progressivement jusqu'en bas de la page pour déclencher le lazy-load.
    On s'arrête quand la hauteur ne change plus ou après max_scrolls.
    """
    last_height = page.evaluate("() => document.body.scrollHeight")
    debug_print(debug, f"Hauteur initiale: {last_height}")

    for i in range(max_scrolls):
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(pause)

        new_height = page.evaluate("() => document.body.scrollHeight")
        debug_print(debug, f"Scroll {i+1}, nouvelle hauteur: {new_height}")
        if new_height == last_height:
            debug_print(debug, "Hauteur inchangée, fin du scroll.")
            break
        last_height = new_height


def scrape_evenements(page: Page, debug: bool = False) -> List[Dict]:
    """
    Scrape les cartes d'événements sur /evenements/immobilier.
    """
    cards = page.query_selector_all("div.MuiCard-root.evenement")
    results: List[Dict] = []
    debug_print(debug, f"[evenements] {len(cards)} cartes trouvées")

    for card in cards:
        # Lien / titre
        a_title = card.query_selector("div.evenement-title a")
        url = a_title.get_attribute("href") if a_title else None
        titre = a_title.inner_text().strip() if a_title else None

        # Date / type "En salle · DU ..."
        date_el = card.query_selector("div.evenement-date")
        date_texte = date_el.inner_text().strip() if date_el else None

        # Organisateur + ville/région
        owner_el = card.query_selector("div.evenement-owner .h4.grey")
        owner_txt = owner_el.inner_text().strip() if owner_el else ""
        organisateur = None
        ville_region = None
        if "·" in owner_txt:
            organisateur, ville_region = [p.strip() for p in owner_txt.split("·", 1)]
        else:
            organisateur = owner_txt

        results.append({
            "source": "evenements",
            "url": url,
            "titre": titre,
            "meta_date": date_texte,
            "organisateur": organisateur,
            "lieu": ville_region,
        })

    return results


def scrape_encheres(page: Page, debug: bool = False) -> List[Dict]:
    """
    Scrape les cartes de lots sur /encheres/immobilier.
    """
    # Cartes : div.MuiCard-root.card (lot en cours)
    cards = page.query_selector_all("div.MuiCard-root.card")
    results: List[Dict] = []
    debug_print(debug, f"[encheres] {len(cards)} cartes trouvées")

    for card in cards:
        # Titre / lien => dans .bottom .nom
        a_title = card.query_selector("div.bottom a")
        url = a_title.get_attribute("href") if a_title else None
        titre_el = card.query_selector("div.bottom .nom span")
        titre = titre_el.inner_text().strip() if titre_el else None

        # Caractéristiques (ville, surface, m2, etc.) dans .bottom .criteres
        crit_el = card.query_selector("div.bottom .criteres")
        criteres = crit_el.inner_text().strip() if crit_el else None

        # Prix dans .bottom .prix
        prix_el = card.query_selector("div.bottom .prix span.b600")
        prix_txt = prix_el.inner_text().strip() if prix_el else None

        results.append({
            "source": "encheres",
            "url": url,
            "titre": titre,
            "meta_criteres": criteres,
            "meta_prix": prix_txt,
        })

    return results


def scrape_resultats(page: Page, debug: bool = False) -> List[Dict]:
    """
    Scrape les cartes de résultats sur /resultats/immobilier.
    """
    # Cartes : div.MuiCard-root.card.resultat
    cards = page.query_selector_all("div.MuiCard-root.card.resultat")
    results: List[Dict] = []
    debug_print(debug, f"[resultats] {len(cards)} cartes trouvées")

    for card in cards:
        # Titre / lien
        a_title = card.query_selector("div.bottom a")
        url = a_title.get_attribute("href") if a_title else None
        titre_el = card.query_selector("div.bottom .nom span")
        titre = titre_el.inner_text().strip() if titre_el else None

        crit_el = card.query_selector("div.bottom .criteres")
        criteres = crit_el.inner_text().strip() if crit_el else None

        prix_el = card.query_selector("div.bottom .prix strike")
        prix_txt = prix_el.inner_text().strip() if prix_el else None

        results.append({
            "source": "resultats",
            "url": url,
            "titre": titre,
            "meta_criteres": criteres,
            "meta_prix": prix_txt,
        })

    return results


def deduplicate_by_url(records: List[Dict]) -> List[Dict]:
    """
    Dédoublonne les enregistrements par URL (en gardant la première occurrence).
    """
    seen = set()
    deduped: List[Dict] = []
    for rec in records:
        url = rec.get("url")
        if not url:
            deduped.append(rec)
            continue
        if url in seen:
            continue
        seen.add(url)
        deduped.append(rec)
    return deduped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import des annonces immobilières depuis encheres-publiques.com (événements, enchères, résultats)."
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Chemin d'un fichier CSV de sortie (toutes catégories confondues).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Lancer le navigateur en mode headless (sans interface).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Affiche des informations de debug sur stderr.",
    )
    args = parser.parse_args()

    base_urls = {
        "evenements": "https://www.encheres-publiques.com/evenements/immobilier",
        "encheres":  "https://www.encheres-publiques.com/encheres/immobilier",
        "resultats": "https://www.encheres-publiques.com/resultats/immobilier",
    }

    all_records: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        page = browser.new_page(viewport={"width": 1280, "height": 800})

        # 1) Événements
        debug_print(args.debug, f"Chargement {base_urls['evenements']}")
        page.goto(base_urls["evenements"], wait_until="networkidle")
        auto_scroll(page, debug=args.debug)
        all_records.extend(scrape_evenements(page, debug=args.debug))

        # 2) Enchères
        debug_print(args.debug, f"Chargement {base_urls['encheres']}")
        page.goto(base_urls["encheres"], wait_until="networkidle")
        auto_scroll(page, debug=args.debug)
        all_records.extend(scrape_encheres(page, debug=args.debug))

        # 3) Résultats
        debug_print(args.debug, f"Chargement {base_urls['resultats']}")
        page.goto(base_urls["resultats"], wait_until="networkidle")
        auto_scroll(page, debug=args.debug)
        all_records.extend(scrape_resultats(page, debug=args.debug))

        browser.close()

    debug_print(args.debug, f"Total bruts (toutes sources) : {len(all_records)}")
    deduped = deduplicate_by_url(all_records)
    debug_print(args.debug, f"Total après dédoublonnage par URL : {len(deduped)}")

    # Écriture CSV
    if deduped:
        fieldnames = sorted(deduped[0].keys())
    else:
        fieldnames = ["source", "url", "titre", "meta_date", "organisateur", "lieu",
                      "meta_criteres", "meta_prix"]

    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in deduped:
            writer.writerow(rec)

    print(f"{len(deduped)} enregistrements écrits dans {args.csv}", file=sys.stderr)


if __name__ == "__main__":
    main()