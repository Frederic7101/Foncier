#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
import_encheres_publiques_v2_playwright_scroll.py

Version 2 du script d'import des annonces immobilières depuis encheres-publiques.com,
basée sur Playwright et le scroll automatique pour charger toutes les annonces
des 3 catégories principales :
  - événements : https://www.encheres-publiques.com/evenements/immobilier
  - enchères   : https://www.encheres-publiques.com/encheres/immobilier
  - résultats  : https://www.encheres-publiques.com/resultats/immobilier

Objectifs :
  - Scroller jusqu'en bas de chaque page pour charger toutes les cartes.
  - Extraire pour chaque carte : URL, titre, texte brut, et quelques champs
    structurés quand ils sont détectables (date, localisation, prix).
  - Ajouter un champ "source" (evenements / encheres / resultats) pour tracer
    la catégorie d'origine.
  - Dédoublonner les annonces sur l'URL.
  - Exporter le tout dans un CSV unique.

Usage basique (dans l'environnement virtuel Python 3.11) :

    py -m playwright install
    py import_encheres_publiques_v2_playwright_scroll.py --csv encheres_publiques.csv

Options :
    --csv PATH           Chemin vers un fichier CSV de sortie (défaut: encheres_publiques.csv)
    --sources LISTE      Liste des catégories à scraper, séparées par des virgules,
                         parmi: evenements, encheres, resultats
                         (défaut: resultats)
    --headless           Exécuter le navigateur en mode headless (défaut: True)
    --debug              Affiche quelques infos de debug sur stderr
"""

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, Page


def debug_print(enabled: bool, *args, **kwargs) -> None:
    if enabled:
        print(*args, file=sys.stderr, **kwargs)


@dataclass
class EnchereRecord:
    url: str
    titre: str
    texte_brut: str
    date: Optional[str]
    localisation: Optional[str]
    prix: Optional[str]
    source: str  # "evenements" | "encheres" | "resultats"
    # Champs enrichis pour les résultats / enchères
    desc_courte: Optional[str] = None
    desc_longue: Optional[str] = None
    mise_depart: Optional[str] = None
    prix_m2_depart: Optional[str] = None
    montant_adjudication: Optional[str] = None
    date_adjudication: Optional[str] = None
    type_local: Optional[str] = None
    adresse: Optional[str] = None
    ville: Optional[str] = None
    code_postal: Optional[str] = None
    surface_habitable: Optional[str] = None
    nb_pieces: Optional[str] = None
    sous_categorie: Optional[str] = None  # "normale" | "surenchere" | None


def _scroll_to_bottom(page: Page, debug: bool = False, max_iterations: int = 80, wait_ms: int = 150) -> None:
    """
    Fait défiler la page jusqu'en bas en répétant un scroll et en attendant
    que le contenu se charge. On s'arrête quand la hauteur du document ne
    change plus pendant quelques itérations, ou après max_iterations.
    """
    same_height_counter = 0
    last_height = 0

    for i in range(max_iterations):
        # Scroll un peu avant le bas pour déclencher le lazy-loading rapidement
        page.evaluate(
            "window.scrollTo(0, document.body.scrollHeight - (window.innerHeight * 0.5));"
        )
        page.wait_for_timeout(wait_ms)

        # Puis scroll tout en bas
        page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        page.wait_for_timeout(wait_ms)

        new_height = page.evaluate("document.body.scrollHeight")
        debug_print(debug, f"Scroll itération {i+1}, hauteur={new_height}")

        if new_height == last_height:
            same_height_counter += 1
            # Si la hauteur ne change plus depuis plusieurs tours, on considère qu'on est en bas.
            if same_height_counter >= 4:
                debug_print(debug, "Hauteur stable, arrêt du scroll.")
                break
        else:
            same_height_counter = 0
            last_height = new_height


def _guess_fields_from_card_text(lines: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Heuristiques très simples pour essayer d'identifier :
      - une date,
      - une localisation (ville, département),
      - un prix ou mise à prix.

    On reste volontairement généraliste pour éviter de dépendre de classes CSS
    précises qui pourraient changer.
    """
    date: Optional[str] = None
    localisation: Optional[str] = None
    prix: Optional[str] = None

    for line in lines:
        l = line.strip()
        if not l:
            continue

        lower = l.lower()

        # Date : on cherche des mots-clés typiques
        if any(keyword in lower for keyword in ["vente le", "le ", "adjugé le", "clôture le", "fin le", "début le"]):
            if date is None:
                date = l

        # Localisation : mots comme "75000", "Paris", "Ville", etc. (heuristique très large)
        if any(keyword in lower for keyword in [" paris", " lyon", " marseille", " toulouse", " bordeaux", "nice", "nantes", "montpellier", "lille", "rennes"]):
            if localisation is None:
                localisation = l

        # Prix : présence de "€", "euro", "mise à prix", "prix de départ"
        if any(keyword in lower for keyword in ["€", "euro", "mise à prix", "prix de départ"]):
            if prix is None:
                prix = l

    return date, localisation, prix


def _parse_resultats_listing_lines(
    titre: str, lines: List[str]
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    """
    Heuristiques pour extraire quelques champs structurés à partir
    du texte de la carte dans la liste des 'resultats'.

    On vise :
      - desc_courte: le titre
      - desc_longue: tout le texte concaténé
      - type_local: dérivé du titre (mot 'appartement', 'maison', etc. si présent)
      - ville, surface_habitable, nb_pieces: à partir de la ligne "Ville · XX m² · YY pièces"
      - mise_depart: prix de départ, si présent dans les lignes
      - montant_adjudication: pris tel quel si une ligne contient 'Adjugé' ou 'Prix d adjudication'
      - sous_categorie: 'surenchere' si le texte mentionne 'surenchère', sinon 'normale'
    """
    desc_courte = titre.strip() if titre else None
    desc_longue = "\n".join(lines).strip() if lines else titre

    type_local = None
    if titre:
        lower_title = titre.lower()
        for kw in ["appartement", "maison", "immeuble", "local", "terrain", "parking"]:
            if kw in lower_title:
                type_local = kw
                break

    ville = None
    surface = None
    nb_pieces = None

    for line in lines:
        l = line.strip()
        if " m²" in l or "m²" in l:
            parts = [p.strip() for p in l.split("·")]
            if parts:
                ville = parts[0]
            for p in parts[1:]:
                if "m²" in p:
                    surface = p
                if "pièce" in p:
                    nb_pieces = p
            break

    mise_depart = None
    montant_adjudication = None

    for line in lines:
        lower = line.lower()
        raw = line.strip()
        if "mise à prix" in lower or "prix de départ" in lower:
            mise_depart = _extract_montant_eur_only(raw) or raw
        if "adjugé" in lower or "prix gagnant" in lower:
            montant_adjudication = _extract_montant_eur_only(raw) or raw

    sous_categorie = None
    all_text_lower = (titre + " " + " ".join(lines)).lower()
    if "surenchère" in all_text_lower or "surencherir" in all_text_lower:
        sous_categorie = "surenchère"
    elif "encher" in all_text_lower:
        sous_categorie = "terminée"

    # date_adjudication sera plutôt extraite depuis la page de détail
    date_adjudication = None

    return (
        desc_courte,
        desc_longue,
        mise_depart,
        montant_adjudication,
        date_adjudication,
        type_local,
        ville,
        surface,
        nb_pieces,
    )


def _extract_montant_eur_only(text: str) -> Optional[str]:
    """
    Extrait du texte le montant en € (hors €/m²).
    Retourne le dernier montant de type "XXX €" qui n'est pas un prix au m².
    """
    import re
    if not text or "€" not in text:
        return None
    candidates = []
    for m in re.finditer(r"([\d\s.,]+€)", text):
        candidate = m.group(1).strip()
        if "m²" in candidate or "m2" in candidate:
            continue
        end = m.end()
        rest = text[end : end + 8].strip().lower()
        if rest.startswith("/") or rest.startswith("m²") or rest.startswith("m2"):
            continue
        candidates.append(candidate)
    return candidates[-1] if candidates else None


def _enrich_record_from_next_data(
    context,
    record: EnchereRecord,
    fail_if_not_logged_in: bool,
    debug: bool = False,
) -> None:
    """
    Ouvre la page de détail de l'annonce, lit le JSON __NEXT_DATA__
    puis scrappe le DOM pour remplir les champs structurés.
    """
    with context.new_page() as detail_page:
        full_url = (
            record.url
            if record.url.startswith("http")
            else f"https://www.encheres-publiques.com{record.url}"
        )
        debug_print(debug, f"Ouverture page détail: {full_url}")
        detail_page.goto(full_url, wait_until="domcontentloaded", timeout=60_000)
        # Attendre que le contenu principal soit visible (réduit les écarts entre pages)
        try:
            detail_page.wait_for_selector("h1", timeout=5000)
        except Exception:
            pass
        detail_page.wait_for_timeout(800)

        # --- NEXT_DATA -------------------------------------------------------
        try:
            script_el = detail_page.locator("script#__NEXT_DATA__")
            if not script_el.count():
                debug_print(debug, f"__NEXT_DATA__ introuvable pour {record.url}")
                return
            script_text = script_el.nth(0).inner_text()
            data = json.loads(script_text)
        except Exception as e:
            debug_print(debug, f"Erreur lecture __NEXT_DATA__ pour {record.url}: {e}")
            return

        query = data.get("query", {})

        # type_local / sous_categorie (maisons, appartements, etc.)
        type_local = query.get("sous_categorie")
        if type_local:
            record.type_local = record.type_local or type_local

        # ville brute (ex: 'bordeaux-33') -> ville (mais PAS code postal, qui vient de l'adresse)
        ville_raw = query.get("ville")
        if ville_raw and not record.ville:
            parts = ville_raw.split("-")
            ville_name = "-".join(parts[:-1]) if len(parts) > 1 else ville_raw
            record.ville = (ville_name.replace("-", " ")).title()

        # nom court (slug) -> desc_courte si non déjà renseignée
        nom_slug = query.get("nom")
        if nom_slug and not record.desc_courte:
            record.desc_courte = nom_slug.replace("-", " ")

        # Texte complet de la page (pour plusieurs heuristiques)
        try:
            body_text = detail_page.inner_text("body").lower()
        except Exception:
            body_text = ""

        # --- Extraction DOM des informations visibles ------------------------

        # 1) Description courte : h1 principal, puis fallbacks ; toujours limiter à une ligne / 250 car
        def _truncate_desc_courte(s: str) -> str:
            s = s.strip()
            first_line = s.split("\n")[0].strip()
            return first_line[:250] if len(first_line) > 250 else first_line

        try:
            h1 = detail_page.locator("h1").first
            if h1.count():
                h1_text = h1.inner_text().strip()
                if h1_text:
                    record.desc_courte = _truncate_desc_courte(h1_text)
            if not record.desc_courte:
                for sel in ["[class*='title']", "[class*='Title']", "h2"]:
                    el = detail_page.locator(sel).first
                    if el.count():
                        t = el.inner_text().strip()
                        if t and len(t) > 3:
                            record.desc_courte = _truncate_desc_courte(t)
                            break
        except Exception:
            pass

        # 1bis) Description longue : bloc descriptif uniquement ; couper avant "Se termine dans"
        def _truncate_desc_longue(txt: str) -> str:
            for stop in ("Se termine dans"):
                i = txt.find(stop)
                if i > 100:
                    return txt[:i].strip()
            return txt.strip()

        try:
            for sel in ["div.text", "[class*='description']", "[class*='Description']", "main div[class*='text']"]:
                text_block = detail_page.locator(sel).first
                if text_block.count():
                    txt = text_block.inner_text().strip()
                    if 50 < len(txt) < 15000:
                        record.desc_longue = _truncate_desc_longue(txt)
                        break
            if not record.desc_longue:
                art = detail_page.locator("article").first
                if art.count():
                    txt = art.inner_text().strip()
                    if txt and len(txt) > 50:
                        record.desc_longue = _truncate_desc_longue(txt)[:8000]
        except Exception:
            pass

        # 2) Prix de départ / Prix m² dans le bloc du haut
        import re
        try:
            header_block = detail_page.locator("text=Prix de départ").locator("xpath=..")
            if header_block.count():
                header_text = header_block.inner_text().replace("\xa0", " ").strip()
                lines = [l.strip() for l in header_text.splitlines() if "€" in l]
                for line in lines:
                    parts = [p.strip() for p in line.split("·")]
                    for p in parts:
                        if "€" in p and ("m²" in p or "m2" in p):
                            record.prix_m2_depart = record.prix_m2_depart or p
                        elif "€" in p:
                            record.mise_depart = record.mise_depart or p
            # Fallback : chercher "Prix de départ" puis un montant en € dans le texte de la page
            if not record.mise_depart:
                try:
                    bloc = detail_page.get_by_text("Prix de départ", exact=False).first.locator("xpath=..")
                    if bloc.count():
                        t = bloc.inner_text().replace("\xa0", " ").strip()
                        for m in re.findall(r"[\d\s.,]+€", t):
                            if "m²" not in m and "m2" not in m:
                                record.mise_depart = m.strip()
                                break
                except Exception:
                    pass
        except Exception:
            pass

        # 2bis) Prix gagnant : repérer par le texte "Prix gagnant", puis chercher le montant
        # dans la même ligne (span.right, span.bold) ou les suivantes (regex XXX €).
        try:
            import re

            montant = None
            # Méthode A : structure connue div.details > div.enchere-details > div.line
            details_root = detail_page.locator("div.details div.enchere-details").first
            if details_root.count():
                lines_divs = details_root.locator("div.line").all()
                prix_idx = None
                for idx, div in enumerate(lines_divs):
                    left_span = div.locator("span.left").first
                    if left_span.count() and "prix gagnant" in left_span.inner_text().strip().lower():
                        prix_idx = idx
                        break
                if prix_idx is not None:
                    div = lines_divs[prix_idx]
                    right_span = div.locator("span.right.bold").first
                    if right_span.count():
                        txt = right_span.inner_text().replace("\xa0", " ").strip()
                        if "€" in txt:
                            montant = txt
                    if montant is None:
                        for j in range(prix_idx + 1, len(lines_divs)):
                            full_text = lines_divs[j].inner_text().replace("\xa0", " ").strip()
                            matches = re.findall(r"([\d\s.,]+€)", full_text)
                            if matches:
                                montant = matches[-1].strip()
                                break

            # Méthode B : fallback par texte "Prix gagnant" (indépendant de la structure)
            if montant is None:
                label_el = detail_page.get_by_text("Prix gagnant", exact=False).first
                if label_el.count():
                    # Remonter au conteneur "ligne" (parent direct ou parent du parent)
                    row = label_el.locator("xpath=..")
                    if row.count():
                        # Montant dans la même ligne : span.right, span.bold, ou texte avec €
                        for sel in ["span.right.bold", "span.right", "span.bold", "[class*='right']"]:
                            right = row.locator(sel).first
                            if right.count():
                                txt = right.inner_text().replace("\xa0", " ").strip()
                                m = re.search(r"([\d\s.,]+€)", txt)
                                if m:
                                    montant = m.group(1).strip()
                                    break
                        if montant is None:
                            full = row.inner_text().replace("\xa0", " ").strip()
                            m = re.search(r"([\d\s.,]+€)", full)
                            if m:
                                montant = m.group(1).strip()
                        # Sinon chercher dans les frères suivants
                        if montant is None:
                            for sibling in row.locator("xpath=following-sibling::*").all():
                                if sibling.count():
                                    full = sibling.inner_text().replace("\xa0", " ").strip()
                                    m = re.search(r"([\d\s.,]+€)", full)
                                    if m:
                                        montant = m.group(1).strip()
                                        break

            # Ne garder que le montant en € (exclure prix au m² type "XXX €/m²")
            montant = _extract_montant_eur_only(montant) if montant else None

            if montant:
                if "inconnu" in montant.lower():
                    record.montant_adjudication = ""
                    debug_print(
                        True,
                        f"ATTENTION: prix gagnant inconnu (mention 'Inconnu') pour {record.url}",
                    )
                    if fail_if_not_logged_in and "se connecter" in body_text:
                        raise RuntimeError(
                            "Non connecté: prix d'adjudication non accessible."
                        )
                else:
                    record.montant_adjudication = montant
            else:
                debug_print(debug, "[detail] Aucun montant en € trouvé pour Prix gagnant; montant_adjudication laissé vide.")

        except Exception:
            pass

        # 3) Date d'adjudication : parcourir les <li> de la liste horaire "À propos"
        # et prendre la première valeur qui ressemble à une date française "12 mars 2026".
        if record.date_adjudication is None:
            try:
                # Liste "À propos" (ul) puis tous les li, on cherche une date.
                liste = detail_page.locator("ul.MuiList-root.MuiList-padding.css-depspc").first
                if liste.count():
                    items = liste.locator("li.MuiListItem-root").all()
                    import re

                    for item in items:
                        span = item.locator("span.MuiListItemText-primary").first
                        if not span.count():
                            continue
                        txt = span.inner_text().strip().lower()
                        m = re.search(r"(\d{1,2})\s+([a-zéû]+)\s+(\d{4})", txt)
                        if m:
                            jour, mois_str, annee = m.groups()
                            mois_map = {
                                "janvier": "01",
                                "février": "02",
                                "fevrier": "02",
                                "mars": "03",
                                "avril": "04",
                                "mai": "05",
                                "juin": "06",
                                "juillet": "07",
                                "août": "08",
                                "aout": "08",
                                "septembre": "09",
                                "octobre": "10",
                                "novembre": "11",
                                "décembre": "12",
                                "decembre": "12",
                            }
                            mois_num = mois_map.get(mois_str, "01")
                            record.date_adjudication = f"{int(jour):02d}/{mois_num}/{annee}"
                            break
            except Exception:
                pass

        # 4) Section "Détails" : type_local, adresse, surface, nb pièces
        try:
            details_section = detail_page.locator("text=Détails").locator("xpath=..")
            if details_section.count():
                lines = details_section.inner_text().splitlines()
                clean_lines = [l.strip() for l in lines if l.strip()]

                for l in clean_lines:
                    lower = l.lower()

                    if any(
                        kw in lower
                        for kw in [
                            "maisons",
                            "appartements",
                            "immeubles",
                            "terrains",
                            "parkings",
                            "locaux",
                        ]
                    ):
                        record.type_local = record.type_local or l

                    if any(c.isdigit() for c in l) and "," in l:
                        record.adresse = l
                        try:
                            parts = [p.strip() for p in l.split(",")]
                            if len(parts) >= 2:
                                voie = parts[0]
                                cp_ville = parts[1]
                                tokens = cp_ville.split()
                                if tokens:
                                    cp = tokens[0]
                                    ville_nom = " ".join(tokens[1:]) if len(tokens) > 1 else ""
                                    record.code_postal = record.code_postal or cp
                                    record.ville = record.ville or ville_nom
                                    record.adresse = voie
                        except Exception:
                            pass

                    if "m²" in lower or "m2" in lower:
                        val = l
                        for prefix in [
                            "surface habitable :",
                            "surface habitable:",
                            "surface :",
                            "surface:",
                        ]:
                            if prefix in val.lower():
                                val = val.lower().replace(prefix, "").strip()
                        record.surface_habitable = record.surface_habitable or val

                    if "pièce" in lower:
                        val = l
                        for prefix in [
                            "nombre de pièces :",
                            "nombre de pieces :",
                            "nb de pièces :",
                            "nb de pieces :",
                        ]:
                            if prefix in val.lower():
                                val = val.lower().replace(prefix, "").strip()
                        record.nb_pieces = record.nb_pieces or val
        except Exception:
            pass

        # 5) Surface habitable de secours : première occurrence "xx m2/m²" dans la description longue
        if not record.surface_habitable and record.desc_longue:
            import re

            txt = record.desc_longue.replace("\xa0", " ")
            m = re.search(r"(\d+(?:[.,]\d+)?)\s*m[²2]", txt.lower())
            if m:
                record.surface_habitable = m.group(1).replace(",", ".") + " m2"


def _extract_cards_from_page(page: Page, source: str, debug: bool = False) -> List[EnchereRecord]:
    """
    Extrait les cartes d'annonces visibles sur la page courante.

    La structure exacte du HTML pouvant évoluer, on utilise des heuristiques :
      - on repère les liens <a> qui contiennent du texte significatif
        et dont le href semble pointer vers une annonce.
      - on récupère le texte intégral de la carte pour le stocker et
        en extraire quelques champs.
    """
    records: List[EnchereRecord] = []

    # On récupère en priorité les liens dont le href semble pointer vers une annonce
    # afin de limiter le nombre de noeuds analysés (meilleures perfs).
    anchors = page.locator(
        "a[href*='/ventes/'], a[href*='/evenements/'], a[href*='/resultats/']"
    ).all()
    debug_print(debug, f"Nombre total de <a> trouvés: {len(anchors)} (source={source})")

    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
        except Exception:
            continue

        if not href:
            continue

        # Texte complet de la carte / lien
        try:
            full_text = a.inner_text().strip()
        except Exception:
            continue

        if not full_text:
            continue

        # On split en lignes pour les heuristiques
        lines = [line.strip() for line in full_text.splitlines() if line.strip()]
        if not lines:
            continue

        titre = lines[0]
        date, localisation, prix = _guess_fields_from_card_text(lines[1:])

        record = EnchereRecord(
            url=href,
            titre=titre,
            texte_brut=full_text,
            date=date,
            localisation=localisation,
            prix=prix,
            source=source,
        )
        records.append(record)

    debug_print(debug, f"{len(records)} annonces brutes extraites pour la source '{source}'.")
    return records

def to_yyyy_mm_dd(d: str) -> str:
    # d = "DD/MM/YYYY"
    try:
        j, m, a = d.split("/")
        return f"{a}/{m}/{j}"
    except ValueError:
        return d  # au cas où le format serait différent

def _collect_for_url_streaming(
    browser,
    url: str,
    source: str,
    writer: csv.DictWriter,
    file_handle,
    seen_urls: set,
    written_count_ref: List[int],
    csv_path: str,
    max_results: Optional[int],
    date_bounds: List[Optional[str]],
    login_email: Optional[str],
    login_password_env: Optional[str],
    fail_if_not_logged_in: bool,
    debug: bool = False,
) -> None:
    """
    Ouvre l'URL dans un contexte Playwright, puis :
      - scrolle progressivement,
      - à chaque itération de scroll, lit les liens visibles
        et écrit immédiatement les nouvelles annonces uniques dans le CSV.

    On ne garde donc pas tout en mémoire : le flux est
    "scroll -> détecter nouvelles annonces -> écrire -> scroll -> ..."
    """
    context = browser.new_context()
    page = context.new_page()
    # Connexion sur la même page (sans naviguer), puis ouverture de l'URL cible ici pour être sûr d'afficher la bonne page
    _login_if_needed(
        context,
        email=login_email,
        password_env=login_password_env,
        debug=debug,
        page=page,
    )

    try:
        debug_print(debug, f"Ouverture de la page ({source}) : {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        # En mode connecté, le contenu des cartes peut se charger en différé : attendre un peu plus
        use_login = bool(login_email and (login_password_env and os.environ.get(login_password_env)))
        if use_login:
            page.wait_for_timeout(2500)
        debug_print(debug, f"URL effective au début du scroll : {page.url}")
        same_height_counter = 0
        last_height = 0
        seen_page_links: set[str] = set()

        max_iterations = 80
        wait_ms = 500 if use_login else 150  # plus long si connecté pour laisser le rendu des cartes

        for i in range(max_iterations):
            # Scroll un peu avant le bas pour déclencher le lazy-loading rapidement
            page.evaluate(
                "window.scrollTo(0, document.body.scrollHeight - (window.innerHeight * 0.5));"
            )
            page.wait_for_timeout(wait_ms)

            # Puis scroll tout en bas
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            page.wait_for_timeout(wait_ms)

            new_height = page.evaluate("document.body.scrollHeight")
            debug_print(debug, f"[{source}] Scroll itération {i+1}, hauteur={new_height}")

            # En mode connecté, laisser le temps au contenu des cartes de s'afficher avant de lire
            if use_login:
                page.wait_for_timeout(600)

            # Extraction / écriture streaming des annonces visibles à cette itération
            # On adapte le sélecteur en fonction de la catégorie pour éviter
            # de récupérer les liens de navigation (ventes, filtres, etc.).
            if source in ("resultats", "encheres"):
                # Annonces : href relatif /encheres/immobilier/... ou absolu .../encheres/immobilier/...
                selector = "a[href*='/encheres/immobilier/']"
            elif source == "evenements":
                selector = "a[href*='/evenements/']"
            else:
                selector = "a"

            anchors = page.locator(selector).all()
            debug_print(debug, f"[{source}] itération {i+1}: {len(anchors)} liens candidats (sélecteur: {selector}).")

            for a in anchors:
                try:
                    href = a.get_attribute("href") or ""
                except Exception:
                    continue

                if not href or href in seen_page_links:
                    continue
                # Exclure les liens de catégorie/filtre (ex. /encheres/immobilier/appartements/ile-de-france)
                # Les vraies annonces ont un slug avec underscore (ex. appartement-paris_54) et au moins 2 segments
                if source in ("resultats", "encheres"):
                    path = href.split("?")[0].rstrip("/")
                    if "/encheres/immobilier/" in path:
                        rest = path.split("/encheres/immobilier/")[-1]
                        if "_" not in rest or rest.count("/") < 1:
                            continue
                seen_page_links.add(href)

                try:
                    full_text = a.inner_text().strip()
                except Exception:
                    continue

                if not full_text:
                    continue

                lines = [line.strip() for line in full_text.splitlines() if line.strip()]
                if not lines:
                    continue

                titre = lines[0]
                date, localisation, prix = _guess_fields_from_card_text(lines[1:])

                # Enrichissement spécifique pour la catégorie "resultats"
                (
                    desc_courte,
                    desc_longue,
                    mise_depart,
                    montant_adjudication,
                    date_adjudication,
                    type_local,
                    ville,
                    surface_habitable,
                    nb_pieces,
                ) = (None, None, None, None, None, None, None, None, None)

                if source == "resultats":
                    (
                        desc_courte,
                        desc_longue,
                        mise_depart,
                        montant_adjudication,
                        date_adjudication,
                        type_local,
                        ville,
                        surface_habitable,
                        nb_pieces,
                    ) = _parse_resultats_listing_lines(titre, lines[1:])

                # En mode connecté la liste peut n'afficher que des placeholders (EN SALLE, 2)
                # → on ne garde pas ça pour desc_courte/desc_longue, l'enrichissement détail les remplira
                if titre and titre.strip().upper() in ("EN SALLE", "CHARGEMENT", "CHARGEMENT..."):
                    desc_courte = None
                    desc_longue = None
                elif desc_longue and len(desc_longue.strip()) < 15 and desc_longue.strip().replace(" ", "").isdigit():
                    desc_longue = None

                record = EnchereRecord(
                    url=href,
                    titre=titre,
                    texte_brut=full_text,
                    date=date,
                    localisation=localisation or ville,
                    prix=prix,
                    source=source,
                    desc_courte=desc_courte,
                    desc_longue=desc_longue,
                    mise_depart=mise_depart,
                    montant_adjudication=montant_adjudication,
                    date_adjudication=date_adjudication,
                    type_local=type_local,
                    adresse=None,
                    ville=ville,
                    code_postal=None,
                    surface_habitable=surface_habitable,
                    nb_pieces=nb_pieces,
                    sous_categorie=None,
                )

                # Enrichissement via la page de détail pour resultats / encheres
                if source in ("resultats", "encheres"):
                    try:
                        _enrich_record_from_next_data(
                            context=context,
                            record=record,
                            fail_if_not_logged_in=fail_if_not_logged_in,
                            debug=debug,
                        )
                    except RuntimeError as e:
                        # Cas fail_if_not_logged_in déclenché
                        debug_print(True, str(e))
                        raise

                # Dédoublonnage global sur l'URL
                if not record.url or record.url in seen_urls:
                    continue
                seen_urls.add(record.url)

                # Écriture immédiate : on ne garde que les champs déclarés
                rec_dict = asdict(record)
                filtered = {k: rec_dict.get(k) for k in writer.fieldnames}
                writer.writerow(filtered)
                written_count_ref[0] += 1

                # Mise à jour des bornes de dates (DD/MM/YYYY) si disponibles
                d = record.date_adjudication
                #print(f"date_adjudication: {d}")
                if d:
                    normalized_d = to_yyyy_mm_dd(d)
                    current_min, current_max = date_bounds
                    if current_min is None or normalized_d < current_min:
                        date_bounds[0] = normalized_d
                    if current_max is None or normalized_d > current_max:
                        date_bounds[1] = normalized_d
                #print(f"date_bounds: {date_bounds}") 
                # Flush après cette écriture (les blocs restent petits avec 1 scroll)
                file_handle.flush()

                if debug:
                    debug_print(
                        True,
                        f"{written_count_ref[0]} enregistrements uniques écrits dans {csv_path}",
                    )

                # Arrêt anticipé si on a atteint le quota demandé
                if max_results is not None and written_count_ref[0] >= max_results:
                    debug_print(
                        debug,
                        f"Nombre maximal d'annonces atteint ({max_results}), arrêt.",
                    )
                    return

            if new_height == last_height:
                same_height_counter += 1
                if same_height_counter >= 4:
                    debug_print(debug, f"[{source}] Hauteur stable, arrêt du scroll.")
                    break
            else:
                same_height_counter = 0
                last_height = new_height

        debug_print(
            debug,
            f"Extraction streaming terminée pour la source '{source}'.",
        )
    finally:
        debug_print(debug, f"Fermeture du contexte pour la source '{source}'.")
        context.close()


def _deduplicate_records(records: List[EnchereRecord], debug: bool = False) -> List[EnchereRecord]:
    """
    Dédoublonne les annonces par URL absolue.
    Si la même URL apparaît dans plusieurs catégories (sources),
    on garde la première occurrence rencontrée.
    """
    seen: Dict[str, EnchereRecord] = {}
    for rec in records:
        url = rec.url
        if not url:
            continue
        if url in seen:
            continue
        seen[url] = rec

    result = list(seen.values())
    debug_print(debug, f"Dédoublonnage : {len(records)} -> {len(result)} enregistrements uniques.")
    return result


def write_csv(records: List[EnchereRecord], path: str) -> None:
    """
    Écrit les enregistrements dans un fichier CSV.
    """
    dicts: List[Dict[str, str]] = [asdict(r) for r in records]

    if not dicts:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("")
        return

    fieldnames = list(dicts[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(dicts)

def _login_if_needed(
    context,
    email: Optional[str],
    password_env: Optional[str],
    debug: bool = False,
    *,
    page: Optional[Page] = None,
) -> None:
    """Connexion au site. Si `page` est fourni, elle est utilisée et laissée ouverte.
    L'appelant doit ensuite faire page.goto(url_cible) pour afficher la bonne page."""
    if not email or not password_env:
        return
    password = os.environ.get(password_env)
    if not password:
        debug_print(debug, f"Variable d'environnement {password_env} absente, connexion ignorée.")
        return

    own_page = None
    if page is None:
        own_page = context.new_page()
        page = own_page
    try:
        debug_print(debug, "Connexion à encheres-publiques.com...")
        # 1) Ouvrir la page d'accueil
        page.goto("https://www.encheres-publiques.com/", wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(2000)

        # 2) Cliquer sur l'icône "Se connecter" (footer / header) pour ouvrir la popup
        page.locator("div[aria-label='Se connecter']").first.click()
        page.wait_for_timeout(1500)

        # 3) Dans la popup : cliquer sur "Se connecter par identifiant" (bouton rouge)
        page.wait_for_selector("button.login-button.red", timeout=15_000)
        page.locator("button.login-button.red").first.click()
        page.wait_for_timeout(2000)

        # 4) Attendre les champs email / mot de passe sur la page de connexion
        page.wait_for_selector("input[type='email'], input[name='email'], input[type='text']", timeout=15_000)
        # Remplir email (plusieurs sélecteurs possibles selon le formulaire)
        email_sel = page.locator("input[type='email']").first
        if not email_sel.count():
            email_sel = page.locator("input[name='email']").first
        if not email_sel.count():
            email_sel = page.locator("input[type='text']").first
        if email_sel.count():
            email_sel.fill(email)
        page.fill("input[type='password']", password)

        # 5) Soumettre le formulaire et attendre la navigation post-login (si le site en déclenche une)
        submit_btn = page.locator("button[type='submit']").first
        try:
            with page.expect_navigation(timeout=5_000, wait_until="commit"):
                submit_btn.click()
        except Exception:
            # Timeout : pas de navigation classique (ex. SPA), le clic a déjà été fait
            pass
        page.wait_for_timeout(5000)

        debug_print(debug, "Connexion terminée.")
    finally:
        if own_page is not None:
            own_page.close()

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Import de toutes les annonces immobilières depuis encheres-publiques.com "
            "pour les catégories événements / enchères / résultats via Playwright + scroll."
        )
    )

    parser.add_argument(
        "--login-email",
        help="Email de connexion encheres-publiques.com (optionnel, pour voir les prix gagnants).",
    )
    parser.add_argument(
        "--login-password-env",
        help="Nom de la variable d'environnement contenant le mot de passe (ex: ENCHERES_PASSWORD).",
    )
    parser.add_argument(
        "--login-password",
        help="Mot de passe de connexion encheres-publiques.com (optionnel, pour voir les prix gagnants).",
    )
    parser.add_argument(
        "--csv",
        default="encheres_publiques.csv",
        help="Chemin d'un fichier CSV de sortie (défaut: encheres_publiques.csv).",
    )
    parser.add_argument(
        "--afficher",
        action="store_true",
        help=(
            "Afficher le contenu du CSV sur la console (avec 2 lignes blanches avant) "
            "au lieu de renommer le fichier avec le suffixe région/type."
        ),
    )
    parser.add_argument(
        "--sources",
        default="resultats",
        help=(
            "Liste des catégories à scraper, séparées par des virgules, "
            "parmi: evenements, encheres, resultats (défaut: resultats)."
        ),
    )
    parser.add_argument(
        "--region",
        help="Slug de région (ex: ile-de-france). Si omis: toutes régions.",
    )
    parser.add_argument(
        "--type-bien",
        help=(
            "Type de bien: appartements, maisons, parkings, immeubles, "
            "locaux-commerciaux, terrains. Si omis: tous types."
        ),
    )
    parser.add_argument(
        "--order",
        choices=["asc", "desc"],
        default="asc",
        help="Ordre chronologique (asc=plus anciennes d'abord, desc=plus récentes).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Exécuter le navigateur en mode headless (par défaut: True).",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Forcer le mode non-headless (fenêtre visible).",
    )
    parser.set_defaults(headless=True)

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Affiche des informations de debug sur stderr.",
    )
    parser.add_argument(
        "--fail-if-not-logged-in",
        action="store_true",
        help=(
            "Arrête le script dès qu'une annonce de résultat nécessite d'être connecté "
            "pour voir le prix d'adjudication."
        ),
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="Nombre maximal d'annonces à scraper pour cette exécution.",
    )

    args = parser.parse_args()

    # Normalisation de la liste de sources demandées
    requested_sources = {
        s.strip() for s in args.sources.split(",") if s.strip()
    }
    allowed_sources = {"evenements", "encheres", "resultats"}
    invalid = requested_sources - allowed_sources
    if invalid:
        raise SystemExit(
            f"Sources invalides: {', '.join(sorted(invalid))}. "
            f"Sources autorisées: evenements, encheres, resultats."
        )

    base_urls = {
        "evenements": "https://www.encheres-publiques.com/evenements/immobilier",
        "encheres": "https://www.encheres-publiques.com/encheres/immobilier",
        "resultats": "https://www.encheres-publiques.com/resultats/immobilier",
    }

    # Application des filtres région / type / ordre uniquement pour les résultats
    if "resultats" in requested_sources:
        base_results = base_urls["resultats"]
        params = []

        # Région (slug de ref_regions), ex: place=ile-de-france
        if args.region:
            params.append(f"place={args.region}")

        # Type de bien (slug), ex: sous_categorie=appartements
        if args.type_bien:
            params.append(f"sous_categorie={args.type_bien}")

        # Ordre chronologique: reverse=1 pour asc, 0 pour desc
        reverse_val = "1" if args.order == "asc" else "0"
        params.append(f"reverse={reverse_val}")

        if params:
            base_results += "?" + "&".join(params)

        base_urls["resultats"] = base_results

    # On garde un ordre fixe pour la reproductibilité
    urls = [
        (base_urls[source], source)
        for source in ("evenements", "encheres", "resultats")
        if source in requested_sources
    ]

    # On écrit au fil de l'eau dans le CSV avec dédoublonnage immédiat sur l'URL.
    # Colonnes de sortie : uniquement les champs utiles demandés
    fieldnames = [
        "url",
        "desc_courte",
        "desc_longue",
        "mise_depart",
        "prix_m2_depart",
        "montant_adjudication",
        "date_adjudication",
        "type_local",
        "adresse",
        "ville",
        "code_postal",
        "surface_habitable",
        "nb_pieces",
        "sous_categorie",
    ]
    seen_urls = set()

    written_count_ref: List[int] = [0]  # compteur modifiable par référence
    date_bounds: List[Optional[str]] = [None, None]  # [date_min, date_max]

    region_slug = args.region or "all"
    type_slug = args.type_bien or "all"
    base, ext = os.path.splitext(args.csv)
    ext = ext or ".csv"
    # Fichier temporaire distinct pour éviter WinError 32 au renommage (OneDrive, IDE, etc.)
    csv_temp = f"{base}_tmp{ext}"

    # Avant de lancer _collect_for_url_streaming, on construit le set seen_urls à partir
    # des URLs déjà présentes dans un CSV précédent, pour éviter de retraiter les annonces
    # déjà scrapées. On gère 2 cas :
    #  - exécution normale terminée : fichier final args.csv existe
    #  - interruption brutale : seul le fichier temporaire csv_temp existe
    existing_csv_path = None
    if os.path.exists(args.csv):
        existing_csv_path = args.csv
    elif os.path.exists(csv_temp):
        existing_csv_path = csv_temp

    if existing_csv_path:
        # On détecte automatiquement le séparateur (',' ou ';') pour relire
        # proprement un ancien CSV, puis on récupère les URLs existantes.
        with open(existing_csv_path, newline="", encoding="utf-8") as f_in:
            first_line = f_in.readline()
            # Sniffer pour trouver le délimiteur parmi ',' et ';'
            try:
                dialect = csv.Sniffer().sniff(first_line, delimiters=";,")
            except csv.Error:
                # Fallback: on suppose le ';'
                dialect = csv.excel
                dialect.delimiter = ";"
            f_in.seek(0)
            reader = csv.DictReader(f_in, dialect=dialect)
            for row in reader:
                url = row.get("url")
                if url:
                    seen_urls.add(url)
        debug_print(
            True,
            f"{len(seen_urls)} URLs déjà présentes récupérées depuis {existing_csv_path}; "
            "elles ne seront pas retraitées."
        )
    else:
        debug_print(
            True,
            f"Aucun fichier CSV précédent trouvé ({args.csv} ni {csv_temp}); on repart de zéro."
        )

    # Pré-remplir le fichier temporaire avec les enregistrements déjà présents dans le CSV
    # précédent (final ou temporaire) afin que le nouveau run ajoute simplement les nouvelles
    # annonces à la suite.
    with open(csv_temp, "w", newline="", encoding="utf-8") as f:
        # On fixe le séparateur de sortie à ';' et on ignore les éventuelles
        # colonnes supplémentaires des anciens fichiers.
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter=";",
            extrasaction="ignore",
        )
        writer.writeheader()

        if existing_csv_path:
            # Réécriture normalisée du CSV précédent dans le nouveau temporaire,
            # en utilisant le même dialecte détecté plus haut.
            with open(existing_csv_path, newline="", encoding="utf-8") as f_in:
                first_line = f_in.readline()
                try:
                    dialect = csv.Sniffer().sniff(first_line, delimiters=";,")
                except csv.Error:
                    dialect = csv.excel
                    dialect.delimiter = ";"
                f_in.seek(0)
                reader = csv.DictReader(f_in, dialect=dialect)
                for row in reader:
                    writer.writerow(row)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=args.headless)
            try:
                for url, source in urls:
                    try:
                        _collect_for_url_streaming(
                            browser=browser,
                            url=url,
                            source=source,
                            writer=writer,
                            file_handle=f,
                            seen_urls=seen_urls,
                            written_count_ref=written_count_ref,
                            csv_path=csv_temp,
                            max_results=args.max_results,
                            date_bounds=date_bounds,
                            login_email=args.login_email,
                            login_password_env=args.login_password_env,
                            fail_if_not_logged_in=args.fail_if_not_logged_in,
                            debug=args.debug,
                        )
                    except Exception as e:
                        debug_print(args.debug, f"Erreur lors de la collecte pour {source} ({url}) : {e}")
                        continue
            finally:
                browser.close()

    # Nom final avec suffixe région / type / dates (dates sans '/')
    date_min, date_max = date_bounds

    def sanitize_date(d: str) -> str:
        return (d or "").replace("/", "_")

    if date_min and date_max:
        final_name = f"{base}.{region_slug}.{type_slug}.{sanitize_date(date_min)}.{sanitize_date(date_max)}{ext}"
    else:
        final_name = f"{base}.{region_slug}.{type_slug}{ext}"

    if args.afficher:
        # Afficher le contenu du CSV sur la console (2 lignes blanches avant)
        print("\n\n\n", end="")
        try:
            with open(csv_temp, "r", encoding="utf-8") as f:
                print(f.read(), end="")
        except Exception as e:
            debug_print(True, f"Impossible de lire le fichier CSV pour affichage: {e}")
        print(
            f"\n{written_count_ref[0]} enregistrements uniques (fichier: {csv_temp})",
            file=sys.stderr,
        )
    else:
        # Copie du temporaire vers le fichier final (évite WinError 32 au rename)
        try:
            with open(csv_temp, "r", encoding="utf-8") as src:
                with open(final_name, "w", newline="", encoding="utf-8") as dst:
                    dst.write(src.read())
            try:
                os.remove(csv_temp)
            except OSError:
                pass
            debug_print(True, f"Fichier écrit: {final_name}")
        except Exception as e:
            debug_print(True, f"Impossible d'écrire le fichier final: {e}. Données dans {csv_temp}")
            final_name = csv_temp
        print(
            f"{written_count_ref[0]} enregistrements uniques écrits dans {final_name}",
            file=sys.stderr,
        )

if __name__ == "__main__":
    main()

