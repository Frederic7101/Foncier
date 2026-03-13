#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
import_encheres_publiques.py

Script d'import des annonces immobilières depuis encheres-publiques.com.

Usage basique :
    python import_encheres_publiques.py \
        --url "https://www.encheres-publiques.com/ventes/immobilier"

Options :
    --url URL            URL à importer (page "Immobilier" ou une page d'événements)
    --csv PATH           Chemin vers un fichier CSV de sortie (optionnel)
    --timeout SECS       Timeout HTTP en secondes (défaut: 20)
    --debug              Affiche quelques infos de debug sur stderr
"""

import argparse
import csv
import json
import sys
from typing import List, Dict, Any

import requests
from bs4 import BeautifulSoup


def debug_print(enabled: bool, *args, **kwargs) -> None:
    if enabled:
        print(*args, file=sys.stderr, **kwargs)


def _extract_from_html(html: str, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Extrait les événements & lots immobiliers à partir du HTML d'une page
    encheres-publiques.com, en lisant le script JSON __NEXT_DATA__.
    """
    soup = BeautifulSoup(html, "html.parser")

    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        raise RuntimeError("Script __NEXT_DATA__ introuvable sur la page.")

    data = json.loads(script.string)

    apollo_state = (
        data
        .get("props", {})
        .get("pageProps", {})
        .get("apolloState", {})
        .get("data", {})
    )
    if not apollo_state:
        raise RuntimeError("Structure apolloState.data introuvable dans le JSON.")

    def resolve(ref: Dict) -> Dict:
        """Résout un objet par son __ref (ex: {'__ref': 'Evenement:21147'})."""
        key = ref.get("__ref")
        if key is None or key not in apollo_state:
            return {}
        return apollo_state[key]

    # 1) Récupérer tous les objets de type Evenement dans apollo_state
    evenements_objs: List[Dict[str, Any]] = []
    for key, value in apollo_state.items():
        if not isinstance(value, dict):
            continue
        if value.get("__typename") != "Evenement":
            continue
        evenements_objs.append(value)

    # Dé-duplication éventuelle
    seen_ids = set()
    filtered_evenements = []
    for ev in evenements_objs:
        ev_id = ev.get("id")
        if not ev_id or ev_id in seen_ids:
            continue
        seen_ids.add(ev_id)
        filtered_evenements.append(ev)

    debug_print(debug, f"Nombre d'événements (tous types) trouvés: {len(filtered_evenements)}")

    results: List[Dict[str, Any]] = []

    for ev in filtered_evenements:
        ev_id = ev.get("id")
        titre = ev.get("titre") or ""
        ouverture_ts = ev.get("ouverture_date")  # timestamp en secondes (int)
        fermeture_ts = ev.get("fermeture_date")

        # On ne garde que les événements immobiliers (filtre sur le titre ou la présence d'une mosaïque 'immobilier')
        titre_lower = titre.lower()
        is_immobilier = "immobil" in titre_lower  # “immobilière”, “immobilier”, etc.

        # Organisateur (tribunal / maison de ventes)
        org = {}
        if isinstance(ev.get("organisateur"), dict):
            org = resolve(ev["organisateur"])
        org_id = org.get("id")
        org_nom = org.get("nom")

        # Adresse (ville / région)
        adr = {}
        if isinstance(ev.get("adresse"), dict):
            adr = resolve(ev["adresse"])
        ville = adr.get("ville")
        ville_slug = adr.get("ville_slug")
        region = adr.get("region")

        # Lots immobiliers: clé variable
        mos_key_candidates = [
            'mosaique({"categorie":"immobilier"})',
            'mosaique({\"categorie\":\"immobilier\"})',
        ]
        mos = None
        for k in mos_key_candidates:
            if k in ev:
                mos = ev[k]
                break

        # Si pas de mosaïque spécifique 'immobilier', on essaie quand même un champ 'mosaique' brut
        if mos is None and "mosaique" in ev and isinstance(ev["mosaique"], dict):
            mos = ev["mosaique"]

        # Si aucun lot associé, on passe
        if not mos:
            continue

        # Si on n'avait pas encore marqué l'événement comme "immobilier", mais qu'il a une mosaïque 'immobilier',
        # on le considère comme tel.
        if not is_immobilier:
            is_immobilier = True

        if not is_immobilier:
            continue

        collection = mos.get("collection") or []
        debug_print(debug, f"  Événement {ev_id} - '{titre}' - nb de lots: {len(collection)}")

        for lot_ref in collection:
            lot = resolve(lot_ref)
            lot_id = lot.get("id")
            lot_nom = lot.get("nom")
            lot_photo = lot.get("photo")

            if not lot_id or not lot_nom:
                continue

            results.append({
                "evenement_id": ev_id,
                "evenement_titre": titre,
                "evenement_ouverture_ts": ouverture_ts,
                "evenement_fermeture_ts": fermeture_ts,
                "organisateur_id": org_id,
                "organisateur_nom": org_nom,
                "ville": ville,
                "ville_slug": ville_slug,
                "region": region,
                "lot_id": lot_id,
                "lot_nom": lot_nom,
                "lot_photo": lot_photo,
            })

    debug_print(debug, f"Total lots immobiliers trouvés: {len(results)}")
    return results

def fetch_encheres_immobilier(url: str, timeout: int = 20, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Télécharge la page à l'URL donnée et retourne la liste des lots immobiliers
    (événements + lots) sous forme de dicts.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    }
    debug_print(debug, f"Téléchargement de la page : {url}")
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    html = resp.text
    return _extract_from_html(html, debug=debug)


def write_csv(records: List[Dict[str, Any]], path: str) -> None:
    """
    Écrit les enregistrements dans un fichier CSV.
    """
    if not records:
        # Rien à écrire
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("")  # fichier vide
        return

    fieldnames = list(records[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import des annonces immobilières depuis encheres-publiques.com"
    )
    parser.add_argument(
        "--url",
        required=True,
        help="URL de la page à importer (ex: https://www.encheres-publiques.com/ventes/immobilier)",
    )
    parser.add_argument(
        "--csv",
        help="Chemin d'un fichier CSV de sortie (optionnel). "
             "Si non fourni, les données sont affichées en JSON sur stdout.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Timeout HTTP en secondes (défaut: 20)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Affiche des informations de debug sur stderr",
    )

    args = parser.parse_args()

    try:
        records = fetch_encheres_immobilier(args.url, timeout=args.timeout, debug=args.debug)
    except Exception as e:
        print(f"Erreur lors de l'import : {e}", file=sys.stderr)
        sys.exit(1)

    if args.csv:
        write_csv(records, args.csv)
        print(f"{len(records)} enregistrements écrits dans {args.csv}", file=sys.stderr)
    else:
        # Affichage JSON prettifié sur stdout
        json.dump(records, sys.stdout, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()