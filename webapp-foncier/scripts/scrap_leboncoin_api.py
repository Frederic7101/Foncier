#!/usr/bin/env python3
"""
Extraction des annonces de location Leboncoin via interception de l'API finder/search.

Deux modes d'acquisition :

  1. Import HAR (manuel) :
     python scrap_leboncoin_api.py --from-har export.har
     → Parse un fichier .har exporté depuis F12 > Network > "Save all as HAR"

  2. Playwright (automatisé) :
     python scrap_leboncoin_api.py --commune Laon --code-postal 02000
     python scrap_leboncoin_api.py --commune Laon --code-postal 02000 --headless
     → Ouvre un navigateur Chromium, navigue sur Leboncoin, intercepte les réponses API
       et parcourt toutes les pages automatiquement.

  Options communes :
     --dry-run   : affiche sans écrire en base
     --max-pages : nombre max de pages à parcourir (défaut 10)

Les annonces sont insérées dans foncier.leboncoin_locations_brut
(ON CONFLICT DO NOTHING sur url_annonce).
"""

import argparse
import base64
import json
import random
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent

# Particules françaises minuscules dans les noms de communes pour les slugs URL
_FR_PARTICLES = frozenset({
    "le", "la", "les", "de", "du", "des", "en", "sur", "sous",
    "lès", "lè", "au", "aux", "d", "l", "et",
})


def _commune_to_lbc_slug(commune: str) -> str:
    """Normalise un nom de commune en slug Leboncoin : Noisy-le-Grand, Saint-Denis, etc.

    Leboncoin attend un format Title-Case avec particules en minuscules, tirets entre les mots.
    Ex. NOISY-LE-GRAND → Noisy-le-Grand, BOURG FIDELE → Bourg-Fidele, L'ISLE-SUR-LA-SORGUE → L-Isle-sur-la-Sorgue
    """
    slug = commune.strip().replace(" ", "-").replace("'", "-").replace("\u2019", "-")
    parts = slug.split("-")
    result: list[str] = []
    for i, part in enumerate(parts):
        if not part:
            continue
        if i > 0 and part.lower() in _FR_PARTICLES:
            result.append(part.lower())
        else:
            result.append(part.capitalize())
    return "-".join(result)


def _geocode_commune(commune: str, code_postal: str) -> Optional[Tuple[float, float]]:
    """Récupère (lat, lng) via geo.api.gouv.fr. Retourne None en cas d'échec."""
    try:
        url = (
            "https://geo.api.gouv.fr/communes?"
            + urllib.parse.urlencode({"codePostal": code_postal, "fields": "centre", "format": "json"})
        )
        req = urllib.request.Request(url, headers={"User-Agent": "webapp-foncier/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if not data:
            return None
        centre = data[0].get("centre", {})
        coords = centre.get("coordinates", [])
        if len(coords) >= 2:
            lng, lat = coords[0], coords[1]
            return (round(lat, 5), round(lng, 5))
    except Exception as exc:
        print(f"  [!] Géocodage échoué pour {commune} ({code_postal}) : {exc}")
    return None


def _build_lbc_search_url(commune: str, code_postal: str, radius_m: int = 5000) -> str:
    """Construit l'URL de recherche Leboncoin locations pour une commune.

    Format attendu par Leboncoin :
      /recherche?category=10&locations=Noisy-le-Grand_93160__48.84887_2.55404_5000
    - category=10 = Locations
    - locations = {slug}_{cp}__{lat}_{lng}_{rayon_m}
    """
    slug = _commune_to_lbc_slug(commune)
    cp = code_postal.strip().zfill(5)

    coords = _geocode_commune(commune, cp)
    if coords:
        lat, lng = coords
        loc_value = f"{slug}_{cp}__{lat}_{lng}_{radius_m}"
        print(f"  Géocodage OK : lat={lat}, lng={lng} (rayon {radius_m}m)")
    else:
        loc_value = f"{slug}_{cp}"
        print(f"  [!] Pas de coordonnées — recherche sans rayon géographique")

    return f"https://www.leboncoin.fr/recherche?category=10&locations={loc_value}"


# ─────────────────────────────────────────────────────────────────────────────
# Configuration DB (réutilise config.postgres.json)
# ─────────────────────────────────────────────────────────────────────────────
_CONFIG_DIRS = (SCRIPT_DIR, SCRIPT_DIR.parent, SCRIPT_DIR.parent.parent)


def _get_db_config() -> dict:
    for base in _CONFIG_DIRS:
        p = base / "config.postgres.json"
        if p.is_file():
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            db = data.get("database") or data
            return {
                "host": db.get("host"),
                "port": int(db.get("port") or 5432),
                "dbname": db.get("database", "foncier"),
                "user": db.get("user"),
                "password": db.get("password") or "",
            }
    raise RuntimeError("config.postgres.json introuvable")


# ─────────────────────────────────────────────────────────────────────────────
# Mapping type de bien (valeur numérique API → libellé)
# ─────────────────────────────────────────────────────────────────────────────
_TYPE_BIEN_MAP = {
    1: "Maison",
    2: "Appartement",
    3: "Terrain",
    4: "Parking",
    5: "Commerce",
    6: "Bureau",
    7: "Loft",
    8: "Chambre",
    9: "Immeuble",
}

_FURNISHED_MAP = {
    1: True,   # Meublé
    2: False,  # Non meublé
}


# ─────────────────────────────────────────────────────────────────────────────
# Parsing d'une annonce depuis la réponse API finder/search
# ─────────────────────────────────────────────────────────────────────────────
def _parse_api_ad(ad: dict) -> Optional[dict]:
    """Parse une annonce issue du JSON de réponse API → dict prêt pour insertion."""
    if not ad or not ad.get("list_id"):
        return None

    # Attributs sous forme clé-valeur
    attrs: Dict[str, str] = {}
    for a in ad.get("attributes", []):
        key = a.get("key", "")
        val = a.get("value")
        if key and val is not None:
            attrs[key] = str(val)

    location = ad.get("location", {})
    owner = ad.get("owner", {})

    # Type de bien
    rt = attrs.get("real_estate_type")
    if rt and rt.isdigit():
        type_bien = _TYPE_BIEN_MAP.get(int(rt), f"Type {rt}")
    else:
        type_bien = rt

    # Surface, pièces, chambres
    surface = None
    sq = attrs.get("square")
    if sq:
        try:
            surface = float(sq)
        except ValueError:
            pass

    nb_pieces = None
    rms = attrs.get("rooms")
    if rms and rms.isdigit():
        nb_pieces = int(rms)

    nb_chambres = None
    bdr = attrs.get("bedrooms")
    if bdr and bdr.isdigit():
        nb_chambres = int(bdr)

    # Loyer (price_cents → euros)
    loyer = None
    pc = ad.get("price_cents")
    if pc:
        loyer = int(pc) // 100
    elif ad.get("price"):
        prices = ad["price"]
        if isinstance(prices, list) and prices:
            loyer = int(prices[0])

    # Loyer HC
    loyer_hc = None
    rhc = attrs.get("rent_excluding_charges")
    if rhc and rhc.isdigit():
        loyer_hc = int(rhc)

    # Charges
    charges = None
    mc = attrs.get("monthly_charges")
    if mc and mc.isdigit():
        charges = int(mc)

    # Charges incluses
    ci = attrs.get("charges_included")
    charges_incluses = (ci == "1") if ci else None

    # Meublé
    furn = attrs.get("furnished")
    meuble = None
    if furn and furn.isdigit():
        meuble = _FURNISHED_MAP.get(int(furn))

    # DPE / GES
    dpe_classe = (attrs.get("energy_rate") or "").upper() or None
    ges_classe = (attrs.get("ges") or "").upper() or None

    # Date de publication
    pub_raw = ad.get("first_publication_date", "")
    date_publication = pub_raw[:10] if pub_raw and len(pub_raw) >= 10 else None

    # URL
    url = ad.get("url", "")
    if not url:
        list_id = ad.get("list_id")
        url = f"https://www.leboncoin.fr/ad/locations/{list_id}"

    # Code département
    dept = location.get("department_id")
    if dept is not None:
        dept = str(dept)

    return {
        "url_annonce": url,
        "ref_leboncoin": str(ad.get("list_id", "")),
        "type_bien": type_bien,
        "titre": ad.get("subject"),
        "commune": location.get("city", ""),
        "code_postal": location.get("zipcode", ""),
        "code_dept": dept,
        "nb_pieces": nb_pieces,
        "nb_chambres": nb_chambres,
        "surface": surface,
        "loyer": loyer,
        "loyer_hc": loyer_hc,
        "charges": charges,
        "charges_incluses": charges_incluses,
        "meuble": meuble,
        "dpe_classe": dpe_classe,
        "dpe_valeur": None,
        "ges_classe": ges_classe,
        "ges_valeur": None,
        "annonceur_type": owner.get("type"),
        "annonceur_nom": owner.get("name"),
        "lien_source": url,
        "description": ad.get("body"),
        "date_publication": date_publication,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Extraction des annonces depuis une réponse API JSON
# ─────────────────────────────────────────────────────────────────────────────
def _extract_ads_from_api_response(data: dict) -> List[dict]:
    """Extrait et parse les annonces depuis un JSON de réponse API."""
    raw_ads = data.get("ads", [])
    parsed = []
    for ad in raw_ads:
        p = _parse_api_ad(ad)
        if p and p["url_annonce"]:
            parsed.append(p)
    return parsed


# ─────────────────────────────────────────────────────────────────────────────
# Mode 1 : Import HAR
# ─────────────────────────────────────────────────────────────────────────────
def extract_from_har(har_path: str) -> List[dict]:
    """Parse un fichier .har et extrait toutes les annonces des réponses API finder/search."""
    with open(har_path, encoding="utf-8") as f:
        har = json.load(f)

    entries = har.get("log", {}).get("entries", [])
    all_ads: List[dict] = []
    seen_urls: set = set()

    api_entries = [
        e for e in entries
        if "api.leboncoin.fr/finder/search" in e.get("request", {}).get("url", "")
        and e.get("response", {}).get("status") == 200
    ]

    print(f"  {len(api_entries)} réponses API finder/search trouvées dans le HAR")

    for i, entry in enumerate(api_entries):
        content = entry.get("response", {}).get("content", {})
        text = content.get("text", "")
        encoding = content.get("encoding", "")

        if not text:
            print(f"  [!] Entry {i} : contenu vide, ignorée")
            continue

        # Décoder si base64
        if encoding == "base64":
            try:
                text = base64.b64decode(text).decode("utf-8")
            except Exception as e:
                print(f"  [!] Entry {i} : erreur décodage base64 : {e}")
                continue

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            print(f"  [!] Entry {i} : JSON invalide : {e}")
            continue

        ads = _extract_ads_from_api_response(data)

        # Dédupliquer par URL
        new_ads = []
        for ad in ads:
            if ad["url_annonce"] not in seen_urls:
                seen_urls.add(ad["url_annonce"])
                new_ads.append(ad)

        all_ads.extend(new_ads)
        total = data.get("total", "?")
        print(f"  Entry {i} : {len(ads)} annonces extraites ({len(new_ads)} nouvelles) — total API : {total}")

    return all_ads


# ─────────────────────────────────────────────────────────────────────────────
# Mode 2 : Playwright (navigateur automatisé + interception réseau)
# ─────────────────────────────────────────────────────────────────────────────
def extract_with_playwright(
    commune: str,
    code_postal: str,
    max_pages: int = 10,
    headless: bool = False,
    timeout_page: int = 30_000,
) -> List[dict]:
    """Ouvre Chromium, navigue sur Leboncoin et intercepte les réponses API."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[!] playwright non installé. Installez-le avec : pip install playwright && playwright install chromium")
        sys.exit(1)

    all_ads: List[dict] = []
    seen_urls: set = set()
    api_responses: list = []

    def _on_response(response):
        """Callback déclenché à chaque réponse HTTP."""
        if "api.leboncoin.fr/finder/search" not in response.url:
            return
        if response.status != 200:
            return
        try:
            data = response.json()
            ads = _extract_ads_from_api_response(data)
            new_count = 0
            for ad in ads:
                if ad["url_annonce"] not in seen_urls:
                    seen_urls.add(ad["url_annonce"])
                    all_ads.append(ad)
                    new_count += 1
            total = data.get("total", "?")
            api_responses.append(data)
            print(f"  [API] {len(ads)} annonces capturées ({new_count} nouvelles) — total : {total}")
        except Exception as e:
            print(f"  [!] Erreur parsing réponse API : {e}")

    # Construire l'URL de recherche (format Leboncoin avec géocodage)
    search_url = _build_lbc_search_url(commune, code_postal)

    print(f"  URL : {search_url}")
    print(f"  Mode : {'headless' if headless else 'visible (résolvez le CAPTCHA si nécessaire)'}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="fr-FR",
            timezone_id="Europe/Paris",
        )

        # Masquer les signes d'automatisation
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)

        page = context.new_page()
        page.on("response", _on_response)

        # Naviguer vers la page de recherche
        print("  Navigation vers Leboncoin...")
        page.goto(search_url, wait_until="domcontentloaded", timeout=60_000)

        # Attendre que la page charge (les annonces apparaissent)
        try:
            page.wait_for_selector("[data-qa-id='aditem_container'], [data-test-id='ad']", timeout=timeout_page)
        except Exception:
            # Peut-être un CAPTCHA DataDome — attendre que l'utilisateur le résolve
            print("  [!] Page non chargée — CAPTCHA ? Attente de 60s max pour résolution manuelle...")
            try:
                page.wait_for_selector("[data-qa-id='aditem_container'], [data-test-id='ad']", timeout=60_000)
            except Exception:
                print("  [!] Timeout : impossible de charger les annonces")
                browser.close()
                return all_ads

        # --- Extraction des annonces SSR (page 1 via __NEXT_DATA__) ---
        # Leboncoin utilise Next.js : les annonces de la page 1 sont embarquées
        # dans le HTML initial (balise <script id="__NEXT_DATA__">), et ne
        # passent PAS par l'API XHR interceptée par _on_response. Il faut donc
        # les extraire directement depuis le DOM.
        try:
            next_data_json = page.evaluate("""
                () => {
                    const el = document.getElementById('__NEXT_DATA__');
                    return el ? el.textContent : null;
                }
            """)
            if next_data_json:
                next_data = json.loads(next_data_json)
                # Chemin probable dans le JSON Next.js de Leboncoin
                ads_raw = (
                    next_data.get("props", {})
                    .get("pageProps", {})
                    .get("initialProps", {})
                    .get("searchData", {})
                    .get("ads", [])
                )
                # Chemin alternatif parfois utilisé
                if not ads_raw:
                    ads_raw = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("searchData", {})
                        .get("ads", [])
                    )
                # Autre chemin alternatif
                if not ads_raw:
                    ads_raw = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("data", {})
                        .get("ads", [])
                    )
                ssr_new = 0
                for ad in ads_raw:
                    parsed = _parse_api_ad(ad)
                    if parsed and parsed["url_annonce"] not in seen_urls:
                        seen_urls.add(parsed["url_annonce"])
                        all_ads.append(parsed)
                        ssr_new += 1
                print(f"  [SSR] {len(ads_raw)} annonces dans __NEXT_DATA__ → {ssr_new} nouvelles (total {len(all_ads)})")
            else:
                print("  [SSR] Pas de balise __NEXT_DATA__ trouvée")
        except Exception as e:
            print(f"  [!] Erreur extraction __NEXT_DATA__ : {e}")

        print(f"  Page 1 chargée — {len(all_ads)} annonces capturées")

        # Parcourir les pages suivantes
        for page_num in range(2, max_pages + 1):
            pause = random.uniform(2.0, 5.0)
            time.sleep(pause)

            # Chercher le bouton/lien "page suivante"
            next_selectors = [
                f"a[href*='/p-{page_num}']",
                "button[aria-label='Page suivante']",
                "a[aria-label='Page suivante']",
                "[data-qa-id='pagination'] a:last-child",
            ]

            clicked = False
            for sel in next_selectors:
                try:
                    el = page.locator(sel).first
                    if el.is_visible(timeout=3_000):
                        ads_before = len(all_ads)
                        el.click()
                        # Attendre la réponse API
                        page.wait_for_load_state("networkidle", timeout=timeout_page)
                        time.sleep(1)
                        ads_after = len(all_ads)
                        if ads_after > ads_before:
                            print(f"  Page {page_num} : +{ads_after - ads_before} annonces (total {ads_after})")
                        else:
                            print(f"  Page {page_num} : aucune nouvelle annonce")
                        clicked = True
                        break
                except Exception:
                    continue

            if not clicked:
                print(f"  Pas de page {page_num} — fin de la pagination")
                break

        browser.close()

    return all_ads


# ─────────────────────────────────────────────────────────────────────────────
# Insertion en base
# ─────────────────────────────────────────────────────────────────────────────
_INSERT_COLS = (
    "url_annonce", "ref_leboncoin", "type_bien", "titre",
    "commune", "code_postal", "code_dept",
    "nb_pieces", "nb_chambres", "surface",
    "loyer", "loyer_hc", "charges", "charges_incluses", "meuble",
    "dpe_classe", "dpe_valeur", "ges_classe", "ges_valeur",
    "annonceur_type", "annonceur_nom", "lien_source", "description",
    "date_publication", "date_scraping",
)

_INSERT_SQL = (
    f"INSERT INTO foncier.leboncoin_locations_brut ({', '.join(_INSERT_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_INSERT_COLS))}) "
    "ON CONFLICT (url_annonce) DO NOTHING"
)


def _build_insert_row(listing: dict, now: datetime) -> tuple:
    """Construit le tuple d'insertion."""
    return tuple(
        listing.get(col) if col != "date_scraping" else now
        for col in _INSERT_COLS
    )


def insert_ads(ads: List[dict], dry_run: bool = False) -> Tuple[int, int]:
    """Insère les annonces en base. Retourne (nouvelles, ignorées)."""
    if not ads:
        return 0, 0

    import psycopg2
    from psycopg2.extras import execute_batch

    now = datetime.now()
    rows = [_build_insert_row(a, now) for a in ads]

    if dry_run:
        print(f"\n  [dry-run] {len(rows)} annonces prêtes à insérer")
        for r in rows[:5]:
            print(f"    {r[0]} | {r[2]} | {r[10]} €/mois | {r[9]} m²")
        if len(rows) > 5:
            print(f"    ... et {len(rows) - 5} autres")
        return len(rows), 0

    db_cfg = _get_db_config()
    conn = psycopg2.connect(**db_cfg)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Vérifier les doublons existants
            urls = [r[0] for r in rows]
            cur.execute(
                "SELECT url_annonce FROM foncier.leboncoin_locations_brut WHERE url_annonce = ANY(%s)",
                (urls,)
            )
            existing = {row[0] for row in cur.fetchall()}

            execute_batch(cur, _INSERT_SQL, rows, page_size=200)
            conn.commit()

            new_count = len(rows) - len(existing)
            skip_count = len(existing)
            return new_count, skip_count
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Extraction Leboncoin via API (HAR ou Playwright) → PostgreSQL"
    )
    parser.add_argument("--from-har", type=str, default=None,
                        help="Fichier .har à importer (exporté depuis F12 > Network)")
    parser.add_argument("--commune", type=str, default=None,
                        help="Nom de la commune (mode Playwright)")
    parser.add_argument("--code-postal", type=str, default=None,
                        help="Code postal (mode Playwright)")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="Nombre max de pages à parcourir (défaut 10)")
    parser.add_argument("--headless", action="store_true",
                        help="Mode headless (sans fenêtre visible)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Afficher sans écrire en base")
    parser.add_argument("--json-output", action="store_true",
                        help="Sortie JSON (pour appel depuis le backend)")
    args = parser.parse_args()

    if not args.from_har and not args.commune:
        parser.error("Spécifiez --from-har FICHIER.har ou --commune NOM --code-postal CP")
    if args.commune and not args.code_postal:
        parser.error("--code-postal requis avec --commune")

    # ── Extraction ──
    if args.from_har:
        print(f"Import HAR : {args.from_har}")
        ads = extract_from_har(args.from_har)
    else:
        print(f"Playwright : {args.commune} ({args.code_postal})")
        ads = extract_with_playwright(
            args.commune,
            args.code_postal,
            max_pages=args.max_pages,
            headless=args.headless,
        )

    print(f"\n{len(ads)} annonces extraites au total")

    if not ads:
        if args.json_output:
            print(json.dumps({"status": "ok", "extracted": 0, "inserted": 0, "skipped": 0}))
        return

    # ── Insertion ──
    new_count, skip_count = insert_ads(ads, dry_run=args.dry_run)
    marker = " (dry-run)" if args.dry_run else ""
    print(f"Terminé{marker} : {new_count} nouvelles annonces, {skip_count} déjà en base")

    if args.json_output:
        print(json.dumps({
            "status": "ok",
            "extracted": len(ads),
            "inserted": new_count,
            "skipped": skip_count,
        }))


if __name__ == "__main__":
    main()
