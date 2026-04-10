#!/usr/bin/env python3
"""
Scraping des résultats d'adjudications Licitor.fr → PostgreSQL (foncier.licitor_brut).

Scrape l'historique des adjudications pour une ou toutes les régions du site
Licitor.fr et insère les nouvelles annonces dans la table PostgreSQL
foncier.licitor_brut.  Les annonces déjà présentes en base sont ignorées
(détection par url_annonce + ON CONFLICT DO NOTHING en filet de sécurité).

Usage :
    python scrap_licitor.py --region paris-et-ile-de-france
    python scrap_licitor.py --all
    python scrap_licitor.py --list-regions
    python scrap_licitor.py --region sud-est-mediterrannee --max-pages 10
    python scrap_licitor.py --region paris-et-ile-de-france --with-details
    python scrap_licitor.py --all --start-page 50

Options :
    --region SLUG     Scraper une seule région (voir --list-regions)
    --all             Scraper toutes les régions
    --list-regions    Lister les régions disponibles et quitter
    --with-details    Scraper aussi la page détail de chaque nouvelle annonce
                      (descriptions complètes, montant adjudication précis ;
                       beaucoup plus lent : 1 requête / annonce)
    --max-pages N     Limiter le nombre de pages à scraper par région
    --start-page N    Page de départ (défaut 1, utile pour reprendre après crash)
    --dry-run         Afficher ce qui serait inséré, sans écrire en base
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent

# ─────────────────────────────────────────────────────────────────────────────
# Régions disponibles sur Licitor.fr  (slug URL → label source_region)
# ─────────────────────────────────────────────────────────────────────────────
REGIONS: Dict[str, str] = {
    "paris-et-ile-de-france": "Ile-de-France",
    "regions-du-nord-est":    "Nord-Est",
    "bretagne-grand-ouest":   "Bretagne / Grand Ouest",
    "centre-loire-limousin":  "Centre / Loire / Limousin",
    "sud-ouest-pyrenees":     "Sud-Ouest / Pyrénées",
    "sud-est-mediterrannee":  "Sud-Est / Méditerranée",
}

BASE_URL = "https://www.licitor.com"

# ─────────────────────────────────────────────────────────────────────────────
# Configuration PostgreSQL
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
    raise RuntimeError("config.postgres.json introuvable dans " + ", ".join(str(d) for d in _CONFIG_DIRS))


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

_session = requests.Session()


def _safe_request(url: str, retries: int = 3) -> requests.Response:
    """GET avec délai aléatoire, retries et rotation du User-Agent."""
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(1.0, 3.0))
            _session.headers["User-Agent"] = random.choice(_USER_AGENTS)
            resp = _session.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            if attempt < retries - 1:
                wait = 5 * (2 ** attempt)
                print(f"  [!] {exc} — retry dans {wait}s")
                time.sleep(wait)
            else:
                raise


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaires de parsing
# ─────────────────────────────────────────────────────────────────────────────
def _parse_montant(value) -> Optional[int]:
    """Convertit '374 000 €' ou '374000' en entier."""
    if not value:
        return None
    s = str(value).strip()
    for ch in ("€", "\u00a0", " "):
        s = s.replace(ch, "")
    s = s.replace(",", ".")
    cleaned = "".join(c for c in s if c.isdigit() or c == ".")
    if not cleaned:
        return None
    try:
        n = float(cleaned)
    except ValueError:
        return None
    return int(n) if n.is_integer() else int(n) + 1


# ─────────────────────────────────────────────────────────────────────────────
# URLs historique
# ─────────────────────────────────────────────────────────────────────────────
def _historique_url(region_slug: str, page: int = 1) -> str:
    url = f"{BASE_URL}/ventes-aux-encheres-immobilieres/{region_slug}/historique-des-adjudications.html"
    if page > 1:
        url += f"?p={page}"
    return url


# ─────────────────────────────────────────────────────────────────────────────
# Extraction des données depuis le HTML
# ─────────────────────────────────────────────────────────────────────────────
def _extract_pagination(soup: BeautifulSoup) -> Tuple[int, int]:
    """Retourne (total_annonces, nb_pages) depuis le HTML de la page listing."""
    form = soup.find("form", class_="PageField")
    if not form:
        return 0, 0
    total_input = form.find("input", {"name": "total"})
    total = int(total_input["value"]) if total_input else 0
    span = form.find("span", class_="PageTotal")
    nb_pages = int(span.text.replace("/", "").strip()) if span else 1
    return total, nb_pages


def _extract_listings(soup: BeautifulSoup) -> List[dict]:
    """Extrait les annonces depuis le HTML d'une page listing."""
    links = soup.select("a[href*='/annonce/']")
    results = []

    for a_tag in links:
        try:
            href = a_tag.get("href", "")
            if not href:
                continue
            url = (BASE_URL + href) if href.startswith("/") else href

            dept_el = a_tag.select_one(".Number")
            code_dept = dept_el.text.strip() if dept_el else None

            ville_el = a_tag.select_one(".City")
            commune = ville_el.text.strip() if ville_el else None

            nom_el = a_tag.select_one(".Name")
            texte_el = a_tag.select_one(".Text")
            nom = nom_el.text.strip() if nom_el else ""
            texte = texte_el.text.strip() if texte_el else ""
            desc = f"{nom} - {texte}" if texte else nom

            prix_el = a_tag.select_one(".PriceNumber")
            montant = _parse_montant(prix_el.text.strip() if prix_el else None)

            # Date : chercher dans/autour du lien
            date_texte = None
            # 1) <time> avec attribut datetime (le plus fiable)
            time_el = a_tag.select_one("time")
            if time_el:
                date_texte = time_el.get("datetime") or time_el.text.strip()
            # 2) élément .Date à l'intérieur du lien
            if not date_texte:
                date_el = a_tag.select_one(".Date")
                if date_el:
                    date_texte = date_el.text.strip()
            # 3) PublishingDate (frère suivant du lien)
            if not date_texte:
                pub = a_tag.find_next(class_="PublishingDate")
                if pub:
                    date_texte = pub.text.strip()

            results.append({
                "url_annonce": url,
                "code_dept": code_dept,
                "commune": commune,
                "desc_courte": desc,
                "montant_adjudication": montant,
                "date_vente_texte": date_texte,
            })
        except Exception as exc:
            print(f"  [!] Erreur parsing annonce : {exc}")

    return results


def _scrape_detail(url: str) -> Optional[dict]:
    """Scrape la page détail pour obtenir la description complète et le montant exact."""
    try:
        resp = _safe_request(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        article = soup.find("article", class_="LegalAd")
        if not article:
            return None

        # Description complète (concaténation des lots)
        lots = article.select(".SousLot, .FirstSousLot")
        descs: List[str] = []
        for lot in lots:
            h = lot.find("h2")
            p = lot.find("p")
            h_t = h.text.strip() if h else ""
            p_t = p.text.strip() if p else ""
            descs.append(f"{h_t} : {p_t}" if p_t else h_t)
        desc_complete = " | ".join(descs) if descs else None

        # Montant adjudication
        adj_el = article.find("h3")
        adj_text = adj_el.text.replace("Adjudication :", "").strip() if adj_el else None

        # Date adjudication
        time_el = article.select_one(".Date time")
        date_adj = time_el["datetime"] if time_el and time_el.get("datetime") else None

        return {
            "desc_courte": desc_complete,
            "montant_adjudication": _parse_montant(adj_text),
            "date_vente_texte": date_adj,
        }
    except Exception as exc:
        print(f"  [!] Erreur détail {url} : {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Insertion PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
_INSERT_COLS = ("source_region", "url_annonce", "code_dept", "commune",
                "desc_courte", "montant_adjudication", "date_vente_texte", "date_scraping")

_INSERT_SQL = (
    f"INSERT INTO foncier.licitor_brut ({', '.join(_INSERT_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_INSERT_COLS))}) "
    "ON CONFLICT (url_annonce, desc_courte, montant_adjudication) DO NOTHING"
)


def _load_existing_urls(conn) -> Set[str]:
    """Charge l'ensemble des url_annonce déjà présentes en base."""
    with conn.cursor() as cur:
        cur.execute("SELECT url_annonce FROM foncier.licitor_brut")
        return {r[0] for r in cur.fetchall()}


# ─────────────────────────────────────────────────────────────────────────────
# Boucle principale de scraping d'une région
# ─────────────────────────────────────────────────────────────────────────────
def scrape_region(conn, region_slug: str, *,
                  with_details: bool = False,
                  max_pages: Optional[int] = None,
                  start_page: int = 1,
                  dry_run: bool = False) -> int:
    source_region = REGIONS[region_slug]
    print(f"\n{'=' * 60}")
    print(f"Region : {source_region}  ({region_slug})")
    print(f"{'=' * 60}")

    # Page 1 : récupérer la pagination ET les premières annonces en une seule requête
    resp = _safe_request(_historique_url(region_slug, start_page))
    soup = BeautifulSoup(resp.text, "html.parser")
    total, nb_pages = _extract_pagination(soup)
    if total == 0:
        print("  Aucune annonce trouvee.")
        return 0

    last_page = min(nb_pages, max_pages) if max_pages else nb_pages
    print(f"  {total} annonces au total, pages {start_page}..{last_page} a parcourir")

    existing_urls = _load_existing_urls(conn)
    print(f"  {len(existing_urls)} annonces deja en base (toutes regions)")

    inserted_total = 0
    skipped_total = 0
    consecutive_empty = 0
    now = datetime.now()

    for page in range(start_page, last_page + 1):
        # La page de départ a déjà été récupérée
        if page == start_page:
            annonces = _extract_listings(soup)
        else:
            annonces = _extract_listings(
                BeautifulSoup(_safe_request(_historique_url(region_slug, page)).text, "html.parser")
            )

        new_annonces: List[dict] = []
        for a in annonces:
            if a["url_annonce"] in existing_urls:
                skipped_total += 1
                continue

            # Page détail si demandé
            if with_details:
                detail = _scrape_detail(a["url_annonce"])
                if detail:
                    if detail.get("desc_courte"):
                        a["desc_courte"] = detail["desc_courte"]
                    if detail.get("montant_adjudication") is not None:
                        a["montant_adjudication"] = detail["montant_adjudication"]
                    if detail.get("date_vente_texte"):
                        a["date_vente_texte"] = detail["date_vente_texte"]

            new_annonces.append(a)
            existing_urls.add(a["url_annonce"])

        # Insertion par lot (par page)
        if new_annonces and not dry_run:
            from psycopg2.extras import execute_batch
            rows = [
                (source_region, a["url_annonce"], a["code_dept"], a["commune"],
                 a["desc_courte"], a["montant_adjudication"], a["date_vente_texte"], now)
                for a in new_annonces
            ]
            with conn.cursor() as cur:
                execute_batch(cur, _INSERT_SQL, rows, page_size=200)
            conn.commit()

        inserted_total += len(new_annonces)
        nb_exist = len(annonces) - len(new_annonces)
        marker = " (dry-run)" if dry_run else ""
        print(f"  Page {page:>5}/{last_page}  |  {len(annonces)} ann.  "
              f"{len(new_annonces)} nouvelles  {nb_exist} existantes{marker}")

        # Indicateur : si plusieurs pages consécutives n'apportent rien de nouveau
        if len(new_annonces) == 0:
            consecutive_empty += 1
            if consecutive_empty >= 5:
                print(f"\n  [info] 5 pages consecutives sans nouvelle annonce.")
                print(f"         Toutes les annonces recentes sont probablement deja en base.")
                print(f"         Pour continuer malgre tout : --start-page {page + 1}")
                break
        else:
            consecutive_empty = 0

    print(f"\n  Bilan {source_region} : {inserted_total} inserees, {skipped_total} ignorees")
    return inserted_total


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Scraping des adjudications Licitor.fr → PostgreSQL (foncier.licitor_brut)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Apres le scraping, lancer enrich_licitor_brut.py pour parser type_local / surfaces.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--region", choices=list(REGIONS.keys()),
                       help="Slug de la region a scraper")
    group.add_argument("--all", action="store_true",
                       help="Scraper toutes les regions")
    group.add_argument("--list-regions", action="store_true",
                       help="Afficher les regions disponibles")

    parser.add_argument("--with-details", action="store_true",
                        help="Scraper aussi les pages detail (descriptions completes ; plus lent)")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Nombre max de pages par region")
    parser.add_argument("--start-page", type=int, default=1,
                        help="Page de depart (defaut 1, utile pour reprendre)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scraper sans inserer en base")

    args = parser.parse_args()

    if args.list_regions:
        print("Regions disponibles :")
        for slug, label in REGIONS.items():
            print(f"  {slug:<35} {label}")
        return

    slugs = list(REGIONS.keys()) if args.all else [args.region]

    import psycopg2
    cfg = _get_db_config()
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False

    try:
        grand_total = 0
        for slug in slugs:
            grand_total += scrape_region(
                conn, slug,
                with_details=args.with_details,
                max_pages=args.max_pages,
                start_page=args.start_page,
                dry_run=args.dry_run,
            )
        print(f"\n{'=' * 60}")
        print(f"Total : {grand_total} nouvelles annonces inserees.")
        if grand_total > 0 and not args.dry_run:
            print("Conseil : lancer  python scripts/import/enrich_licitor_brut.py  "
                  "pour parser type_local / surfaces.")
    except KeyboardInterrupt:
        print("\n\nInterrompu par l'utilisateur. Les pages deja traitees sont commitees.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
