#!/usr/bin/env python3
"""
Scraping des résultats d'adjudications Licitor.fr → PostgreSQL (foncier.licitor_brut).

Scrape l'historique des adjudications pour une ou toutes les régions du site
Licitor.fr et insère les nouvelles annonces dans la table PostgreSQL
foncier.licitor_brut.  Les annonces déjà présentes en base sont ignorées
(détection par url_annonce + ON CONFLICT DO NOTHING en filet de sécurité).

Par défaut, la page détail de chaque *nouvelle* annonce est scrapée pour
récupérer la description longue, la date de vente exacte, l'adresse, la mise
à prix, le tribunal, le statut d'occupation, l'avocat et les dépendances.

Usage :
    python scrap_licitor.py --region paris-et-ile-de-france
    python scrap_licitor.py --all
    python scrap_licitor.py --list-regions
    python scrap_licitor.py --region sud-est-mediterrannee --max-pages 10
    python scrap_licitor.py --region paris-et-ile-de-france --no-details
    python scrap_licitor.py --backfill-details --max-rows 500

Options :
    --region SLUG       Scraper une seule région (voir --list-regions)
    --all               Scraper toutes les régions
    --list-regions      Lister les régions disponibles et quitter
    --no-details        Ne pas scraper les pages détail (plus rapide, moins de données)
    --max-pages N       Limiter le nombre de pages listing par région
    --start-page N      Page de départ (défaut 1, utile pour reprendre après crash)
    --dry-run           Scraper sans écrire en base
    --backfill-details  Compléter les annonces existantes sans description_longue
    --max-rows N        Nombre max de lignes à compléter en mode backfill (défaut 200)
"""

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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


_RE_DATE_LISTING = re.compile(r"(\d{2})[-/](\d{2})[-/](\d{4})")


def _clean_ws(text: Optional[str]) -> Optional[str]:
    """Normalise les espaces multiples (tabs/newlines internes) en un seul espace."""
    if not text:
        return text
    return re.sub(r"\s+", " ", text).strip()


def _parse_listing_date(text: str) -> Tuple[Optional[str], Optional[date_type]]:
    """Extrait DD-MM-YYYY du texte p.Result du listing → (texte, date)."""
    m = _RE_DATE_LISTING.search(text or "")
    if not m:
        return None, None
    dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        d = date_type(yyyy, mm, dd)
        return m.group(0), d
    except ValueError:
        return m.group(0), None


# ─────────────────────────────────────────────────────────────────────────────
# URLs historique
# ─────────────────────────────────────────────────────────────────────────────
def _historique_url(region_slug: str, page: int = 1) -> str:
    url = f"{BASE_URL}/ventes-aux-encheres-immobilieres/{region_slug}/historique-des-adjudications.html"
    if page > 1:
        url += f"?p={page}"
    return url


# ─────────────────────────────────────────────────────────────────────────────
# Extraction des données depuis la page listing
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
    """Extrait les annonces depuis le HTML d'une page listing.

    Structure HTML de chaque annonce :
        <a class="Ad Archives" href="/annonce/...">
            <p class="Location"><span class="Number">75</span><span class="City">Paris 13ème</span></p>
            <p class="Description"><span class="Name">Un appartement</span><span class="Text">au 24ème étage …[...]</span></p>
            <div class="Footer"><div class="Price"><p class="Result">09-04-2026 : <span class="PriceNumber">100 001 €</span></p></div></div>
        </a>
    """
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

            # Date de vente : dans <p class="Result">DD-MM-YYYY : <span class="PriceNumber">…</span></p>
            date_vente_texte = None
            date_vente = None
            result_el = a_tag.select_one("p.Result")
            if result_el:
                date_vente_texte, date_vente = _parse_listing_date(result_el.get_text())

            results.append({
                "url_annonce": url,
                "code_dept": code_dept,
                "commune": commune,
                "desc_courte": desc,
                "montant_adjudication": montant,
                "date_vente_texte": date_vente_texte,
                "date_vente": date_vente,
            })
        except Exception as exc:
            print(f"  [!] Erreur parsing annonce : {exc}")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Scraping de la page détail
# ─────────────────────────────────────────────────────────────────────────────
def _compose_description_longue(article) -> str:
    """Compose le texte de description longue depuis les éléments HTML structurés."""
    parts: List[str] = []

    # Tribunal
    court = article.select_one("p.Court")
    if court:
        parts.append(_clean_ws(court.get_text(strip=True)))

    # Type de vente
    type_el = article.select_one("p.Type")
    if type_el:
        parts.append(_clean_ws(type_el.get_text(strip=True)))

    # Date et heure
    date_el = article.select_one("p.Date")
    if date_el:
        parts.append(date_el.get_text(strip=True))

    # Description des lots (SousLot inclut FirstSousLot car il a aussi la classe SousLot)
    for lot in article.select(".SousLot"):
        h2 = lot.find("h2")
        p = lot.find("p")
        h2_t = h2.get_text(strip=True) if h2 else ""
        p_t = p.get_text(separator="\n", strip=True) if p else ""
        parts.append(f"{h2_t}\n{p_t}" if p_t else h2_t)

    # Adjudication + Mise à prix
    adj = article.select_one("div.Lot > h3")
    if adj:
        parts.append(adj.get_text(strip=True))
    mep = article.select_one("div.Lot > h4")
    if mep:
        parts.append(mep.get_text(strip=True))

    # Localisation
    city = article.select_one("div.Location p.City")
    street = article.select_one("div.Location p.Street")
    loc = []
    if city:
        loc.append(city.get_text(strip=True))
    if street:
        loc.append(street.get_text(separator="\n", strip=True))
    if loc:
        parts.append("\n".join(loc))

    # Avocats
    for trust in article.select("div.Trust"):
        h3 = trust.find("h3")
        p_tags = trust.find_all("p")
        h3_t = h3.get_text(strip=True) if h3 else ""
        p_texts = []
        for p in p_tags:
            t = p.get_text(separator="\n", strip=True)
            # Exclure les liens (« Pour plus de détails : www... »)
            if t and not t.startswith("www."):
                p_texts.append(t)
        trust_text = "\n".join([h3_t] + p_texts) if p_texts else h3_t
        if trust_text:
            parts.append(trust_text)

    return "\n\n".join(parts)


def _scrape_detail(url: str) -> Optional[dict]:
    """Scrape la page détail d'une annonce.

    Structure HTML :
        <article class="LegalAd">
          <p class="Court">Tribunal …</p>
          <p class="Date"><time datetime="2026-04-07T14:00:00">mardi 7 avril 2026 à 14h</time></p>
          <div class="Lot">
            <div class="FirstSousLot SousLot"><h2>…</h2><p>…</p></div>
            <div class="SousLot"><h2>Une cave</h2><p>…</p></div>
            <h3>Adjudication : 138 000 €</h3>
            <h4>(Mise à prix : 30 000 €)</h4>
          </div>
          <div class="Location"><p class="City">…</p><p class="Street">…</p></div>
          <div class="Trusts"><div class="Trust"><h3>Maître …</h3><p>…<br/>Tél.: …</p></div></div>
        </article>
    """
    try:
        resp = _safe_request(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        article = soup.find("article", class_="LegalAd")
        if not article:
            return None

        result: dict = {}

        # ── Description longue ──────────────────────────────────────────
        result["description_longue"] = _compose_description_longue(article)

        # ── Date de vente (ISO depuis l'attribut datetime) ──────────────
        time_el = article.select_one("p.Date time")
        if time_el and time_el.get("datetime"):
            try:
                dt_str = time_el["datetime"]  # ex. "2026-04-07T14:00:00"
                result["date_vente"] = datetime.fromisoformat(dt_str).date()
                result["date_vente_texte"] = time_el.get_text(strip=True)
            except (ValueError, TypeError):
                pass

        # ── desc_courte complète (1er lot, non tronquée) ────────────────
        first_lot = article.select_one(".FirstSousLot")
        if first_lot:
            h2 = first_lot.find("h2")
            p = first_lot.find("p")
            h2_t = h2.get_text(strip=True) if h2 else ""
            p_t = p.get_text(separator=" ", strip=True) if p else ""
            result["desc_courte"] = f"{h2_t} {p_t}".strip() if p_t else h2_t

        # ── Tribunal ────────────────────────────────────────────────────
        court = article.select_one("p.Court")
        if court:
            result["tribunal"] = _clean_ws(court.get_text(strip=True))

        # ── Montant adjudication (plus précis que le listing) ──────────
        adj = article.select_one("div.Lot > h3")
        if adj:
            adj_text = adj.get_text(strip=True).replace("Adjudication", "").replace(":", "").strip()
            result["montant_adjudication"] = _parse_montant(adj_text)

        # ── Mise à prix ────────────────────────────────────────────────
        mep = article.select_one("div.Lot > h4")
        if mep:
            mep_text = mep.get_text(strip=True)
            for tok in ("Mise à prix", "Mise a prix", "(", ")", ":"):
                mep_text = mep_text.replace(tok, "")
            result["mise_a_prix"] = _parse_montant(mep_text)

        # ── Adresse ─────────────────────────────────────────────────────
        street = article.select_one("div.Location p.Street")
        if street:
            result["adresse"] = _clean_ws(street.get_text(separator=", ", strip=True))

        # ── Statut occupation ───────────────────────────────────────────
        full_text = article.get_text(separator="\n")
        if re.search(r"\boccup[eé]", full_text, re.I):
            result["statut_occupation"] = "occupé"
        elif re.search(r"\b(?:libre|inoccu|vacant)", full_text, re.I):
            result["statut_occupation"] = "libre"

        # ── Avocat (premier trust) ──────────────────────────────────────
        trusts = article.select("div.Trust")
        if trusts:
            h3 = trusts[0].find("h3")
            if h3:
                result["avocat_nom"] = _clean_ws(h3.get_text(strip=True))
            p_first = trusts[0].find("p")
            if p_first:
                p_text = p_first.get_text(separator="\n", strip=True)
                lines = [ln.strip() for ln in p_text.split("\n") if ln.strip()]
                for ln in lines:
                    tel_m = re.search(r"T[eé]l\.?\s*:?\s*([\d\s.]+)", ln)
                    if tel_m:
                        result["avocat_tel"] = tel_m.group(1).strip()
                        break
                addr_lines = []
                for ln in lines:
                    if re.search(r"T[eé]l", ln):
                        break
                    if not re.match(r"^(?:Pour plus|www\.)", ln):
                        addr_lines.append(ln)
                if addr_lines:
                    result["avocat_adresse"] = ", ".join(addr_lines)

        # ── Dépendances (lots après le premier = dépendances) ──────────
        sous_lots = article.select(".SousLot")
        dep_titles = " | ".join(
            lot.find("h2").get_text(strip=True)
            for lot in sous_lots[1:]
            if lot.find("h2")
        ) if len(sous_lots) > 1 else ""
        result["has_cave"] = bool(re.search(r"\bcave\b", dep_titles, re.I))
        result["has_parking_dep"] = bool(re.search(r"\b(?:parking|stationnement)\b", dep_titles, re.I))
        result["has_garage"] = bool(re.search(r"\bgarage\b", dep_titles, re.I))
        # Jardin, balcon, terrasse : chercher dans l'ensemble du texte
        result["has_jardin"] = bool(re.search(r"\bjardin\b", full_text, re.I))
        result["has_balcon"] = bool(re.search(r"\bbalcon\b", full_text, re.I))
        result["has_terrasse"] = bool(re.search(r"\bterrasse\b", full_text, re.I))

        return result

    except Exception as exc:
        print(f"  [!] Erreur detail {url} : {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Insertion & mise à jour PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
_INSERT_COLS = (
    "source_region", "url_annonce", "code_dept", "commune",
    "desc_courte", "montant_adjudication", "date_vente_texte", "date_scraping",
    "date_vente", "description_longue", "adresse", "mise_a_prix",
    "tribunal", "statut_occupation",
    "avocat_nom", "avocat_tel", "avocat_adresse",
    "has_cave", "has_parking_dep", "has_jardin",
    "has_balcon", "has_terrasse", "has_garage",
)

_INSERT_SQL = (
    f"INSERT INTO foncier.licitor_brut ({', '.join(_INSERT_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_INSERT_COLS))}) "
    "ON CONFLICT (url_annonce, desc_courte, montant_adjudication) DO NOTHING"
)

_UPDATE_DETAIL_SQL = """
    UPDATE foncier.licitor_brut SET
        date_vente          = %(date_vente)s,
        date_vente_texte    = COALESCE(%(date_vente_texte)s, date_vente_texte),
        description_longue  = %(description_longue)s,
        adresse             = %(adresse)s,
        mise_a_prix         = %(mise_a_prix)s,
        tribunal            = %(tribunal)s,
        statut_occupation   = %(statut_occupation)s,
        avocat_nom          = %(avocat_nom)s,
        avocat_tel          = %(avocat_tel)s,
        avocat_adresse      = %(avocat_adresse)s,
        has_cave            = %(has_cave)s,
        has_parking_dep     = %(has_parking_dep)s,
        has_jardin          = %(has_jardin)s,
        has_balcon          = %(has_balcon)s,
        has_terrasse        = %(has_terrasse)s,
        has_garage          = %(has_garage)s
    WHERE id = %(id)s
"""


def _load_existing_urls(conn) -> Set[str]:
    """Charge l'ensemble des url_annonce déjà présentes en base."""
    with conn.cursor() as cur:
        cur.execute("SELECT url_annonce FROM foncier.licitor_brut")
        return {r[0] for r in cur.fetchall()}


def _safe_integer(value: Any, max_value: int = 9223372036854775807) -> Optional[int]:
    """Convertit une valeur en entier sécurisé pour BIGINT PostgreSQL.

    - Accepte int, float, string
    - Retourne None si conversion échoue ou valeur invalide
    - Vérifie que la valeur est dans les limites BIGINT
    """
    if value is None:
        return None

    try:
        # Convertir en float d'abord pour gérer les décimales (ex: "123.45" → 123)
        if isinstance(value, str):
            # Nettoyer les espaces
            value = value.strip()
            if not value:
                return None
            # Convertir en float
            num = float(value)
        else:
            num = float(value)

        # Arrondir à l'entier le plus proche
        result = int(round(num))

        # Vérifier les limites BIGINT
        if result > max_value or result < -max_value:
            return None

        return result
    except (ValueError, TypeError, OverflowError):
        return None


def _build_insert_row(source_region: str, listing: dict, detail: Optional[dict], now: datetime) -> tuple:
    """Construit le tuple INSERT à partir des données listing + éventuel détail."""
    d = detail or {}
    return (
        source_region,
        listing["url_annonce"],
        listing["code_dept"],
        listing["commune"],
        # desc_courte : préférer la version complète du détail si disponible
        d.get("desc_courte") or listing["desc_courte"],
        # montant : préférer le détail (plus précis), convertir en entier sûr
        _safe_integer(d.get("montant_adjudication") if d.get("montant_adjudication") is not None else listing["montant_adjudication"]),
        # date_vente_texte : préférer le détail (texte complet)
        d.get("date_vente_texte") or listing.get("date_vente_texte"),
        now,
        # date_vente : préférer le détail (ISO), sinon listing (DD-MM-YYYY)
        d.get("date_vente") or listing.get("date_vente"),
        d.get("description_longue"),
        d.get("adresse"),
        _safe_integer(d.get("mise_a_prix")),
        d.get("tribunal"),
        d.get("statut_occupation"),
        d.get("avocat_nom"),
        d.get("avocat_tel"),
        d.get("avocat_adresse"),
        d.get("has_cave"),
        d.get("has_parking_dep"),
        d.get("has_jardin"),
        d.get("has_balcon"),
        d.get("has_terrasse"),
        d.get("has_garage"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Boucle principale de scraping d'une région
# ─────────────────────────────────────────────────────────────────────────────
def scrape_region(conn, region_slug: str, *,
                  no_details: bool = False,
                  max_pages: Optional[int] = None,
                  start_page: int = 1,
                  dry_run: bool = False) -> int:
    source_region = REGIONS[region_slug]
    print(f"\n{'=' * 60}")
    print(f"Region : {source_region}  ({region_slug})")
    print(f"Mode   : {'listing seul' if no_details else 'listing + detail'}")
    print(f"{'=' * 60}")

    # Page de départ : récupérer pagination ET premières annonces en une seule requête
    resp = _safe_request(_historique_url(region_slug, start_page))
    soup = BeautifulSoup(resp.text, "html.parser")
    total, nb_pages = _extract_pagination(soup)
    if total == 0:
        print("  Aucune annonce trouvee.")
        return 0

    last_page = min(nb_pages, start_page + max_pages - 1) if max_pages else nb_pages
    print(f"  {total} annonces au total, pages {start_page}..{last_page} a parcourir")

    existing_urls = _load_existing_urls(conn)
    print(f"  {len(existing_urls)} annonces deja en base (toutes regions)")

    from psycopg2.extras import execute_batch

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

        rows_to_insert: list = []
        for a in annonces:
            if a["url_annonce"] in existing_urls:
                skipped_total += 1
                continue

            detail = None
            if not no_details:
                detail = _scrape_detail(a["url_annonce"])

            rows_to_insert.append(_build_insert_row(source_region, a, detail, now))
            existing_urls.add(a["url_annonce"])

        # Insertion par lot (par page) → commit immédiat pour résilience
        if rows_to_insert and not dry_run:
            with conn.cursor() as cur:
                execute_batch(cur, _INSERT_SQL, rows_to_insert, page_size=200)
            conn.commit()

        inserted_total += len(rows_to_insert)
        nb_exist = len(annonces) - len(rows_to_insert)
        marker = " (dry-run)" if dry_run else ""
        print(f"  Page {page:>5}/{last_page}  |  {len(annonces)} ann.  "
              f"{len(rows_to_insert)} nouvelles  {nb_exist} existantes{marker}")

        # Indicateur : si plusieurs pages consécutives n'apportent rien de nouveau
        if len(rows_to_insert) == 0:
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
# Mode backfill : compléter les lignes existantes sans description_longue
# ─────────────────────────────────────────────────────────────────────────────
def backfill_details(conn, *, max_rows: int = 200, dry_run: bool = False) -> int:
    """Récupère la page détail pour les annonces existantes sans description_longue.

    Le matching est sûr : on lit (id, url_annonce) depuis la base, on scrape
    l'url_annonce correspondante, et on met à jour la ligne par son id (PK).
    """
    print(f"\n{'=' * 60}")
    print(f"Backfill details  (max {max_rows} lignes)")
    print(f"{'=' * 60}")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, url_annonce FROM foncier.licitor_brut "
            "WHERE description_longue IS NULL "
            "ORDER BY id DESC LIMIT %s",
            (max_rows,),
        )
        rows = cur.fetchall()

    if not rows:
        print("  Aucune ligne a completer.")
        return 0

    print(f"  {len(rows)} lignes a completer")

    updated = 0
    errors = 0
    for i, (row_id, url) in enumerate(rows, 1):
        detail = _scrape_detail(url)
        if not detail or not detail.get("description_longue"):
            errors += 1
            print(f"  [{i}/{len(rows)}] id={row_id}  SKIP (page indisponible)")
            continue

        if not dry_run:
            params = {
                "id": row_id,
                "date_vente": detail.get("date_vente"),
                "date_vente_texte": detail.get("date_vente_texte"),
                "description_longue": detail.get("description_longue"),
                "desc_courte": detail.get("desc_courte"),
                "montant_adjudication": _safe_integer(detail.get("montant_adjudication")),
                "adresse": detail.get("adresse"),
                "mise_a_prix": _safe_integer(detail.get("mise_a_prix")),
                "tribunal": detail.get("tribunal"),
                "statut_occupation": detail.get("statut_occupation"),
                "avocat_nom": detail.get("avocat_nom"),
                "avocat_tel": detail.get("avocat_tel"),
                "avocat_adresse": detail.get("avocat_adresse"),
                "has_cave": detail.get("has_cave"),
                "has_parking_dep": detail.get("has_parking_dep"),
                "has_jardin": detail.get("has_jardin"),
                "has_balcon": detail.get("has_balcon"),
                "has_terrasse": detail.get("has_terrasse"),
                "has_garage": detail.get("has_garage"),
            }
            with conn.cursor() as cur:
                cur.execute(_UPDATE_DETAIL_SQL, params)
            conn.commit()

        updated += 1
        marker = " (dry-run)" if dry_run else ""
        if i % 10 == 0 or i == len(rows):
            print(f"  [{i}/{len(rows)}]  {updated} mises a jour, {errors} erreurs{marker}")

    print(f"\n  Bilan backfill : {updated} mises a jour, {errors} erreurs")
    return updated


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Scraping des adjudications Licitor.fr -> PostgreSQL (foncier.licitor_brut)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Apres le scraping, lancer enrich_licitor_brut.py pour parser type_local / surfaces.\n"
               "Puis enrich_licitor_detail.py pour re-parser description_longue si besoin.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--region", choices=list(REGIONS.keys()),
                       help="Slug de la region a scraper")
    group.add_argument("--all", action="store_true",
                       help="Scraper toutes les regions")
    group.add_argument("--list-regions", action="store_true",
                       help="Afficher les regions disponibles")
    group.add_argument("--backfill-details", action="store_true",
                       help="Completer les lignes existantes sans description_longue")

    parser.add_argument("--no-details", action="store_true",
                        help="Ne pas scraper les pages detail (plus rapide)")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Nombre max de pages par region")
    parser.add_argument("--start-page", type=int, default=1,
                        help="Page de depart (defaut 1, utile pour reprendre)")
    parser.add_argument("--max-rows", type=int, default=200,
                        help="Nombre max de lignes en mode backfill (defaut 200)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scraper sans ecrire en base")

    args = parser.parse_args()

    if args.list_regions:
        print("Regions disponibles :")
        for slug, label in REGIONS.items():
            print(f"  {slug:<35} {label}")
        return

    import psycopg2
    cfg = _get_db_config()
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False

    try:
        if args.backfill_details:
            backfill_details(conn, max_rows=args.max_rows, dry_run=args.dry_run)
        else:
            slugs = list(REGIONS.keys()) if args.all else [args.region]
            grand_total = 0
            for slug in slugs:
                grand_total += scrape_region(
                    conn, slug,
                    no_details=args.no_details,
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
