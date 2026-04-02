#!/usr/bin/env python3
"""
Verifie pour chaque commune du fichier liste_communes.csv :
  1. Si la commune a ete fusionnee / rattachee (intro Wikipedia)
  2. Le code commune mentionne dans l'infobox Wikipedia (<th scope="row">)

Strategie API (3 appels max par commune) :
  1. search : "commune departement commune" -> titre exact
  2. query extracts : intro plain-text (detection fusion)
  3. parse section 0 : HTML infobox (code commune)

Produit : liste_communes_enrichie.csv
"""

import csv
import re
import time
import sys
import os
import urllib.request
import urllib.parse
import json
from html.parser import HTMLParser
from typing import Optional

# -- Config ----------------------------------------------------------------
WIKI_API = "https://fr.wikipedia.org/w/api.php"
INPUT_CSV  = os.path.join(os.path.dirname(os.path.dirname(__file__)), "liste_communes.csv")
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.dirname(__file__)), "liste_communes_enrichie.csv")
DELAY = 0.5       # secondes entre chaque requete
MAX_RETRIES = 3   # retries sur 429

# Mots-cles de fusion/rattachement dans l'intro Wikipedia
FUSION_KEYWORDS = [
    "commune déléguée",
    "commune nouvelle",
    "commune associée",
    "ancienne commune",
    "fusionn",
    "rattach",
    "a été intégrée",
    "a été absorbée",
    "regroupement de communes",
    "fusion de",
    "fusion des communes",
    "issue de la fusion",
    "village détruit",
    "commune morte",
    "territoire rattaché",
]


def wiki_get(params: dict) -> dict:
    """Appel GET vers l'API Wikipedia fr avec retry sur 429."""
    params.setdefault("format", "json")
    params.setdefault("origin", "*")
    url = WIKI_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "webapp-foncier/1.0 (commune-check; contact@example.com)"
    })
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 5 * (attempt + 1)
                print(f"\n  [429 rate limit, attente {wait}s]", end=" ", flush=True)
                time.sleep(wait)
                continue
            print(f"\n  [HTTP {e.code}]", end=" ", file=sys.stderr)
            return {}
        except Exception as e:
            print(f"\n  [ERREUR] {e}", end=" ", file=sys.stderr)
            return {}
    return {}


def _is_commune_snippet(snippet: str) -> bool:
    """Le snippet ressemble-t-il a un article de commune ?"""
    s = (snippet or "").lower()
    return any(k in s for k in [
        "commune", "département", "departement", "région", "region",
        "canton", "habitants", "code postal", "intercommunal"
    ])


def resolve_title(commune: str, dep_nom: str) -> Optional[str]:
    """Resout le titre Wikipedia via UNE recherche."""
    q = commune.strip()
    if not q:
        return None

    # Recherche : "nom_commune commune nom_departement"
    search_q = f"{q} commune {dep_nom}" if dep_nom else f"{q} commune France"
    data = wiki_get({
        "action": "query",
        "list": "search",
        "srsearch": search_q,
        "srlimit": "5",
    })
    results = (data.get("query") or {}).get("search") or []
    if not results:
        return None

    # Priorite : titre contenant le nom de la commune ET snippet commune
    commune_lower = q.lower().replace("-", " ").replace("'", " ")
    commune_words = set(w for w in commune_lower.split() if len(w) > 2)

    best = None
    for r in results:
        title = r.get("title", "")
        snippet = r.get("snippet", "")
        title_lower = title.lower().replace("-", " ").replace("'", " ")
        title_words = set(title_lower.split())

        # Le titre partage des mots significatifs avec le nom de la commune
        overlap = commune_words & title_words
        is_commune = _is_commune_snippet(snippet)

        if overlap and is_commune:
            return title
        if overlap and best is None:
            best = title
        if is_commune and best is None:
            best = title

    return best or results[0].get("title")


def get_extract(title: str) -> Optional[str]:
    """Recupere l'intro plain-text (suit les redirects)."""
    data = wiki_get({
        "action": "query",
        "titles": title,
        "redirects": "1",
        "prop": "extracts",
        "exintro": "true",
        "explaintext": "true",
        "exsentences": "6",
    })
    pages = (data.get("query") or {}).get("pages") or {}
    for pid, page in pages.items():
        if page and not page.get("missing") and page.get("extract"):
            return page["extract"].strip()
    return None


def detect_fusion(extract: str) -> str:
    """Detecte fusion/rattachement dans l'intro."""
    if not extract:
        return ""
    lower = extract.lower()
    found = [kw for kw in FUSION_KEYWORDS if kw.lower() in lower]
    if not found:
        return ""

    sentences = re.split(r'(?<=[.!?])\s+', extract)
    for s in sentences:
        sl = s.lower()
        if any(kw.lower() in sl for kw in found):
            txt = s.strip()
            return txt[:250] if len(txt) <= 250 else txt[:247] + "..."
    return "OUI (" + ", ".join(found[:3]) + ")"


# -- Parser HTML pour code commune infobox ---------------------------------

class InfoboxCodeParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._in_th = False
        self._found_code_th = False
        self._in_td = False
        self._code_value = None
        self._current_text = ""

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        if tag == "th" and d.get("scope") == "row":
            self._in_th = True
            self._current_text = ""
        elif tag == "td" and self._found_code_th:
            self._in_td = True
            self._current_text = ""

    def handle_endtag(self, tag):
        if tag == "th" and self._in_th:
            self._in_th = False
            txt = self._current_text.strip().lower()
            if "code commune" in txt or "code insee" in txt:
                self._found_code_th = True
        elif tag == "td" and self._in_td:
            self._in_td = False
            if self._found_code_th and self._code_value is None:
                val = re.sub(r'\[.*?\]', '', self._current_text).strip()
                if val:
                    self._code_value = val
                self._found_code_th = False

    def handle_data(self, data):
        if self._in_th or self._in_td:
            self._current_text += data


def get_code_commune(title: str) -> Optional[str]:
    """Extrait le code commune de l'infobox (suit les redirects)."""
    data = wiki_get({
        "action": "parse",
        "page": title,
        "redirects": "true",
        "prop": "text",
        "section": "0",
    })
    html = ((data.get("parse") or {}).get("text") or {}).get("*", "")
    if not html:
        return None
    parser = InfoboxCodeParser()
    try:
        parser.feed(html)
    except Exception:
        pass
    return parser._code_value


def normalize_code(code: str) -> str:
    """Normalise un code commune : extrait le code actuel, LPAD 5.
    Si multi-codes avec dates, prend celui associe a 'depuis' (= code actuel)."""
    if not code:
        return ""
    # Cas multi-codes : "69114 (depuis le 15 mars 2024)69159 (-14 mars 2024)"
    # Chercher le code suivi de "(depuis"
    m = re.search(r'(2[AB]\d{3}|\d{4,5})\s*\(depuis', code)
    if m:
        raw = m.group(1)
        if raw.startswith("2A") or raw.startswith("2B"):
            return raw
        return raw.zfill(5)
    # Cas multi-codes avec "date-)" = ancien code, et "date date-" = ancien aussi
    # Chercher les codes et prendre le premier (souvent le code actuel en haut)
    parts = re.findall(r'(2[AB]\d{3}|\d{4,5})', code)
    if not parts:
        return code.strip()
    raw = parts[0]
    if raw.startswith("2A") or raw.startswith("2B"):
        return raw
    return raw.zfill(5)


# -- Main ------------------------------------------------------------------

def main():
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    print(f"Traitement de {total} communes...\n")

    results = []
    for i, row in enumerate(rows, 1):
        commune = row["commune"]
        code_insee = row["code_insee"]
        dep_nom = row["nom_dept"]
        code_insee_norm = code_insee if code_insee.startswith("2A") or code_insee.startswith("2B") else code_insee.zfill(5)

        print(f"[{i}/{total}] {commune} ({code_insee}) ...", end=" ", flush=True)

        # 1. Titre Wikipedia (1 appel)
        title = resolve_title(commune, dep_nom)
        time.sleep(DELAY)

        if not title:
            print("ERREUR titre")
            results.append({**row, "wiki_titre": "", "fusion_rattachement": "ERREUR: titre non trouve", "code_commune_wiki": ""})
            continue

        print(f"-> {title}", end=" | ", flush=True)

        # 2. Intro / fusion (1 appel)
        extract = get_extract(title)
        time.sleep(DELAY)
        fusion_info = detect_fusion(extract)

        # 3. Code infobox (1 appel)
        code_wiki_raw = get_code_commune(title)
        time.sleep(DELAY)

        code_wiki_norm = normalize_code(code_wiki_raw or "")

        # Affichage
        parts = []
        if fusion_info:
            parts.append("FUSION")
        if code_wiki_raw:
            if code_wiki_norm == code_insee_norm:
                parts.append(f"code:{code_wiki_raw} OK")
            else:
                parts.append(f"code:{code_wiki_raw} != {code_insee}")
        else:
            parts.append("code:?")
        print(" ".join(parts))

        results.append({
            **row,
            "wiki_titre": title,
            "fusion_rattachement": fusion_info,
            "code_commune_wiki": code_wiki_raw or "",
        })

    # Ecriture CSV
    fieldnames = ["code_dept", "nom_dept", "commune", "code_insee",
                  "wiki_titre", "fusion_rattachement", "code_commune_wiki"]
    with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n{'='*60}")
    print(f"Resultat : {OUTPUT_CSV}")

    nb_fusion = sum(1 for r in results if r["fusion_rattachement"] and not r["fusion_rattachement"].startswith("ERREUR"))
    nb_ok = sum(1 for r in results if r["code_commune_wiki"]
                and normalize_code(r["code_commune_wiki"]) ==
                    (r["code_insee"] if r["code_insee"].startswith("2A") or r["code_insee"].startswith("2B") else r["code_insee"].zfill(5)))
    nb_diff = sum(1 for r in results if r["code_commune_wiki"]
                  and normalize_code(r["code_commune_wiki"]) !=
                      (r["code_insee"] if r["code_insee"].startswith("2A") or r["code_insee"].startswith("2B") else r["code_insee"].zfill(5)))
    nb_miss = sum(1 for r in results if not r["code_commune_wiki"])

    print(f"  Communes         : {total}")
    print(f"  Fusions          : {nb_fusion}")
    print(f"  Code concordant  : {nb_ok}")
    print(f"  Code different   : {nb_diff}")
    print(f"  Code absent      : {nb_miss}")


if __name__ == "__main__":
    main()
