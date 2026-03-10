"""
Exemple minimal de création/mise à jour de page Confluence via l'API REST.

Pré-requis :
- Installer la bibliothèque requests :  pip install requests
- Définir les variables d'environnement :
  - CONFLUENCE_URL, CONFLUENCE_EMAIL (ou CONFLUENCE_USER), CONFLUENCE_TOKEN
"""

import base64
import json
import os
from typing import Optional

import requests


def _auth_header() -> dict:
    user = os.getenv("CONFLUENCE_EMAIL") or os.getenv("CONFLUENCE_USER")
    token = os.getenv("CONFLUENCE_TOKEN")
    if not user or not token:
        raise RuntimeError("CONFLUENCE_EMAIL/CONFLUENCE_USER ou CONFLUENCE_TOKEN non définis")
    raw = f"{user}:{token}".encode("utf-8")
    return {"Authorization": "Basic " + base64.b64encode(raw).decode("ascii")}


def create_or_update_page(
    space_key: str,
    title: str,
    body_markdown: str,
    confluence_url: Optional[str] = None,
) -> dict:
    confluence_url = confluence_url or os.getenv("CONFLUENCE_URL")
    if not confluence_url:
        raise RuntimeError("CONFLUENCE_URL non défini")

    api_base = confluence_url.rstrip("/") + "/wiki/rest/api"

    headers = {
        "Content-Type": "application/json",
        **_auth_header(),
    }

    # Rechercher une page existante portant ce titre
    search_url = api_base + "/content"
    params = {"spaceKey": space_key, "title": title}
    resp = requests.get(search_url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])

    storage_value = body_markdown

    if results:
        page = results[0]
        page_id = page["id"]
        version = page["version"]["number"] + 1
        update_url = api_base + f"/content/{page_id}"
        payload = {
            "id": page_id,
            "type": "page",
            "title": title,
            "space": {"key": space_key},
            "body": {
                "storage": {
                    "value": storage_value,
                    "representation": "wiki",
                }
            },
            "version": {"number": version},
        }
        resp = requests.put(update_url, headers=headers, data=json.dumps(payload), timeout=30)
        resp.raise_for_status()
        return resp.json()

    create_url = api_base + "/content"
    payload = {
        "type": "page",
        "title": title,
        "space": {"key": space_key},
        "body": {
            "storage": {
                "value": storage_value,
                "representation": "wiki",
            }
        },
    }
    resp = requests.post(create_url, headers=headers, data=json.dumps(payload), timeout=30)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Créer ou mettre à jour une page Confluence.")
    parser.add_argument("space_key", help="Clé d'espace Confluence")
    parser.add_argument("title", help="Titre de la page")
    parser.add_argument("fichier_markdown", help="Fichier Markdown à publier")
    args = parser.parse_args()

    contenu = Path(args.fichier_markdown).read_text(encoding="utf-8")
    page = create_or_update_page(args.space_key, args.title, contenu)
    print(f"Page Confluence publiée: {page.get('_links', {}).get('webui')}")

