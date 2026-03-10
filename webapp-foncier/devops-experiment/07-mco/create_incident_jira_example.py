"""
Exemple minimal de création de ticket d'incident pour la MCO via l'API JIRA.

Pré-requis :
- Installer la bibliothèque requests :  pip install requests
- Définir les variables d'environnement JIRA_URL, JIRA_EMAIL/JIRA_USER, JIRA_TOKEN.
"""

import json
import os
from typing import Optional

import requests


def create_incident(
    project_key: str,
    summary: str,
    description: str,
    priority: str = "High",
    jira_url: Optional[str] = None,
) -> dict:
    jira_url = jira_url or os.getenv("JIRA_URL")
    if not jira_url:
        raise RuntimeError("JIRA_URL non défini")

    user = os.getenv("JIRA_EMAIL") or os.getenv("JIRA_USER")
    token = os.getenv("JIRA_TOKEN")
    if not user or not token:
        raise RuntimeError("JIRA_EMAIL/JIRA_USER ou JIRA_TOKEN non définis")

    api_url = jira_url.rstrip("/") + "/rest/api/3/issue"
    auth = (user, token)

    payload = {
        "fields": {
            "project": {"key": project_key},
            "summary": summary,
            "description": description,
            "issuetype": {"name": "Incident"},
            "priority": {"name": priority},
        }
    }

    headers = {"Content-Type": "application/json"}
    resp = requests.post(api_url, auth=auth, headers=headers, data=json.dumps(payload), timeout=30)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Créer un ticket d'incident JIRA.")
    parser.add_argument("project_key", help="Clé du projet JIRA")
    parser.add_argument("summary", help="Résumé de l'incident")
    parser.add_argument("description", help="Description détaillée")
    args = parser.parse_args()

    created = create_incident(args.project_key, args.summary, args.description)
    print(f"Incident créé: {created.get('key')}")

