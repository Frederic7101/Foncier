"""
Exemple minimal de création d'issue JIRA via l'API REST.

Pré-requis :
- Installer la bibliothèque requests :  pip install requests
- Définir les variables d'environnement :
  - JIRA_URL, JIRA_EMAIL (ou JIRA_USER), JIRA_TOKEN
"""

import json
import os
from typing import Optional

import requests


def create_issue(
    project_key: str,
    summary: str,
    description: str,
    issue_type: str = "Story",
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
            "issuetype": {"name": issue_type},
        }
    }

    headers = {"Content-Type": "application/json"}
    resp = requests.post(api_url, auth=auth, headers=headers, data=json.dumps(payload), timeout=30)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Créer une issue JIRA depuis la ligne de commande.")
    parser.add_argument("project_key", help="Clé du projet JIRA (ex: DVF)")
    parser.add_argument("summary", help="Résumé de l'issue")
    parser.add_argument("description", help="Description de l'issue")
    args = parser.parse_args()

    created = create_issue(args.project_key, args.summary, args.description)
    print(f"Issue créée: {created.get('key')}")

