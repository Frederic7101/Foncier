# -*- coding: utf-8 -*-
"""Fixtures communes : serveur HTTP statique (frontend) + option backend :8000."""
from __future__ import annotations

import http.server
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"
BACKEND_GEO_URL = "http://127.0.0.1:8000/api/geo"
BACKEND_TIMEOUT_S = 3.0


def _make_handler_class(directory: Path):
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(directory), **kwargs)

        def log_message(self, format, *args):
            pass  # silence console pendant les tests

    return Handler


@pytest.fixture(scope="session")
def frontend_base_url() -> str:
    """Sert le dossier frontend/ sur un port libre (127.0.0.1)."""
    handler = _make_handler_class(FRONTEND_DIR)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    yield base
    server.shutdown()
    thread.join(timeout=2)


@pytest.fixture(scope="session")
def backend_geo_reachable() -> bool:
    """True si FastAPI répond sur /api/geo (port 8000), comme le front l’attend."""
    try:
        with urllib.request.urlopen(BACKEND_GEO_URL, timeout=BACKEND_TIMEOUT_S) as r:
            return r.status == 200
    except (urllib.error.URLError, OSError, TimeoutError):
        return False


@pytest.fixture
def require_backend(backend_geo_reachable):
    """À utiliser dans les tests qui ont besoin de données /api/geo ou suivantes."""
    if not backend_geo_reachable:
        pytest.skip(
            "Backend FastAPI introuvable sur http://127.0.0.1:8000 "
            "(lance uvicorn depuis webapp-foncier/backend avec le port 8000)."
        )


@pytest.fixture
def comparaison_page(page, frontend_base_url):
    """Ouvre comparaison_scores.html et attend le chargement initial."""
    url = f"{frontend_base_url}/comparaison_scores.html"
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_selector("h1", state="visible", timeout=15_000)
    return page
