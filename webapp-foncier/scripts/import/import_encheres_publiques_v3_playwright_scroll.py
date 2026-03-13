#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
import_encheres_publiques_v3_playwright_scroll.py

Orchestrateur pour lancer la v2 du script d'import
(`import_encheres_publiques_v2_playwright_scroll.py`) en parallèle
sur plusieurs types de biens pour une région donnée.

Nouvelle option :
    --type-biens all
        Lance un run par type de bien parmi :
        appartements, maisons, parkings, immeubles,
        locaux-commerciaux, terrains.

On ne modifie pas la v2 ; cette v3 ne fait qu'appeler la v2
en sous-processus, éventuellement en parallèle.
"""

import argparse
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional


TYPE_BIENS_ALL = [
    "appartements",
    "maisons",
    "parkings",
    "immeubles",
    "locaux-commerciaux",
    "terrains",
]


def _build_v2_command(
    type_bien: str,
    region: Optional[str],
    base_csv: str,
    args: argparse.Namespace,
) -> List[str]:
    """
    Construit la ligne de commande pour appeler la v2 avec un type de bien donné.
    On ajoute le type dans le nom de fichier CSV pour ne pas écraser les autres.
    """
    base, ext = os.path.splitext(base_csv)
    ext = ext or ".csv"
    csv_name = f"{base}.{region or 'all'}.{type_bien}{ext}"

    cmd: List[str] = [
        sys.executable,
        "import_encheres_publiques_v2_playwright_scroll.py",
        "--csv",
        csv_name,
        "--sources",
        args.sources,
        "--order",
        args.order,
    ]

    if region:
        cmd += ["--region", region]

    # Options de login si fournies
    if args.login_email:
        cmd += ["--login-email", args.login_email]
    if args.login_password_env:
        cmd += ["--login-password-env", args.login_password_env]
    if args.login_password:
        cmd += ["--login-password", args.login_password]

    # Headless / non-headless
    if args.headless:
        cmd += ["--headless"]
    else:
        cmd += ["--no-headless"]

    # Debug
    if args.debug:
        cmd += ["--debug"]

    # fail-if-not-logged-in
    if args.fail_if_not_logged_in:
        cmd += ["--fail-if-not-logged-in"]

    # max-results
    if args.max_results is not None:
        cmd += ["--max-results", str(args.max_results)]

    # Type de bien ciblé
    cmd += ["--type-bien", type_bien]

    return cmd


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Orchestrateur v3 : lance la v2 de l'import en parallèle "
            "sur plusieurs types de biens pour une région donnée."
        )
    )

    parser.add_argument(
        "--region",
        help="Slug de région (ex: ile-de-france). Obligatoire avec --type-biens all.",
    )
    parser.add_argument(
        "--type-biens",
        default="all",
        help=(
            "Liste de types de biens séparés par des virgules, "
            "ou 'all' pour tous: "
            "appartements, maisons, parkings, immeubles, "
            "locaux-commerciaux, terrains. (défaut: all)."
        ),
    )
    parser.add_argument(
        "--csv",
        default="encheres_publiques.csv",
        help=(
            "Préfixe du fichier CSV. Un fichier par type sera généré en ajoutant "
            "'.<region>.<type>.csv' (défaut: encheres_publiques.csv)."
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
        "--order",
        choices=["asc", "desc"],
        default="asc",
        help="Ordre chronologique (asc=plus anciennes d'abord, desc=plus récentes).",
    )

    # Options de login passées à la v2
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
            "Arrête chaque run de la v2 dès qu'une annonce de résultat nécessite d'être connecté "
            "pour voir le prix d'adjudication."
        ),
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="Nombre maximal d'annonces par type de bien.",
    )

    args = parser.parse_args()

    # Détermination de la liste des types à lancer
    if args.type_biens.strip().lower() == "all":
        if not args.region:
            raise SystemExit(
                "--region est obligatoire lorsque --type-biens=all."
            )
        type_list = TYPE_BIENS_ALL
    else:
        type_list = [t.strip() for t in args.type_biens.split(",") if t.strip()]
        if not type_list:
            raise SystemExit(
                "Aucun type de bien valide fourni dans --type-biens."
            )

    print(
        f"Lancement de la v2 pour {len(type_list)} type(s) de bien : "
        f"{', '.join(type_list)} (region={args.region or 'toutes'})",
        file=sys.stderr,
    )

    commands = [
        (t, _build_v2_command(t, args.region, args.csv, args))
        for t in type_list
    ]

    # Exécution en parallèle (un sous-processus par type de bien)
    results = {}
    with ThreadPoolExecutor(max_workers=len(commands)) as executor:
        futures = {
            executor.submit(
                subprocess.run,
                cmd,
                check=False,
                capture_output=False,
                text=True,
            ): type_bien
            for type_bien, cmd in commands
        }

        for fut in as_completed(futures):
            type_bien = futures[fut]
            try:
                proc = fut.result()
                results[type_bien] = proc.returncode
                print(
                    f"[{type_bien}] v2 terminée avec code {proc.returncode}",
                    file=sys.stderr,
                )
            except Exception as e:
                results[type_bien] = -1
                print(
                    f"[{type_bien}] Erreur lors de l'exécution de la v2: {e}",
                    file=sys.stderr,
                )

    # Bilan
    ok = [t for t, rc in results.items() if rc == 0]
    ko = [t for t, rc in results.items() if rc != 0]

    print(
        f"Types OK    : {', '.join(ok) if ok else 'aucun'}",
        file=sys.stderr,
    )
    if ko:
        print(
            f"Types en erreur : {', '.join(ko)}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()

