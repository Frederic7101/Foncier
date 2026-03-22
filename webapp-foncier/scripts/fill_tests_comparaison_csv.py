# -*- coding: utf-8 -*-
"""Remplit colonnes G-K (indices 6-10) du fichier Tests comparaison scores.csv."""
from pathlib import Path

DATE_TEST = "15/03/2026"

RESULTS = {
    "1.1": (
        "Oui",
        "Ouvrir comparaison_scores.html (le pas de test cite comparaison_html.html : coquille). Prérequis : backend démarré.",
        "OK",
        "",
    ),
    "2.1": (
        "Oui",
        "<title> onglet : « Comparaison des villes » ; <h1> : « Comparaison des communes » (centré). Écart sur le titre document.",
        "KO",
        "ACC001",
    ),
    "2.2": (
        "Oui",
        "Liens Fiche commune, Stats des ventes, Ventes ; href corrects ; nav centrée ; fond bleu #2563eb texte blanc (style.css).",
        "OK",
        "",
    ),
    "2.3": (
        "Oui",
        "Périmètre géographique + Critères (masqué sans sélection géo) + Indicateurs.",
        "OK",
        "",
    ),
    "2.3.1": (
        "Oui",
        "Texte d’aide et radios Communes / Départements / Régions conformes au HTML.",
        "OK",
        "",
    ),
    "2.3.2": (
        "Oui",
        "switchModeUI() : affichage conditionnel des 3 blocs. Validation manuelle des clics recommandée.",
        "N/A",
        "",
    ),
    "2.3.3.1": (
        "Oui",
        "Mode Régions : « Toutes les régions » n’est pas pré-sélectionnée (fillRegionsMultiSelectRegionsOnly désélectionne tout).",
        "KO",
        "ACC002",
    ),
    "2.3.2.1.1": (
        "Oui",
        "Non rejoué dans un navigateur ; comportement attendu cohérent avec le code.",
        "N/A",
        "",
    ),
    "2.3.2.1.2": (
        "Oui",
        "Non rejoué dans un navigateur.",
        "N/A",
        "",
    ),
    "2.3.2.1.3": (
        "Oui",
        "Énoncé « Toutes les communes » dans la liste des régions : probable coquille. Non rejoué en UI.",
        "N/A",
        "",
    ),
    "2.3.3.3.5-R": (
        "Oui",
        "Suppression d’une région (×) : logique côté JS ; non cliqué en navigateur.",
        "N/A",
        "",
    ),
    "2.3.2.1.4": (
        "Oui",
        "Bouton Vider la liste (mode régions) : non cliqué en navigateur.",
        "N/A",
        "",
    ),
    "2.3.2.1": (
        "Oui",
        "Réinitialisation en revenant sur le mode Régions : non automatisé.",
        "N/A",
        "",
    ),
    "2.3.3.2": (
        "Oui",
        "Deux listes multi-régions / multi-départements ; défauts « Toutes les régions » et « Tous les départements » (setOnlySelected).",
        "OK",
        "",
    ),
    "2.3.3.2.1": (
        "Oui",
        "Filtrage départements : fillDeptsMultiSelect ; validation manuelle.",
        "N/A",
        "",
    ),
    "2.3.3.2.2": (
        "Oui",
        "Idem plusieurs régions.",
        "N/A",
        "",
    ),
    "2.3.3.2.3": (
        "Oui",
        "Plusieurs cas listés sous le même n° de pas ; non rejoué en détail en UI.",
        "N/A",
        "",
    ),
    "2.3.3.2.4": (
        "Oui",
        "« Tous les départements » : logique dans getSelectedDeptCodes.",
        "N/A",
        "",
    ),
    "2.3.3.3.5-D": (
        "Oui",
        "Suppression d’un département (×) : non cliqué en navigateur.",
        "N/A",
        "",
    ),
    "2.3.3.2.6": (
        "Oui",
        "Vider la liste mode départements : initDepartementsModeSelectionState().",
        "N/A",
        "",
    ),
    "2.3.2.2": (
        "Oui",
        "Réinit mode Départements : non automatisé.",
        "N/A",
        "",
    ),
    "2.3.3.3": (
        "Oui",
        "Selects Région / Département / Commune + recherche ; option commune « Toutes les communes du département » (écart possible avec libellé « Choisir une commune » du test).",
        "N/A",
        "",
    ),
    "2.3.3.3.1": (
        "Oui",
        "Select région simple : non rejoué en UI.",
        "N/A",
        "",
    ),
    "2.3.3.3.2": (
        "Oui",
        "Filtrage départements : non rejoué en UI.",
        "N/A",
        "",
    ),
    "2.3.3.3.3": (
        "Oui",
        "Sélection commune : non rejoué en UI.",
        "N/A",
        "",
    ),
    "2.3.3.3.4": (
        "Oui",
        "Option __ALL__ : code présent ; non rejoué en UI.",
        "N/A",
        "",
    ),
    "2.3.3.3.5-C": (
        "Oui",
        "Suppression commune (×) : non cliqué en navigateur.",
        "N/A",
        "",
    ),
    "2.3.3.3.6": (
        "Oui",
        "Vider la liste : vide selectedCommunes ; ne réapplique pas « Toutes les régions » (libellé issu d’un autre mode).",
        "N/A",
        "",
    ),
    "2.3.2.3": (
        "Oui",
        "Réinit mode Communes : non automatisé.",
        "N/A",
        "",
    ),
    "2.3.3-crit": (
        "Oui",
        "Granularité Agrégée/Détaillée ; colonnes Surface / Pièces souvent masquées : ne correspond pas à 3 colonnes toujours visibles avec seulement « Tous ».",
        "N/A",
        "",
    ),
    "2.3.3-ind": (
        "Oui",
        "Catégorie d’indicateurs, Nb max = 20, boutons, cases cochées par défaut.",
        "OK",
        "",
    ),
    "2.4": (
        "Oui",
        "Onglets tableaux / cartes présents ; pas de scénario détaillé dans cette feuille.",
        "N/A",
        "",
    ),
    "2.4.1": (
        "Oui",
        "Pas de cas détaillé dans la feuille.",
        "N/A",
        "",
    ),
    "2.4.2": (
        "Oui",
        "Pas de cas détaillé dans la feuille.",
        "N/A",
        "",
    ),
}


def fill_line(line: str, state: dict) -> str:
    if "|" not in line:
        return line
    parts = line.rstrip("\n\r").split("|")
    # garantir 11 colonnes
    while len(parts) < 11:
        parts.append("")

    pas = None
    if len(parts) >= 2:
        if parts[0] == "" and parts[1].strip():
            pas = parts[1].strip()
        elif parts[0] == "0" and len(parts) >= 3 and parts[1].strip():
            # 0|2.4|Zone...
            pas = parts[1].strip()

    key = None
    if pas and pas[0].isdigit():
        key = pas
    if key == "2.3.3":
        if "Crit" in line and "locaux" in line:
            key = "2.3.3-crit"
        elif "Indicateurs" in line and "Formulaire" in line:
            key = "2.3.3-ind"
    if key == "2.3.3.3.5":
        state["335"] = state.get("335", 0) + 1
        n = state["335"]
        if n == 1:
            key = "2.3.3.3.5-R"
        elif n == 2:
            key = "2.3.3.3.5-D"
        else:
            key = "2.3.3.3.5-C"

    if key and key in RESULTS:
        g, h, i, k = RESULTS[key]
        parts[6] = g
        parts[7] = h
        parts[8] = i
        parts[9] = DATE_TEST
        parts[10] = k
    return "|".join(parts[:11])


def main():
    path = Path(__file__).resolve().parents[1] / "Tests comparaison scores.csv"
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    state = {}
    out = [fill_line(line, state) for line in lines]
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print("OK:", path)


if __name__ == "__main__":
    main()
