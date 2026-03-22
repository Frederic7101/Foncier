#!/usr/bin/env python
"""
Réconcilie un fichier d'usage Cursor (CSV) avec un ou plusieurs transcripts JSONL.

Hypothèses :
- Le CSV contient une ligne par appel modèle, dans la colonne "Date" au format ISO 8601 (ordre inverse).
- Les transcripts JSONL contiennent tous les messages, dans l'ordre, avec des objets :
  {"role": "user" | "assistant", "message": {"content": [{"type": "text", "text": "..."}]}}
- On aligne chaque ligne d'usage avec le NIÈME message assistant dans l'ordre des transcripts.
- Pour chaque réponse assistant, on prend comme "demande" le dernier message user précédent.

Utilisation (exemple) :
    python reconcile_usage_transcripts.py \
        "c:\\Users\\frede\\Downloads\\usage-events-2026-03-93 à 2026-03-09.csv" \
        "c:\\Users\\frede\\OneDrive\\Documents\\Cursor\\reconciliation_usage_transcripts.csv" \
        "c:\\Users\\frede\\.cursor\\projects\\c-Users-frede-OneDrive-Documents-Cursor\\agent-transcripts\\2720a608-e897-45a4-b484-0040738f954e\\2720a608-e897-45a4-b484-0040738f954e.jsonl" \
        "c:\\Users\\frede\\.cursor\\projects\\c-Users-frede-OneDrive-Documents-Cursor\\agent-transcripts\\5ef2a0f5-a530-4e78-ad47-e3a01df8d3a1\\5ef2a0f5-a530-4e78-ad47-e3a01df8d3a1.jsonl"
"""

import csv
import json
import sys
from pathlib import Path


def load_usage_csv(path: Path):
    """
    Charge le CSV d'usage et le trie par Date croissante.

    Colonnes attendues (en-têtes) :
    Date,Kind,Model,Max Mode,Input (w/ Cache Write),Input (w/o Cache Write),
    Cache Read,Output Tokens,Total Tokens,Cost
    """
    rows = []
    with path.open(newline="", encoding="latin-1", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # On garde les lignes telles quelles (les champs restent des chaînes)
            rows.append(row)

    # Le CSV est dans l'ordre anti-chronologique => on le remet en ordre chronologique
    # Comme le format Date est ISO 8601, un tri lexicographique sur la chaîne suffit.
    rows_sorted = sorted(rows, key=lambda r: r["Date"])
    return rows_sorted


def extract_text_from_message_obj(obj: dict) -> str:
    """
    Extrait le texte d'un objet JSONL de transcript :
    {"role": "...", "message": {"content": [{"type": "text", "text": "..."}]}}.
    On concatène tous les blocs de type "text".
    """
    msg = obj.get("message", {})
    content = msg.get("content", [])
    parts = []
    for part in content:
        if isinstance(part, dict) and part.get("type") == "text":
            parts.append(part.get("text", ""))
    text = "\n".join(parts).strip()
    return text


def strip_user_query_tags(text: str) -> str:
    """
    Retire les balises <user_query> et </user_query> (et retours à la ligne
    adjacents) du texte des messages utilisateur.
    """
    if not text or not isinstance(text, str):
        return text
    s = text.strip()
    if s.startswith("<user_query>"):
        s = s[len("<user_query>"):].lstrip("\n")
    if s.endswith("</user_query>"):
        s = s[: -len("</user_query>")].rstrip("\n")
    return s.strip()


def load_messages_from_transcripts(transcript_paths):
    """
    Charge tous les messages depuis une liste de fichiers JSONL de transcript.

    Retourne une liste de dicts :
        {"role": "user" | "assistant", "text": "..."}
    dans l'ordre strict des lignes JSONL (et des fichiers),
    en supprimant les doublons consécutifs stricts (même role + même texte).
    """
    messages = []
    last_role = None
    last_text = None

    for tpath in transcript_paths:
        p = Path(tpath)
        with p.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                role = obj.get("role")
                text = extract_text_from_message_obj(obj)

                # Dédoublonnage : si même role + même texte que la ligne précédente, on saute
                if role == last_role and text == last_text:
                    continue

                messages.append({"role": role, "text": text})
                last_role = role
                last_text = text

    return messages

def build_turns(messages):
    """
    Construit des tours (user -> assistant*) à partir de la liste de messages.

    Un tour = dernier message user + tous les messages assistant qui suivent
    jusqu'au prochain user. Les réponses assistant sont concaténées avec deux
    retours chariots (\n\n).
    """
    turns = []
    current_user = None
    current_assistant_parts = []

    def flush_turn():
        nonlocal current_user, current_assistant_parts
        if current_user is not None or current_assistant_parts:
            assistant_text = "\n\n".join(p.strip() for p in current_assistant_parts if p.strip())
            turns.append({
                "user": current_user or "",
                "assistant": assistant_text,
            })
        current_user = None
        current_assistant_parts = []

    for m in messages:
        role = m.get("role")
        text = m.get("text", "") or ""
        if role == "user":
            # On clôt le tour précédent et on démarre un nouveau tour
            flush_turn()
            current_user = strip_user_query_tags(text)
            current_assistant_parts = []
        elif role == "assistant":
            # On accumule toutes les réponses assistant du tour courant
            current_assistant_parts.append(text)

    # Dernier tour éventuel
    flush_turn()
    return turns

def deduplicate_turns(turns):
    """
    Filtrage des tours :

    - Supprime TOUS les tours sans réponse (assistant == "").
    - Pour les tours avec réponse, ne dédoublonne que les doublons consécutifs
      (même user + même assistant) pour éviter un éventuel bug de transcript.
    """
    cleaned = []
    last_full_key = None

    for t in turns:
        user_text = (t.get("user", "") or "").strip()
        assistant_text = (t.get("assistant", "") or "").strip()

        # On ignore tous les tours sans réponse assistant
        if not assistant_text:
            continue

        full_key = (user_text, assistant_text)
        if full_key == last_full_key:
            # Même tour complet répété juste après : on le saute
            continue

        cleaned.append({"user": user_text, "assistant": assistant_text})
        last_full_key = full_key

    return cleaned

def build_usage_message_pairs(usage_rows, messages):
    """
    Construit la liste des paires (usage, user_message, assistant_message)
    en travaillant par TOURS (user -> assistant*).

    - On construit d'abord une liste de tours via build_turns(messages).
    - On dédoublonne les tours.
    - On aligne la i-ème ligne de usage_rows avec la i-ème tour.
    - Les tours restants (sans match usage) sont ajoutés à la fin avec
      des champs usage vides.
    """
    raw_turns = build_turns(messages)
    turns = deduplicate_turns(raw_turns)

    n = min(len(usage_rows), len(turns))
    if len(usage_rows) != len(turns):
        print(
            f"ATTENTION : {len(usage_rows)} lignes d'usage, "
            f"{len(turns)} tours user/assistant (après dédoublonnage global). "
            f"On ne réconcilie que les {n} premiers via usage.",
            file=sys.stderr,
        )

    pairs = []

    # 1) Paires usage + tour pour les n premiers
    for k in range(n):
        u = usage_rows[k]
        t = turns[k]
        pairs.append({
            "Date": u.get("Date", ""),
            "Kind": u.get("Kind", ""),
            "Model": u.get("Model", ""),
            "MaxMode": u.get("Max Mode", ""),
            "InputTokensWithCache": u.get("Input (w/ Cache Write)", ""),
            "InputTokensNoCache": u.get("Input (w/o Cache Write)", ""),
            "CacheReadTokens": u.get("Cache Read", ""),
            "OutputTokens": u.get("Output Tokens", ""),
            "TotalTokens": u.get("Total Tokens", ""),
            "Cost": u.get("Cost", ""),
            "UserMessage": t.get("user", ""),
            "AssistantMessage": t.get("assistant", ""),
        })

    # 2) Tours restants sans usage (tours[n:]) → ajoutés avec colonnes usage vides
    for t in turns[n:]:
        pairs.append({
            "Date": "",
            "Kind": "",
            "Model": "",
            "MaxMode": "",
            "InputTokensWithCache": "",
            "InputTokensNoCache": "",
            "CacheReadTokens": "",
            "OutputTokens": "",
            "TotalTokens": "",
            "Cost": "",
            "UserMessage": t.get("user", ""),
            "AssistantMessage": t.get("assistant", ""),
        })

    return pairs


def write_output_csv(pairs, output_path: Path):
    """
    Écrit le CSV final avec les colonnes :
    Date,Kind,Model,MaxMode,InputTokensWithCache,InputTokensNoCache,
    CacheReadTokens,OutputTokens,TotalTokens,Cost,UserMessage,AssistantMessage
    """
    fieldnames = [
        "Date",
        "Kind",
        "Model",
        "MaxMode",
        "InputTokensWithCache",
        "InputTokensNoCache",
        "CacheReadTokens",
        "OutputTokens",
        "TotalTokens",
        "Cost",
        "UserMessage",
        "AssistantMessage",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in pairs:
            writer.writerow(row)


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) < 3:
        print(
            "Usage :\n"
            "  python reconcile_usage_transcripts.py "
            "<usage.csv> <sortie.csv> <transcript1.jsonl> [<transcript2.jsonl> ...]\n",
            file=sys.stderr,
        )
        sys.exit(1)

    usage_csv_path = Path(argv[0])
    output_csv_path = Path(argv[1])
    transcript_paths = argv[2:]

    print(f"Lecture du CSV d'usage : {usage_csv_path}")
    usage_rows = load_usage_csv(usage_csv_path)
    print(f"{len(usage_rows)} lignes d'usage chargées (après tri par Date croissante).")

    print("Lecture des transcripts JSONL :")
    for t in transcript_paths:
        print(f"  - {t}")
    messages = load_messages_from_transcripts(transcript_paths)
    print(f"{len(messages)} messages (user/assistant) chargés au total.")

    print("Construction des paires usage ↔ messages…")
    pairs = build_usage_message_pairs(usage_rows, messages)
    print(f"{len(pairs)} paires construites.")

    print(f"Écriture du CSV de sortie : {output_csv_path}")
    write_output_csv(pairs, output_csv_path)

    print("Terminé.")


if __name__ == "__main__":
    main()