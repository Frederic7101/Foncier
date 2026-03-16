#!/bin/bash
# Lancer pgLoader depuis WSL pour éviter l'erreur MYSQL-UNSUPPORTED-AUTHENTICATION
# du conteneur Docker. À exécuter dans WSL (Ubuntu) : bash run_pgloader_wsl.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_FILE="${SCRIPT_DIR}/mysql_to_ventes_notaire.load"

# Depuis WSL, l'hôte Windows est le nameserver (sauf si tu as modifié resolv.conf)
WIN_HOST="${WIN_HOST:-$(grep nameserver /etc/resolv.conf 2>/dev/null | awk '{print $2}')}"
if [ -z "$WIN_HOST" ]; then
  echo "Impossible de détecter l'IP de l'hôte Windows. Définir WIN_HOST : export WIN_HOST=192.168.x.x"
  exit 1
fi

echo "Hôte MySQL/PostgreSQL (Windows) : $WIN_HOST"

# Fichier temporaire avec l'hôte substitué (host.docker.internal → IP Windows)
TMP_LOAD=$(mktemp)
sed "s/host\.docker\.internal/$WIN_HOST/g" "$LOAD_FILE" > "$TMP_LOAD"
trap "rm -f $TMP_LOAD" EXIT

pgloader "$TMP_LOAD"
