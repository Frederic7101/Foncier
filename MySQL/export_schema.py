"""
Exporte le schéma de la base foncier (structure seule, sans données)
vers MySQL/foncier_schema.sql.
Utilise webapp/backend/.env pour les paramètres de connexion.
À lancer depuis la racine du projet : python MySQL/export_schema.py
"""
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / "webapp" / "backend" / ".env"
OUTPUT_FILE = PROJECT_ROOT / "MySQL" / "foncier_schema.sql"


def load_dotenv_manual(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


def main() -> int:
    os.chdir(PROJECT_ROOT)
    load_dotenv_manual(ENV_FILE)

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    database = os.getenv("DB_NAME", "foncier")

    mysqldump = "mysqldump"
    if sys.platform == "win32":
        for base in (Path(r"C:\Program Files\MySQL"), Path(r"C:\Program Files (x86)\MySQL")):
            if base.exists():
                found = list(base.rglob("mysqldump.exe"))
                if found:
                    mysqldump = str(found[0])
                    break

    args = [
        mysqldump,
        "--no-data",
        "--single-transaction",
        "-h", host,
        "-P", port,
        "-u", user,
        database,
    ]
    env = {**os.environ}
    if password:
        env["MYSQL_PWD"] = password

    try:
        r = subprocess.run(
            args,
            capture_output=True,
            text=True,
            env=env,
            cwd=str(PROJECT_ROOT),
        )
        if r.returncode != 0:
            print(r.stderr or r.stdout, file=sys.stderr)
            return 1
        OUTPUT_FILE.write_text(r.stdout, encoding="utf-8")
        print(f"Schéma exporté vers {OUTPUT_FILE}")
        return 0
    except FileNotFoundError:
        print("Erreur : mysqldump introuvable. Installez le client MySQL et ajoutez-le au PATH.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
