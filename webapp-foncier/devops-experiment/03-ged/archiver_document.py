import shutil
from datetime import datetime
from pathlib import Path


def archiver_document(chemin_source: str, dossier_archive: str = "archive") -> Path:
    """
    Copie un document vers un dossier d'archive avec horodatage.

    Exemple d'utilisation :
        archiver_document("SPEC_IHM-Stats_v1.2.md")
    """
    src = Path(chemin_source)
    if not src.is_file():
        raise FileNotFoundError(f"Fichier introuvable: {src}")

    archive_root = Path(dossier_archive)
    archive_root.mkdir(parents=True, exist_ok=True)

    horodatage = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest_name = f"{src.stem}_{horodatage}{src.suffix}"
    dest = archive_root / dest_name

    shutil.copy2(src, dest)
    return dest


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Archiver un document avec horodatage.")
    parser.add_argument("fichier", help="Chemin du fichier à archiver")
    parser.add_argument(
        "--dossier-archive",
        default="archive",
        help="Dossier d'archive (par défaut: archive)",
    )
    args = parser.parse_args()

    cible = archiver_document(args.fichier, args.dossier_archive)
    print(f"Fichier archivé vers: {cible}")

