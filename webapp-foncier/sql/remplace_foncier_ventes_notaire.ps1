# Remplace 'foncier' par 'ventes_notaire' dans le dump SQL (traitement ligne par ligne, peu de mémoire).
# Ajoute en tête du fichier généré la désactivation des contraintes (FOREIGN_KEY_CHECKS, UNIQUE_CHECKS)
# pour accélérer l'import, et les réactive en fin de fichier.
# Usage: .\remplace_foncier_ventes_notaire.ps1 [fichier_dump]

param(
    [string]$InputFile = "foncier_dump.sql",
    [string]$OutputFile = "ventes_notaire_dump.sql"
)

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not [IO.Path]::IsPathRooted($InputFile))  { $InputFile  = Join-Path $here $InputFile }
if (-not [IO.Path]::IsPathRooted($OutputFile)) { $OutputFile = Join-Path $here $OutputFile }

if (-not (Test-Path $InputFile)) {
    Write-Error "Fichier introuvable: $InputFile"
    exit 1
}

$reader = [System.IO.StreamReader]::new($InputFile, [System.Text.Encoding]::UTF8)
$writer = [System.IO.StreamWriter]::new($OutputFile, $false, [System.Text.Encoding]::UTF8)
$lineCount = 0
try {
    # Désactiver les contraintes en début de fichier pour accélérer l'import
    $writer.WriteLine("-- Désactivation des contraintes pour import rapide")
    $writer.WriteLine("SET FOREIGN_KEY_CHECKS=0;")
    $writer.WriteLine("SET UNIQUE_CHECKS=0;")
    $writer.WriteLine("")

    while ($null -ne ($line = $reader.ReadLine())) {
        $lineCount++
        if ($lineCount % 100000 -eq 0) { Write-Host "  $lineCount lignes..." }
        $writer.WriteLine($line -replace '\bfoncier\b', 'ventes_notaire')
    }

    # Réactiver les contraintes en fin de fichier
    $writer.WriteLine("")
    $writer.WriteLine("-- Réactivation des contraintes")
    $writer.WriteLine("SET UNIQUE_CHECKS=1;")
    $writer.WriteLine("SET FOREIGN_KEY_CHECKS=1;")
} finally {
    $reader.Dispose()
    $writer.Dispose()
}

Write-Host "OK: $OutputFile créé ($lineCount lignes, foncier -> ventes_notaire, contraintes désactivées pendant l'import)."
