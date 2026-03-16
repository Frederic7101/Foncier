# Appeler psql par chemin complet (sans dépendre du PATH).
# Usage: .\psql_bin.ps1 -U postgres
#        .\psql_bin.ps1 -U postgres -d foncier -f script.sql
# Adapter $pgBin si ta version (16, 17, 18) ou chemin diffère.

$pgBin = "C:\Program Files\PostgreSQL\18\bin"
$psql = Join-Path $pgBin "psql.exe"
if (-not (Test-Path $psql)) {
    Write-Host "Introuvable: $psql (adapter la version dans le script si besoin)"
    exit 1
}
& $psql @args
exit $LASTEXITCODE
