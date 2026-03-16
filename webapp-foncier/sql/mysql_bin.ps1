# Appeler les outils MySQL par chemin complet (sans dépendre du PATH).
# Usage: .\mysql_bin.ps1 mysqldump -u root -p foncier
#        .\mysql_bin.ps1 mysql -u root -p encheres

$mysqlBin = "C:\Program Files\MySQL\MySQL Server 8.0\bin"
$exe = $args[0]
$rest = $args[1..($args.Length - 1)]

if (-not $exe) {
    Write-Host "Usage: .\mysql_bin.ps1 mysqldump -u root -p foncier"
    Write-Host "       .\mysql_bin.ps1 mysql -u root -p encheres"
    exit 1
}

$path = Join-Path $mysqlBin "$exe.exe"
if (-not (Test-Path $path)) {
    Write-Host "Introuvable: $path"
    exit 1
}

& $path @rest
exit $LASTEXITCODE
