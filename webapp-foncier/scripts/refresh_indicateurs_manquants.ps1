<#
.SYNOPSIS
    Rafraîchit les indicateurs des communes absentes de indicateurs_communes.

.DESCRIPTION
    1. Interroge PostgreSQL pour lister les communes de ref_communes
       dont le code_insee ne figure pas dans indicateurs_communes
       (= communes sans indicateurs précalculés).
    2. Zero-pad les code_insee à 5 caractères (ex. "2168" → "02168").
    3. Appelle POST /api/refresh-indicateurs par lots de $BatchSize communes.
    4. Affiche la progression et écrit un rapport dans logs/.

.PARAMETER ApiBase
    URL de base de l'API (défaut : http://localhost:8000)

.PARAMETER BatchSize
    Nombre de communes par appel API (défaut : 200 — ~4 ko de query string)

.PARAMETER Workers
    Nombre de workers parallèles côté serveur pour calculer les fiches (1-16, défaut : 4)

.PARAMETER DryRun
    Si présent, affiche les codes mais n'appelle pas l'API.

.PARAMETER ConfigFile
    Chemin du config.postgres.json (défaut : relatif au répertoire du script)

.EXAMPLE
    .\refresh_indicateurs_manquants.ps1
    .\refresh_indicateurs_manquants.ps1 -BatchSize 100 -Workers 8
    .\refresh_indicateurs_manquants.ps1 -DryRun
#>

param(
    [string] $ApiBase    = "http://localhost:8000",
    [int]    $BatchSize  = 200,
    [int]    $Workers    = 4,
    [switch] $DryRun,
    [string] $ConfigFile = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Résolution des chemins ────────────────────────────────────────────────────
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir
if (-not $ConfigFile) {
    $ConfigFile = Join-Path $ProjectDir "config.postgres.json"
}
$LogDir = Join-Path $ProjectDir "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$LogFile = Join-Path $LogDir ("refresh_indicateurs_" + (Get-Date -Format "yyyyMMdd_HHmmss") + ".log")

# ── Lecture de la config PostgreSQL ──────────────────────────────────────────
if (-not (Test-Path $ConfigFile)) {
    Write-Error "Fichier de config introuvable : $ConfigFile"
    exit 1
}
$cfg     = (Get-Content $ConfigFile -Raw | ConvertFrom-Json).database
$PgHost  = $cfg.host
$PgPort  = $cfg.port
$PgUser  = $cfg.user
$PgPwd   = $cfg.password
$PgDb    = $cfg.database

# ── Logging ──────────────────────────────────────────────────────────────────
function Log {
    param([string]$msg, [string]$level = "INFO")
    $ts   = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] [$level] $msg"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

# ── Vérification que psql est disponible ─────────────────────────────────────
$psqlExe = Get-Command psql -ErrorAction SilentlyContinue
if (-not $psqlExe) {
    # Essayer les chemins d'installation courants de PostgreSQL sur Windows
    $candidates = @(
        "C:\Program Files\PostgreSQL\17\bin\psql.exe",
        "C:\Program Files\PostgreSQL\16\bin\psql.exe",
        "C:\Program Files\PostgreSQL\15\bin\psql.exe",
        "C:\Program Files\PostgreSQL\14\bin\psql.exe"
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { $psqlExe = $c; break }
    }
    if (-not $psqlExe) {
        Write-Error "psql introuvable. Ajouter le répertoire bin de PostgreSQL au PATH."
        exit 1
    }
} else {
    $psqlExe = $psqlExe.Source
}
Log "psql : $psqlExe"

# ── Requête SQL pour récupérer les code_insee manquants ──────────────────────
# Requête directe (plus rapide que la vue v_diagnostic_completude_communes)
# LPAD assure 5 caractères ; on exclut les codes non numériques vides.
$SQL = @"
SELECT LPAD(code_insee, 5, '0')
FROM   foncier.ref_communes
WHERE  code_insee IS NOT NULL
  AND  code_insee <> ''
  AND  NOT EXISTS (
           SELECT 1
           FROM   foncier.indicateurs_communes i
           WHERE  i.code_insee = foncier.ref_communes.code_insee
       )
ORDER  BY code_insee;
"@

Log "Interrogation de la base $PgDb@${PgHost}:${PgPort} pour les communes sans indicateurs..."

# Passer le mot de passe via variable d'environnement (évite l'invite interactive)
$env:PGPASSWORD = $PgPwd
try {
    $raw = & $psqlExe `
        -h $PgHost -p $PgPort -U $PgUser -d $PgDb `
        -t -A -c $SQL 2>&1
} finally {
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}

if ($LASTEXITCODE -ne 0) {
    Log "Erreur psql (code $LASTEXITCODE) : $raw" "ERROR"
    exit 1
}

# Filtrer les lignes vides et les messages psql (ex. "N rows")
$codeList = $raw |
    Where-Object { $_ -match '^\d{5}$' -or $_ -match '^[0-9]{1,2}[A-Za-z][0-9]{2}$' } |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -ne "" }

$total = $codeList.Count
Log "Communes sans indicateurs : $total"

if ($total -eq 0) {
    Log "Rien à faire — tous les indicateurs sont à jour."
    exit 0
}

if ($DryRun) {
    Log "DryRun activé — premiers codes : $($codeList[0..([Math]::Min(9, $total-1))] -join ', ')..."
    Log "Aucun appel API effectué."
    exit 0
}

# ── Appels API par lots ───────────────────────────────────────────────────────
$batchCount  = [Math]::Ceiling($total / $BatchSize)
$doneTotal   = 0
$rejectTotal = 0
$errorBatches = 0

Log "Début du refresh : $total communes, $batchCount lots de $BatchSize, $Workers workers"
Log "API : $ApiBase/api/refresh-indicateurs"
Log "Log : $LogFile"
Log ("-" * 70)

for ($i = 0; $i -lt $total; $i += $BatchSize) {

    $end     = [Math]::Min($i + $BatchSize - 1, $total - 1)
    $batch   = $codeList[$i..$end]
    $batchNo = [Math]::Floor($i / $BatchSize) + 1

    # Construire la query string : ?code_insee_list=X&code_insee_list=Y&...
    $qs  = ($batch | ForEach-Object { "code_insee_list=$_" }) -join "&"
    $url = "$ApiBase/api/refresh-indicateurs?$qs&workers=$Workers"

    $preview = "$($batch[0])…$($batch[-1])"
    Log "Lot $batchNo/$batchCount ($($batch.Count) communes : $preview)"

    try {
        $resp = Invoke-RestMethod `
            -Method      Post `
            -Uri         $url `
            -ContentType "application/json" `
            -TimeoutSec  600   # 10 min max par lot (fiches réseau)

        $done    = if ($null -eq $resp.refreshed)      { 0 } else { [int]$resp.refreshed }
        $cached  = if ($null -eq $resp.from_cache)     { 0 } else { [int]$resp.from_cache }
        $reject  = if ($null -eq $resp.codes_rejected) { 0 } else { [int]$resp.codes_rejected }
        $doneTotal   += $done
        $rejectTotal += $reject

        Log ("  → refreshed=$done (from_cache=$cached), rejected=$reject" +
             $(if ($resp.codes_fiche_indisponible.Count -gt 0) {
                 ", fiche_indisponible=" + $resp.codes_fiche_indisponible.Count } else { "" }))

        # Détail des rejets si présents
        if ($resp.codes_missing_dept_or_commune.Count -gt 0) {
            Log "    missing_dept_or_commune : $($resp.codes_missing_dept_or_commune -join ', ')" "WARN"
        }
        if ($resp.codes_upsert_failed.Count -gt 0) {
            Log "    upsert_failed : $($resp.codes_upsert_failed -join ', ')" "WARN"
        }

    } catch {
        $msg = $_.Exception.Message
        Log "  ERREUR lot $batchNo : $msg" "ERROR"
        $errorBatches++
        # Continuer avec le lot suivant
    }
}

# ── Rapport final ─────────────────────────────────────────────────────────────
Log ("-" * 70)
Log "RÉSUMÉ"
Log "  Communes traitées   : $doneTotal / $total"
Log "  Rejetées (total)    : $rejectTotal"
Log "  Lots en erreur HTTP : $errorBatches"
Log "  Rapport complet     : $LogFile"

if ($errorBatches -gt 0) {
    Log "Des erreurs se sont produites. Relancer le script pour les lots manquants." "WARN"
    exit 2
}
exit 0
