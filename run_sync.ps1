Param(
    [string]$ImageName = "corp-ledger",
    [string]$ConfigFile = "config.yaml"
)

$ErrorActionPreference = "Stop"

Write-Host "Building Docker image: $ImageName" -ForegroundColor Cyan
docker build -t $ImageName .

# Resolve current directory path for volume mount
$hostDir = (Get-Location).Path
Write-Host "Using host directory for persistence: $hostDir" -ForegroundColor Yellow

# Run sync-wallet inside the container
Write-Host "Running wallet sync..." -ForegroundColor Cyan
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile sync-wallet

Write-Host "Wallet sync complete. ledger.db should be in $hostDir" -ForegroundColor Green
Write-Host "Running contract sync..." -ForegroundColor Cyan
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile sync-contracts

Write-Host "Contract sync complete. ledger.db should be in $hostDir" -ForegroundColor Green

Write-Host "Running industry sync..." -ForegroundColor Cyan
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile sync-industry

Write-Host "Industry sync complete. ledger.db should be in $hostDir" -ForegroundColor Green

Write-Host "Running market sync..." -ForegroundColor Cyan
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile sync-market

Write-Host "Market sync complete. ledger.db should be in $hostDir" -ForegroundColor Green

# PRICES CONTRACT NEEDED BEFORE WE APPLY FLOWS
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile list-contracts
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile sync-flows


Write-Host "Listing last $Limit donations..." -ForegroundColor Cyan
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile list-donations



# NOW WE CAN GENERATE OUT FLOW AND DASHBOARD
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile report-flows

Write-Host "---Dashboard---" -ForegroundColor Cyan
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile dashboard
docker run --rm `
    -v "$($hostDir):/app" `
    $ImageName `
    python corp_ledger.py --config $ConfigFile export-excel