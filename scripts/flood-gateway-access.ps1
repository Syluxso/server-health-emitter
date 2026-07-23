# Flood real gateway traffic so byz.gateway.access fills and Admin live feed updates.
# Usage (from Windows):
#   .\scripts\flood-gateway-access.ps1
#   .\scripts\flood-gateway-access.ps1 -Count 1000 -Concurrency 20
#   .\scripts\flood-gateway-access.ps1 -BaseUrl https://api.byzantineapp.dev -Path /files/actuator/health

param(
  [int]$Count = 1000,
  [int]$Concurrency = 20,
  [string]$BaseUrl = "https://api.byzantineapp.dev",
  [string]$Path = "/files/actuator/health"
)

$ErrorActionPreference = "Stop"
$url = ($BaseUrl.TrimEnd("/") + $Path)
Write-Host "POSTURE: GET $url  x$Count  (concurrency=$Concurrency)"

$ok = 0
$fail = 0
$sw = [System.Diagnostics.Stopwatch]::StartNew()

$jobs = for ($i = 1; $i -le $Count; $i++) {
  while ((Get-Job -State Running).Count -ge $Concurrency) {
    Start-Sleep -Milliseconds 20
    Get-Job -State Completed | ForEach-Object {
      try {
        $r = Receive-Job $_
        if ($r -eq "ok") { $script:ok++ } else { $script:fail++ }
      } catch { $script:fail++ }
      Remove-Job $_
    }
  }

  Start-Job -ScriptBlock {
    param($u)
    try {
      $resp = Invoke-WebRequest -Uri $u -Method GET -UseBasicParsing -TimeoutSec 15
      if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 400) { "ok" } else { "fail" }
    } catch { "fail" }
  } -ArgumentList $url
}

$jobs | Wait-Job | ForEach-Object {
  try {
    $r = Receive-Job $_
    if ($r -eq "ok") { $ok++ } else { $fail++ }
  } catch { $fail++ }
  Remove-Job $_
}

$sw.Stop()
Write-Host ("Done in {0:N1}s  ok={1} fail={2}" -f $sw.Elapsed.TotalSeconds, $ok, $fail)
Write-Host "Watch Admin dashboard Live Requests / RPS — events land on byz.gateway.access"
