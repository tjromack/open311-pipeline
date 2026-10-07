<#
.SYNOPSIS
  Scheduled portal re-pull (add-on A2): re-pull the fiscal year's last N days, resolve parents,
  rebuild dbt, print the report. Logs to logs/scheduled_repull_<timestamp>.log.

  On success: a Windows notification. On any failure: a Windows notification AND a GitHub issue
  on the repo (GitHub emails the repo owner) with the failing step and the log tail.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File scripts\scheduled_repull.ps1
  # Test without touching the main warehouse or opening an issue:
  powershell -File scripts\scheduled_repull.ps1 -Days 1 -DuckDbPath C:\tmp\copy.duckdb -DryRunIssue
#>
param(
    [int]$FiscalYear = 2026,
    [int]$Days = 30,
    [string]$DuckDbPath = "",
    [string]$Repo = "tjromack/open311-pipeline",
    [switch]$DryRunIssue,      # print the GitHub issue instead of creating it
    [switch]$SimulateFailure   # force a failure after the first step (tests the alert path)
)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$stamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
New-Item -ItemType Directory -Force (Join-Path $root "logs") | Out-Null
$log = Join-Path $root "logs\scheduled_repull_$stamp.log"
$py = Join-Path $root ".venv\Scripts\python.exe"
$dbt = Join-Path $root ".venv\Scripts\dbt.exe"
$env:DBT_PROFILES_DIR = Join-Path $root "dbt_project"
$env:PYTHONIOENCODING = "utf-8"
if ($DuckDbPath) { $env:DUCKDB_PATH = $DuckDbPath }

function Write-Log([string]$msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $log -Value $line -Encoding UTF8
}

function Show-Toast([string]$title, [string]$body) {
    try {
        [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
        $xml = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02)
        $text = $xml.GetElementsByTagName("text")
        $text.Item(0).AppendChild($xml.CreateTextNode($title)) | Out-Null
        $text.Item(1).AppendChild($xml.CreateTextNode($body)) | Out-Null
        $appId = "{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\WindowsPowerShell\v1.0\powershell.exe"
        [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($appId).Show(
            [Windows.UI.Notifications.ToastNotification]::new($xml))
    } catch {
        Write-Log "toast failed: $($_.Exception.Message)"
    }
}

function Invoke-Step([string]$name, [string]$cmdline) {
    Write-Log "==> $name :: $cmdline"
    # cmd /c keeps native stderr out of PowerShell's error stream (PS 5.1 wraps it otherwise).
    cmd /c "$cmdline >> `"$log`" 2>&1"
    $code = $LASTEXITCODE
    Write-Log "<== $name exit=$code"
    if ($code -ne 0) { throw "step '$name' failed with exit code $code" }
}

$failedStep = $null
try {
    Write-Log "scheduled portal re-pull: FY$FiscalYear, last $Days days, warehouse=$($env:DUCKDB_PATH)"
    $failedStep = "re-pull"
    Invoke-Step "re-pull" "`"$py`" -m ingestion.portal_loader --fy $FiscalYear --last-days $Days"
    if ($SimulateFailure) { $failedStep = "simulated"; throw "simulated failure (-SimulateFailure)" }
    $failedStep = "resolve parents"
    Invoke-Step "resolve parents" "`"$py`" -m ingestion.portal_loader --resolve-parents"
    $failedStep = "dbt build"
    Invoke-Step "dbt build" "`"$dbt`" build --project-dir dbt_project"
    $failedStep = "report"
    Invoke-Step "report" "`"$py`" scripts\portal_report.py"
    Write-Log "SUCCESS"
    Show-Toast "Open311 re-pull succeeded" "FY$FiscalYear last $Days days re-pulled, dbt build green. Log: logs\scheduled_repull_$stamp.log"
    exit 0
}
catch {
    $err = $_.Exception.Message
    Write-Log "FAILED: $err"
    $tail = (Get-Content $log -Tail 60 -ErrorAction SilentlyContinue) -join "`n"
    $title = "Scheduled portal re-pull failed ($stamp): $failedStep"
    $body = @"
The scheduled A2 portal re-pull failed on $env:COMPUTERNAME.

- Step: **$failedStep**
- Error: ``$err``
- Full log: ``logs/scheduled_repull_$stamp.log`` (local)

What to do: rerun ``make portal-repull FY=$FiscalYear``, then ``make dbt && make portal-report``. A
``count_mismatch`` means the portal's data changed mid-pull: rerun. A TLS error usually means the
local CA bundle went stale (README -> Troubleshooting).

<details><summary>Last 60 log lines</summary>

``````
$tail
``````
</details>
"@
    if ($DryRunIssue) {
        Write-Log "DRY RUN: would open GitHub issue: $title"
        Write-Output "DRY RUN issue title: $title"
        Write-Output $body
    } else {
        $bodyFile = Join-Path $env:TEMP "open311_repull_issue_$stamp.md"
        Set-Content -Path $bodyFile -Value $body -Encoding UTF8
        & gh issue create --repo $Repo --title $title --body-file $bodyFile 2>&1 | ForEach-Object { Write-Log "gh: $_" }
    }
    Show-Toast "Open311 re-pull FAILED" "Step: $failedStep. A GitHub issue was opened on $Repo. Log: logs\scheduled_repull_$stamp.log"
    exit 1
}
