param(
    [string]$PythonExe = "",
    [switch]$Clean = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

function Resolve-PythonExe {
    param([string]$Preferred)

    if ($Preferred -and (Test-Path $Preferred)) {
        return $Preferred
    }

    $candidates = @(
        "$env:LOCALAPPDATA\Programs\Python\Python37\python3.exe",
        "python3",
        "python"
    )

    foreach ($candidate in $candidates) {
        try {
            $null = & $candidate --version 2>$null
            if ($LASTEXITCODE -eq 0) {
                return $candidate
            }
        } catch {
            continue
        }
    }

    throw "No usable Python 3 executable found. Pass -PythonExe explicitly."
}

$py = Resolve-PythonExe -Preferred $PythonExe
Write-Host "Using Python: $py"
Write-Host ("Python version: " + (& $py --version 2>&1))

$pyMinor = & $py -c "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')"
$installerSpec = if ([version]$pyMinor -le [version]"3.7") { "pyinstaller==5.13.2" } else { "pyinstaller" }

Write-Host "Installing builder: $installerSpec"
& $py -m pip install --upgrade $installerSpec

if ($Clean) {
    if (Test-Path "build\windows") { Remove-Item -Recurse -Force "build\windows" }
    if (Test-Path "dist\windows") { Remove-Item -Recurse -Force "dist\windows" }
}

New-Item -ItemType Directory -Force -Path "dist\windows" | Out-Null
New-Item -ItemType Directory -Force -Path "build\windows" | Out-Null

$common = @(
    "--clean",
    "--noconfirm",
    "--onefile",
    "--collect-data", "botocore",
    "--collect-data", "boto3",
    "--collect-data", "s3transfer",
    "--distpath", "dist/windows",
    "--workpath", "build/windows"
)

Write-Host "Building s3-sync.exe ..."
& $py -m PyInstaller @common --name "s3-sync" "sync_s3_cross_account.py"

Write-Host "Building s3-key-test.exe ..."
& $py -m PyInstaller @common --name "s3-key-test" "test_aws_key.py"

Write-Host "Build done. Files:"
Get-ChildItem "dist\windows" | Select-Object Name, Length, LastWriteTime | Format-Table -AutoSize
