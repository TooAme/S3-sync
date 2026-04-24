param(
    [string]$Image = "python:3.11-slim"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path

$cmd = @"
set -e
python3 --version
python3 -m pip install --upgrade pip pyinstaller boto3 botocore s3transfer
python3 -m PyInstaller --clean --noconfirm --onefile --name s3-sync --collect-data botocore --collect-data boto3 --collect-data s3transfer --distpath dist/linux --workpath build/linux sync_s3_cross_account.py
python3 -m PyInstaller --clean --noconfirm --onefile --name s3-key-test --collect-data botocore --collect-data boto3 --collect-data s3transfer --distpath dist/linux --workpath build/linux test_aws_key.py
ls -lh dist/linux
"@

docker run --rm `
    -v "${root}:/work" `
    -w /work `
    $Image `
    bash -lc "$cmd"

if ($LASTEXITCODE -ne 0) {
    throw "Docker build failed. Make sure Docker Desktop is running."
}
