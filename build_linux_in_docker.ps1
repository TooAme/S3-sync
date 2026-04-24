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
python3 - <<'PY'
import importlib.util
for name in ("boto3", "botocore", "s3transfer"):
    spec = importlib.util.find_spec(name)
    if spec is None:
        raise SystemExit(f"ERROR: missing module '{name}'")
    is_pkg = bool(getattr(spec, "submodule_search_locations", None))
    print(f"{name}: origin={spec.origin} package={is_pkg}")
    if not is_pkg:
        raise SystemExit(f"ERROR: '{name}' is not a package: {spec.origin}")
PY
python3 -m PyInstaller --clean --noconfirm --onefile --name s3-sync --collect-all botocore --collect-all boto3 --collect-all s3transfer --distpath dist/linux --workpath build/linux sync_s3_cross_account.py
python3 -m PyInstaller --clean --noconfirm --onefile --name s3-key-test --collect-all botocore --collect-all boto3 --collect-all s3transfer --distpath dist/linux --workpath build/linux test_aws_key.py
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
