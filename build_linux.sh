#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

SYSTEM_PYTHON_BIN="${PYTHON_BIN:-python3}"
USE_VENV="${USE_VENV:-1}"
VENV_DIR="${VENV_DIR:-.venv-build}"
PYTHON_BIN="$SYSTEM_PYTHON_BIN"

if ! command -v "$SYSTEM_PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: $SYSTEM_PYTHON_BIN not found"
  exit 1
fi

if [[ "$USE_VENV" == "1" ]]; then
  VENV_PATH="$ROOT_DIR/$VENV_DIR"
  if [[ ! -x "$VENV_PATH/bin/python" ]]; then
    echo "Creating virtual environment: $VENV_PATH"
    if ! "$SYSTEM_PYTHON_BIN" -m venv "$VENV_PATH"; then
      echo "ERROR: failed to create venv."
      echo "Hint: install python3-venv (Debian/Ubuntu: apt install -y python3-venv)."
      exit 1
    fi
  fi
  PYTHON_BIN="$VENV_PATH/bin/python"
fi

echo "Using Python: $PYTHON_BIN"
"$PYTHON_BIN" --version

echo "Upgrading pip ..."
"$PYTHON_BIN" -m pip install --upgrade pip

PY_MINOR="$("$PYTHON_BIN" -c 'import sys;print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
if "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info[:2] <= (3,7) else 1)'; then
  INSTALLER_SPEC="pyinstaller==5.13.2"
else
  INSTALLER_SPEC="pyinstaller"
fi

BUILD_DEPS=(
  "$INSTALLER_SPEC"
  "boto3"
  "botocore"
  "s3transfer"
)

echo "Installing build dependencies: ${BUILD_DEPS[*]}"
"$PYTHON_BIN" -m pip install --upgrade "${BUILD_DEPS[@]}"

echo "Verifying Python dependencies ..."
"$PYTHON_BIN" - <<'PY'
import importlib.util

modules = ("boto3", "botocore", "s3transfer")
for name in modules:
    spec = importlib.util.find_spec(name)
    if spec is None:
        raise SystemExit(f"ERROR: missing module '{name}' in build interpreter")
    is_pkg = bool(getattr(spec, "submodule_search_locations", None))
    print(f"{name}: origin={spec.origin} package={is_pkg}")
    if not is_pkg:
        raise SystemExit(
            f"ERROR: '{name}' resolved to non-package ({spec.origin}). "
            "Likely module shadowing in PYTHONPATH/workdir."
        )
PY
"$PYTHON_BIN" -c 'import PyInstaller'

mkdir -p dist/linux build/linux
rm -rf build/linux/*

COMMON_ARGS=(
  --clean
  --noconfirm
  --onefile
  --collect-all botocore
  --collect-all boto3
  --collect-all s3transfer
  --distpath dist/linux
  --workpath build/linux
)

echo "Building s3-sync ..."
"$PYTHON_BIN" -m PyInstaller "${COMMON_ARGS[@]}" --name s3-sync sync_s3_cross_account.py

echo "Building s3-key-test ..."
"$PYTHON_BIN" -m PyInstaller "${COMMON_ARGS[@]}" --name s3-key-test test_aws_key.py

chmod +x dist/linux/s3-sync dist/linux/s3-key-test || true

echo "Build done:"
ls -lh dist/linux
