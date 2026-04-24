#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: $PYTHON_BIN not found"
  exit 1
fi

echo "Using Python: $PYTHON_BIN"
"$PYTHON_BIN" --version

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
"$PYTHON_BIN" -c 'import boto3, botocore, s3transfer, PyInstaller'

mkdir -p dist/linux build/linux
rm -rf build/linux/*

COMMON_ARGS=(
  --clean
  --noconfirm
  --onefile
  --collect-data botocore
  --collect-data boto3
  --collect-data s3transfer
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
