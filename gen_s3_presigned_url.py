#!/usr/bin/env python3
"""
Generate S3 presigned URLs using credentials from .env / CLI.

Examples:
  python gen_s3_presigned_url.py --side src --op get --key path/to/file.txt
  python gen_s3_presigned_url.py --side src --op list --prefix some/folder/
  python gen_s3_presigned_url.py --side dst --op put --key upload/demo.txt --expires 900
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config


RETRY_CONFIG = Config(retries={"max_attempts": 8, "mode": "adaptive"})


def load_dotenv(path: str, *, override: bool = True) -> int:
    env_path = Path(path)
    if not env_path.exists():
        return 0

    loaded = 0
    for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export ") :].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()

        if override:
            os.environ[key] = value
        else:
            os.environ.setdefault(key, value)
        loaded += 1

    return loaded


def has_text(value: str | None) -> bool:
    return value is not None and value.strip() != ""


def build_client_config(addressing_style: str) -> Config:
    if addressing_style in {"path", "virtual"}:
        return Config(
            retries={"max_attempts": 8, "mode": "adaptive"},
            s3={"addressing_style": addressing_style},
        )
    return RETRY_CONFIG


def env_of(side: str, suffix: str) -> str:
    return f"{side.upper()}_{suffix}"


def build_client(
    *,
    region: str | None,
    endpoint_url: str | None,
    addressing_style: str,
    access_key_id: str | None,
    secret_access_key: str | None,
    session_token: str | None,
    profile: str | None,
):
    resolved_region = region or ("us-east-1" if has_text(endpoint_url) else None)
    kwargs = {"config": build_client_config(addressing_style)}
    if has_text(endpoint_url):
        kwargs["endpoint_url"] = endpoint_url

    if has_text(access_key_id) and has_text(secret_access_key):
        session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
            region_name=resolved_region,
        )
        return session.client("s3", **kwargs)

    if has_text(profile):
        session = boto3.Session(profile_name=profile, region_name=resolved_region)
        return session.client("s3", **kwargs)

    session = boto3.Session(region_name=resolved_region)
    return session.client("s3", **kwargs)


def parse_args() -> argparse.Namespace:
    preload_parser = argparse.ArgumentParser(add_help=False)
    preload_parser.add_argument("--env-file", default=".env")
    preload_args, _ = preload_parser.parse_known_args()
    load_dotenv(preload_args.env_file, override=True)

    parser = argparse.ArgumentParser(
        parents=[preload_parser],
        description="Generate S3 presigned URLs.",
    )
    parser.add_argument("--side", choices=["src", "dst"], default="src")
    parser.add_argument("--op", choices=["get", "put", "head", "list"], default="get")
    parser.add_argument("--bucket", default=None, help="default: from env SRC_BUCKET / DST_BUCKET")
    parser.add_argument("--region", default=None, help="default: from env SRC_REGION / DST_REGION")
    parser.add_argument(
        "--endpoint",
        default=None,
        help="default: from env SRC_S3_ENDPOINT / DST_S3_ENDPOINT",
    )
    parser.add_argument(
        "--addressing-style",
        default=None,
        choices=["auto", "path", "virtual"],
        help="default: from env SRC_S3_ADDRESSING_STYLE / DST_S3_ADDRESSING_STYLE",
    )

    parser.add_argument("--ak", default=None, help="default: from env SRC_AWS_ACCESS_KEY_ID / DST_*")
    parser.add_argument("--sk", default=None, help="default: from env SRC_AWS_SECRET_ACCESS_KEY / DST_*")
    parser.add_argument("--token", default=None, help="default: from env SRC_AWS_SESSION_TOKEN / DST_*")
    parser.add_argument("--profile", default=None, help="default: from env SRC_AWS_PROFILE / DST_*")

    parser.add_argument("--key", default="", help="required for get/put/head")
    parser.add_argument("--prefix", default="")
    parser.add_argument("--delimiter", default="")
    parser.add_argument("--max-keys", type=int, default=1000)
    parser.add_argument("--expires", type=int, default=3600, help="seconds, max usually 604800")
    parser.add_argument(
        "--content-type",
        default="application/octet-stream",
        help="used when --op put",
    )

    args = parser.parse_args()

    side = args.side.upper()
    args.bucket = args.bucket or os.getenv(env_of(side, "BUCKET"))
    args.region = args.region or os.getenv(env_of(side, "REGION"))
    args.endpoint = args.endpoint or os.getenv(env_of(side, "S3_ENDPOINT"))
    args.addressing_style = args.addressing_style or os.getenv(env_of(side, "S3_ADDRESSING_STYLE"), "auto")
    args.ak = args.ak or os.getenv(env_of(side, "AWS_ACCESS_KEY_ID"))
    args.sk = args.sk or os.getenv(env_of(side, "AWS_SECRET_ACCESS_KEY"))
    args.token = args.token or os.getenv(env_of(side, "AWS_SESSION_TOKEN"))
    args.profile = args.profile or os.getenv(env_of(side, "AWS_PROFILE"))

    if not has_text(args.bucket):
        parser.error("bucket is required: --bucket or .env SRC_BUCKET/DST_BUCKET")
    if has_text(args.ak) != has_text(args.sk):
        parser.error("AK/SK must be provided together")
    if args.expires < 1:
        parser.error("--expires must be >= 1")
    if args.op in {"get", "put", "head"} and not has_text(args.key):
        parser.error("--key is required for get/put/head")
    if args.max_keys < 1:
        parser.error("--max-keys must be >= 1")
    return args


def main() -> int:
    args = parse_args()
    client = build_client(
        region=args.region,
        endpoint_url=args.endpoint,
        addressing_style=args.addressing_style,
        access_key_id=args.ak,
        secret_access_key=args.sk,
        session_token=args.token,
        profile=args.profile,
    )

    if args.op == "get":
        method = "get_object"
        params = {"Bucket": args.bucket, "Key": args.key}
    elif args.op == "put":
        method = "put_object"
        params = {"Bucket": args.bucket, "Key": args.key, "ContentType": args.content_type}
    elif args.op == "head":
        method = "head_object"
        params = {"Bucket": args.bucket, "Key": args.key}
    else:
        method = "list_objects_v2"
        params = {"Bucket": args.bucket, "MaxKeys": args.max_keys}
        if has_text(args.prefix):
            params["Prefix"] = args.prefix
        if has_text(args.delimiter):
            params["Delimiter"] = args.delimiter

    url = client.generate_presigned_url(
        ClientMethod=method,
        Params=params,
        ExpiresIn=args.expires,
    )

    print(f"side={args.side}")
    print(f"operation={args.op}")
    print(f"bucket={args.bucket}")
    if has_text(args.key):
        print(f"key={args.key}")
    if has_text(args.region):
        print(f"region={args.region}")
    if has_text(args.endpoint):
        print(f"endpoint={args.endpoint}")
    print(f"addressing_style={args.addressing_style}")
    print(f"expires_seconds={args.expires}")
    print("")
    print(url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
