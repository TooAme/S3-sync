#!/usr/bin/env python3
"""
Validate AWS credentials and optional S3 bucket access for source/destination.

Examples:
  python3 test_aws_key.py --which src
  python3 test_aws_key.py --which dst
  python3 test_aws_key.py --which both --list 3
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import date, datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


RETRY_CONFIG = Config(retries={"max_attempts": 8, "mode": "adaptive"})
LOGGER = logging.getLogger("aws-key-test")


def runtime_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def default_env_file() -> str:
    return str(runtime_base_dir() / ".env")


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


def get_env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = raw.strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def mask_access_key(value: str | None) -> str:
    if not has_text(value):
        return "-"
    text = value.strip()
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def pretty_json(value: Any) -> str:
    return json.dumps(to_jsonable(value), ensure_ascii=False, indent=2, sort_keys=True)


def build_client_config(addressing_style: str) -> Config:
    if addressing_style in {"path", "virtual"}:
        return Config(
            retries={"max_attempts": 8, "mode": "adaptive"},
            s3={"addressing_style": addressing_style},
        )
    return RETRY_CONFIG


def setup_logging(
    *,
    level_name: str,
    log_file: str | None,
    debug_botocore: bool,
) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if has_text(log_file):
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )
    if debug_botocore:
        logging.getLogger("botocore").setLevel(logging.DEBUG)
        logging.getLogger("boto3").setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)


def log_api_response(label: str, response: Any, enabled: bool) -> None:
    if not enabled:
        return
    LOGGER.info("%s full response:\n%s", label, pretty_json(response))


def choose_credential_mode(access_key_id: str | None, secret_access_key: str | None, profile: str | None) -> str:
    if has_text(access_key_id) and has_text(secret_access_key):
        return "AK/SK"
    if has_text(profile):
        return f"profile:{profile}"
    return "default-credential-chain"


def parse_args() -> argparse.Namespace:
    preload_parser = argparse.ArgumentParser(add_help=False)
    preload_parser.add_argument(
        "-e",
        "--env-file",
        default=default_env_file(),
        help="Path to .env file (default: .env next to executable/script)",
    )
    preload_args, _ = preload_parser.parse_known_args()
    load_dotenv(preload_args.env_file, override=True)

    parser = argparse.ArgumentParser(
        parents=[preload_parser],
        description="Validate source/destination S3 credentials and bucket access.",
    )
    parser.add_argument("-w", "--which", choices=["src", "dst", "both"], default="both")
    parser.add_argument("-p", "--prefix", default=os.getenv("PREFIX", ""))
    parser.add_argument("-l", "--list", "--list-objects", dest="list_objects", type=int, default=0, help="List up to N keys")

    args = parser.parse_args()

    args.log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    args.log_file = os.getenv("LOG_FILE")
    args.debug_botocore = get_env_bool("DEBUG_BOTOCORE", False)
    args.full_response = get_env_bool("FULL_RESPONSE", True)
    args.traceback_on_error = get_env_bool("TRACEBACK_ON_ERROR", False)

    args.src_ak = os.getenv("SRC_AWS_ACCESS_KEY_ID")
    args.src_sk = os.getenv("SRC_AWS_SECRET_ACCESS_KEY")
    args.src_token = os.getenv("SRC_AWS_SESSION_TOKEN")
    args.src_profile = os.getenv("SRC_AWS_PROFILE")
    args.src_region = os.getenv("SRC_REGION")
    args.src_bucket = os.getenv("SRC_BUCKET")
    args.src_endpoint = os.getenv("SRC_S3_ENDPOINT")
    args.src_addressing_style = (os.getenv("SRC_S3_ADDRESSING_STYLE", "auto").strip().lower() or "auto")

    args.dst_ak = os.getenv("DST_AWS_ACCESS_KEY_ID")
    args.dst_sk = os.getenv("DST_AWS_SECRET_ACCESS_KEY")
    args.dst_token = os.getenv("DST_AWS_SESSION_TOKEN")
    args.dst_profile = os.getenv("DST_AWS_PROFILE")
    args.dst_region = os.getenv("DST_REGION")
    args.dst_bucket = os.getenv("DST_BUCKET")
    args.dst_endpoint = os.getenv("DST_S3_ENDPOINT")
    args.dst_addressing_style = (os.getenv("DST_S3_ADDRESSING_STYLE", "auto").strip().lower() or "auto")

    if has_text(args.src_ak) != has_text(args.src_sk):
        parser.error("source AK/SK must be provided together in .env")
    if has_text(args.dst_ak) != has_text(args.dst_sk):
        parser.error("destination AK/SK must be provided together in .env")
    if args.list_objects < 0:
        parser.error("--list must be >= 0")
    if args.src_addressing_style not in {"auto", "path", "virtual"}:
        parser.error("SRC_S3_ADDRESSING_STYLE must be one of: auto, path, virtual")
    if args.dst_addressing_style not in {"auto", "path", "virtual"}:
        parser.error("DST_S3_ADDRESSING_STYLE must be one of: auto, path, virtual")
    return args


def build_session(
    *,
    region: str | None,
    endpoint_url: str | None,
    access_key_id: str | None,
    secret_access_key: str | None,
    session_token: str | None,
    profile: str | None,
) -> boto3.Session:
    resolved_region = region or ("us-east-1" if has_text(endpoint_url) else None)
    if has_text(access_key_id) and has_text(secret_access_key):
        return boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
            region_name=resolved_region,
        )
    if has_text(profile):
        return boto3.Session(profile_name=profile, region_name=resolved_region)
    return boto3.Session(region_name=resolved_region)


def build_s3_client(
    session: boto3.Session,
    *,
    endpoint_url: str | None,
    addressing_style: str,
):
    kwargs: dict[str, Any] = {"config": build_client_config(addressing_style)}
    if has_text(endpoint_url):
        kwargs["endpoint_url"] = endpoint_url
    return session.client("s3", **kwargs)


def classify_client_error(exc: ClientError) -> str:
    code = exc.response.get("Error", {}).get("Code", "")
    if code == "InvalidAccessKeyId":
        return (
            "InvalidAccessKeyId: key id is not recognized. "
            "Check if key is copied correctly, not disabled/deleted, and partition is correct "
            "(aws vs aws-cn)."
        )
    if code == "SignatureDoesNotMatch":
        return (
            "SignatureDoesNotMatch: secret key or session token is wrong, or request signed for wrong partition/region."
        )
    if code in {"ExpiredToken", "RequestExpired"}:
        return "Token expired: refresh temporary credentials."
    if code in {"AccessDenied", "AllAccessDisabled"}:
        return (
            "Access denied: key may be valid but missing permission on this API/bucket. "
            "For ListObjectsV2/HeadBucket, policy usually also needs bucket ARN "
            "(arn:aws:s3:::bucket) in addition to object ARN (arn:aws:s3:::bucket/*)."
        )
    return f"{code}: {exc.response.get('Error', {}).get('Message', str(exc))}"


def log_client_error(context: str, exc: ClientError, *, traceback_on_error: bool) -> None:
    LOGGER.error("%s failed: %s", context, classify_client_error(exc))
    LOGGER.error("%s error response:\n%s", context, pretty_json(exc.response))
    if traceback_on_error:
        LOGGER.error("Traceback:\n%s", traceback.format_exc())


@dataclass(frozen=True)
class Target:
    name: str
    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None
    profile: str | None
    region: str | None
    endpoint: str | None
    addressing_style: str
    bucket: str | None


def verify_target(
    target: Target,
    prefix: str,
    list_objects: int,
    *,
    full_response: bool,
    traceback_on_error: bool,
) -> bool:
    LOGGER.info("=== [%s] ===", target.name)
    LOGGER.info(
        "Credential mode: %s",
        choose_credential_mode(target.access_key_id, target.secret_access_key, target.profile),
    )
    if has_text(target.access_key_id):
        LOGGER.info("Configured access key: %s", mask_access_key(target.access_key_id))
    if has_text(target.region):
        LOGGER.info("Region: %s", target.region)
    else:
        LOGGER.info("Region: <not set>")
    LOGGER.info("S3 endpoint: %s", target.endpoint or "<aws-default>")
    LOGGER.info("S3 addressing style: %s", target.addressing_style)

    session = build_session(
        region=target.region,
        endpoint_url=target.endpoint,
        access_key_id=target.access_key_id,
        secret_access_key=target.secret_access_key,
        session_token=target.session_token,
        profile=target.profile,
    )

    resolved = session.get_credentials()
    resolved_ak = None
    resolved_method = "<unknown>"
    if resolved is not None:
        frozen = resolved.get_frozen_credentials()
        resolved_ak = frozen.access_key
        resolved_method = getattr(resolved, "method", "<unknown>")
    LOGGER.info("Resolved access key: %s", mask_access_key(resolved_ak))
    LOGGER.info("Credential provider method: %s", resolved_method)

    if has_text(target.endpoint):
        LOGGER.info("Custom endpoint detected, STS check skipped (typical for MinIO).")
    else:
        try:
            sts = session.client("sts", config=RETRY_CONFIG)
            identity = sts.get_caller_identity()
            LOGGER.info("STS OK: account=%s arn=%s", identity.get("Account"), identity.get("Arn"))
            log_api_response("sts.get_caller_identity", identity, full_response)
        except ClientError as exc:
            log_client_error("sts.get_caller_identity", exc, traceback_on_error=traceback_on_error)
            return False
        except Exception as exc:
            LOGGER.error("sts.get_caller_identity failed: %s", exc)
            if traceback_on_error:
                LOGGER.error("Traceback:\n%s", traceback.format_exc())
            return False

    if not has_text(target.bucket):
        LOGGER.info("Bucket check skipped: bucket not provided")
        return True

    s3 = build_s3_client(
        session,
        endpoint_url=target.endpoint,
        addressing_style=target.addressing_style,
    )
    try:
        location_resp = s3.get_bucket_location(Bucket=target.bucket)
        LOGGER.info("GetBucketLocation OK: %s", target.bucket)
        log_api_response("s3.get_bucket_location", location_resp, full_response)
    except ClientError as exc:
        LOGGER.warning("s3.get_bucket_location not available: %s", classify_client_error(exc))
        log_api_response("s3.get_bucket_location error_response", exc.response, full_response)
    except Exception as exc:
        LOGGER.warning("s3.get_bucket_location failed: %s", exc)
        if traceback_on_error:
            LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    try:
        head_resp = s3.head_bucket(Bucket=target.bucket)
        LOGGER.info("HeadBucket OK: %s", target.bucket)
        log_api_response("s3.head_bucket", head_resp, full_response)
    except ClientError as exc:
        log_client_error("s3.head_bucket", exc, traceback_on_error=traceback_on_error)
        return False
    except Exception as exc:
        LOGGER.error("s3.head_bucket failed: %s", exc)
        if traceback_on_error:
            LOGGER.error("Traceback:\n%s", traceback.format_exc())
        return False

    if list_objects > 0:
        try:
            resp = s3.list_objects_v2(Bucket=target.bucket, Prefix=prefix, MaxKeys=list_objects)
            keys = [item["Key"] for item in resp.get("Contents", [])]
            LOGGER.info("ListObjectsV2 OK: returned=%s (prefix=%s)", len(keys), prefix or "<ALL>")
            for key in keys:
                LOGGER.info("Object key: %s", key)
            log_api_response("s3.list_objects_v2", resp, full_response)
        except ClientError as exc:
            log_client_error("s3.list_objects_v2", exc, traceback_on_error=traceback_on_error)
            return False
        except Exception as exc:
            LOGGER.error("s3.list_objects_v2 failed: %s", exc)
            if traceback_on_error:
                LOGGER.error("Traceback:\n%s", traceback.format_exc())
            return False

    return True


def main() -> int:
    args = parse_args()
    setup_logging(
        level_name=args.log_level,
        log_file=args.log_file,
        debug_botocore=args.debug_botocore,
    )
    LOGGER.info(
        "Start key validation: which=%s prefix=%s list_objects=%s full_response=%s",
        args.which,
        args.prefix or "<ALL>",
        args.list_objects,
        args.full_response,
    )
    targets: list[Target] = []

    if args.which in {"src", "both"}:
        targets.append(
            Target(
                name="source",
                access_key_id=args.src_ak,
                secret_access_key=args.src_sk,
                session_token=args.src_token,
                profile=args.src_profile,
                region=args.src_region,
                endpoint=args.src_endpoint,
                addressing_style=args.src_addressing_style,
                bucket=args.src_bucket,
            )
        )

    if args.which in {"dst", "both"}:
        targets.append(
            Target(
                name="destination",
                access_key_id=args.dst_ak,
                secret_access_key=args.dst_sk,
                session_token=args.dst_token,
                profile=args.dst_profile,
                region=args.dst_region,
                endpoint=args.dst_endpoint,
                addressing_style=args.dst_addressing_style,
                bucket=args.dst_bucket,
            )
        )

    ok = True
    for target in targets:
        if not verify_target(
            target,
            args.prefix,
            args.list_objects,
            full_response=args.full_response,
            traceback_on_error=args.traceback_on_error,
        ):
            ok = False

    if ok:
        LOGGER.info("All checks passed.")
        return 0

    LOGGER.error("Some checks failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
