#!/usr/bin/env python3
"""
Sync objects from a source S3 bucket to a destination S3 bucket.

Each side supports credential modes with this priority:
  1) Access key pair (AK/SK[/token])
  2) AWS profile
  3) Default credential chain (role/instance metadata/env)
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import json
import logging
import os
import sys
import traceback
from datetime import date, datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError


RETRY_CONFIG = Config(retries={"max_attempts": 10, "mode": "adaptive"})
LOGGER = logging.getLogger("s3-sync")


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
            # Support inline comments: KEY=value # comment
            value = value.split(" #", 1)[0].rstrip()

        if override:
            os.environ[key] = value
        else:
            os.environ.setdefault(key, value)
        loaded += 1

    return loaded


def get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


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


def has_text(value: str | None) -> bool:
    return value is not None and value.strip() != ""


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

    class_name = value.__class__.__name__
    if class_name == "StreamingBody":
        return "<StreamingBody>"
    return repr(value)


def pretty_json(value: Any) -> str:
    return json.dumps(to_jsonable(value), ensure_ascii=False, indent=2, sort_keys=True)


def build_client_config(addressing_style: str) -> Config:
    if addressing_style in {"path", "virtual"}:
        return Config(
            retries={"max_attempts": 10, "mode": "adaptive"},
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


def log_client_error(context: str, exc: ClientError, *, traceback_on_error: bool) -> None:
    code = exc.response.get("Error", {}).get("Code", "Unknown")
    message = exc.response.get("Error", {}).get("Message", str(exc))
    LOGGER.error("%s failed: %s - %s", context, code, message)
    LOGGER.error("%s error response:\n%s", context, pretty_json(exc.response))
    if traceback_on_error:
        LOGGER.error("Traceback:\n%s", traceback.format_exc())


@dataclass(frozen=True)
class S3ObjectInfo:
    key: str
    size: int
    etag: str


def parse_args() -> argparse.Namespace:
    preload_parser = argparse.ArgumentParser(add_help=False)
    preload_parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file (default: .env in current folder)",
    )
    preload_args, _ = preload_parser.parse_known_args()
    load_dotenv(preload_args.env_file, override=True)

    parser = argparse.ArgumentParser(
        parents=[preload_parser],
        description=(
            "Cross-account S3 sync with flexible credentials for source and destination "
            "(AK/SK, profile, or default credential chain)."
        )
    )
    parser.add_argument(
        "--src-bucket",
        default=os.getenv("SRC_BUCKET"),
        help="Source (prod) bucket name (default: env SRC_BUCKET)",
    )
    parser.add_argument(
        "--dst-bucket",
        default=os.getenv("DST_BUCKET"),
        help="Destination (test) bucket name (default: env DST_BUCKET)",
    )
    parser.add_argument(
        "--src-region",
        default=os.getenv("SRC_REGION"),
        help="Source bucket region (default: env SRC_REGION)",
    )
    parser.add_argument(
        "--dst-region",
        default=os.getenv("DST_REGION"),
        help="Destination bucket region (default: env DST_REGION)",
    )
    parser.add_argument(
        "--src-endpoint",
        default=os.getenv("SRC_S3_ENDPOINT"),
        help="Source custom S3 endpoint, for example http://minio.example.com:9000",
    )
    parser.add_argument(
        "--dst-endpoint",
        default=os.getenv("DST_S3_ENDPOINT"),
        help="Destination custom S3 endpoint, for example http://minio.example.com:9000",
    )
    parser.add_argument(
        "--src-addressing-style",
        default=os.getenv("SRC_S3_ADDRESSING_STYLE", "auto"),
        choices=["auto", "path", "virtual"],
        help="Source S3 addressing style (default: env SRC_S3_ADDRESSING_STYLE or auto)",
    )
    parser.add_argument(
        "--dst-addressing-style",
        default=os.getenv("DST_S3_ADDRESSING_STYLE", "auto"),
        choices=["auto", "path", "virtual"],
        help="Destination S3 addressing style (default: env DST_S3_ADDRESSING_STYLE or auto)",
    )
    parser.add_argument(
        "--prefix",
        default=os.getenv("PREFIX", ""),
        help="Only sync this prefix (default: env PREFIX or whole bucket)",
    )
    parser.add_argument(
        "--exact-key",
        default=os.getenv("EXACT_KEY"),
        help="Sync only this exact object key (default: env EXACT_KEY)",
    )
    parser.add_argument(
        "--src-ak",
        default=os.getenv("SRC_AWS_ACCESS_KEY_ID"),
        help="Source AWS access key id (default: env SRC_AWS_ACCESS_KEY_ID)",
    )
    parser.add_argument(
        "--src-sk",
        default=os.getenv("SRC_AWS_SECRET_ACCESS_KEY"),
        help="Source AWS secret access key (default: env SRC_AWS_SECRET_ACCESS_KEY)",
    )
    parser.add_argument(
        "--src-token",
        default=os.getenv("SRC_AWS_SESSION_TOKEN"),
        help="Optional source AWS session token (default: env SRC_AWS_SESSION_TOKEN)",
    )
    parser.add_argument(
        "--src-profile",
        default=os.getenv("SRC_AWS_PROFILE"),
        help="Optional source AWS profile (default: env SRC_AWS_PROFILE)",
    )
    parser.add_argument(
        "--dst-ak",
        default=os.getenv("DST_AWS_ACCESS_KEY_ID"),
        help="Destination AWS access key id (default: env DST_AWS_ACCESS_KEY_ID)",
    )
    parser.add_argument(
        "--dst-sk",
        default=os.getenv("DST_AWS_SECRET_ACCESS_KEY"),
        help="Destination AWS secret access key (default: env DST_AWS_SECRET_ACCESS_KEY)",
    )
    parser.add_argument(
        "--dst-token",
        default=os.getenv("DST_AWS_SESSION_TOKEN"),
        help="Optional destination AWS session token (default: env DST_AWS_SESSION_TOKEN)",
    )
    parser.add_argument(
        "--dst-profile",
        default=os.getenv("DST_AWS_PROFILE"),
        help="Optional destination AWS profile (default: env DST_AWS_PROFILE)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=get_env_int("WORKERS", 8),
        help="Concurrent workers for object copy (default: env WORKERS or 8)",
    )
    parser.add_argument(
        "--multipart-threshold-mb",
        type=int,
        default=get_env_int("MULTIPART_THRESHOLD_MB", 64),
        help="Multipart threshold in MB (default: env MULTIPART_THRESHOLD_MB or 64)",
    )
    parser.add_argument(
        "--multipart-chunk-mb",
        type=int,
        default=get_env_int("MULTIPART_CHUNK_MB", 16),
        help="Multipart chunk size in MB (default: env MULTIPART_CHUNK_MB or 16)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Always overwrite destination objects",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete destination objects that do not exist in source",
    )
    parser.add_argument(
        "--copy-tags",
        action="store_true",
        help="Also copy S3 object tags",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions only, do not upload/delete",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=get_env_int("PROGRESS_EVERY", 200),
        help="Print progress every N source objects (default: env PROGRESS_EVERY or 200)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO").upper(),
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level (default: env LOG_LEVEL or INFO)",
    )
    parser.add_argument(
        "--log-file",
        default=os.getenv("LOG_FILE"),
        help="Optional log file path (default: env LOG_FILE)",
    )
    parser.add_argument(
        "--debug-botocore",
        action="store_true",
        default=get_env_bool("DEBUG_BOTOCORE", False),
        help="Enable boto3/botocore/urllib3 debug logging",
    )
    parser.add_argument(
        "--full-response",
        dest="full_response",
        action="store_true",
        help="Print full AWS API responses in logs",
    )
    parser.add_argument(
        "--no-full-response",
        dest="full_response",
        action="store_false",
        help="Do not print full AWS API responses in logs",
    )
    parser.add_argument(
        "--traceback-on-error",
        action="store_true",
        default=get_env_bool("TRACEBACK_ON_ERROR", False),
        help="Print full traceback on exceptions",
    )
    parser.set_defaults(full_response=get_env_bool("FULL_RESPONSE", False))
    args = parser.parse_args()

    if not args.src_bucket:
        parser.error("source bucket is required: --src-bucket or .env SRC_BUCKET")
    if not args.dst_bucket:
        parser.error("destination bucket is required: --dst-bucket or .env DST_BUCKET")
    if has_text(args.src_ak) != has_text(args.src_sk):
        parser.error(
            "source AK/SK must be provided together: "
            "--src-ak + --src-sk (or env SRC_AWS_ACCESS_KEY_ID + SRC_AWS_SECRET_ACCESS_KEY)"
        )
    if has_text(args.dst_ak) != has_text(args.dst_sk):
        parser.error(
            "destination AK/SK must be provided together: "
            "--dst-ak + --dst-sk (or env DST_AWS_ACCESS_KEY_ID + DST_AWS_SECRET_ACCESS_KEY)"
        )
    if args.src_bucket == args.dst_bucket:
        parser.error("--src-bucket and --dst-bucket cannot be the same")
    if has_text(args.exact_key) and has_text(args.prefix):
        parser.error("--exact-key and --prefix cannot be used together")
    if has_text(args.exact_key) and args.delete:
        parser.error("--delete cannot be used with --exact-key")
    if args.workers < 1:
        parser.error("--workers must be >= 1")
    if args.multipart_threshold_mb < 5 or args.multipart_chunk_mb < 5:
        parser.error("multipart values must be >= 5 MB")

    return args


def strip_quotes(value: str) -> str:
    return value.strip('"') if value else ""


def build_s3_client(
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
    client_config = build_client_config(addressing_style)
    client_kwargs: Dict[str, Any] = {"config": client_config}
    if has_text(endpoint_url):
        client_kwargs["endpoint_url"] = endpoint_url

    if has_text(access_key_id) and has_text(secret_access_key):
        session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
            region_name=resolved_region,
        )
        return session.client("s3", **client_kwargs)

    if has_text(profile):
        session = boto3.Session(profile_name=profile, region_name=resolved_region)
        return session.client("s3", **client_kwargs)

    session = boto3.Session(region_name=resolved_region)
    return session.client("s3", **client_kwargs)


def build_source_client(args: argparse.Namespace):
    return build_s3_client(
        region=args.src_region,
        endpoint_url=args.src_endpoint,
        addressing_style=args.src_addressing_style,
        access_key_id=args.src_ak,
        secret_access_key=args.src_sk,
        session_token=args.src_token,
        profile=args.src_profile,
    )


def build_destination_client(args: argparse.Namespace):
    return build_s3_client(
        region=args.dst_region,
        endpoint_url=args.dst_endpoint,
        addressing_style=args.dst_addressing_style,
        access_key_id=args.dst_ak,
        secret_access_key=args.dst_sk,
        session_token=args.dst_token,
        profile=args.dst_profile,
    )


def credential_mode(access_key_id: str | None, secret_access_key: str | None, profile: str | None) -> str:
    if has_text(access_key_id) and has_text(secret_access_key):
        return "AK/SK"
    if has_text(profile):
        return f"profile:{profile}"
    return "default-credential-chain"


def list_bucket_objects(
    client,
    bucket: str,
    prefix: str,
    *,
    full_response: bool,
) -> List[S3ObjectInfo]:
    objects: List[S3ObjectInfo] = []
    paginator = client.get_paginator("list_objects_v2")
    for page_no, page in enumerate(paginator.paginate(Bucket=bucket, Prefix=prefix), start=1):
        LOGGER.debug(
            "list_objects_v2 page=%s key_count=%s is_truncated=%s next_token=%s",
            page_no,
            page.get("KeyCount"),
            page.get("IsTruncated"),
            page.get("NextContinuationToken"),
        )
        log_api_response(f"source.list_objects_v2(page={page_no})", page, full_response)
        for obj in page.get("Contents", []):
            objects.append(
                S3ObjectInfo(
                    key=obj["Key"],
                    size=int(obj.get("Size", 0)),
                    etag=strip_quotes(obj.get("ETag", "")),
                )
            )
    LOGGER.info("Listed objects: bucket=%s prefix=%s total=%s", bucket, prefix or "<ALL>", len(objects))
    return objects


def get_single_object_info(
    client,
    bucket: str,
    key: str,
    *,
    full_response: bool,
    traceback_on_error: bool,
) -> S3ObjectInfo:
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        log_api_response(f"source.head_object(key={key})", head, full_response)
        obj = S3ObjectInfo(
            key=key,
            size=int(head.get("ContentLength", 0)),
            etag=strip_quotes(head.get("ETag", "")),
        )
        LOGGER.info("Resolved exact key: %s size=%s etag=%s", obj.key, obj.size, obj.etag)
        return obj
    except ClientError as exc:
        log_client_error(
            f"source.head_object key={key}",
            exc,
            traceback_on_error=traceback_on_error,
        )
        raise


def destination_needs_update(
    dst_client,
    dst_bucket: str,
    src_obj: S3ObjectInfo,
    *,
    full_response: bool,
    traceback_on_error: bool,
) -> bool:
    try:
        head = dst_client.head_object(Bucket=dst_bucket, Key=src_obj.key)
        log_api_response(f"destination.head_object(key={src_obj.key})", head, full_response)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            LOGGER.debug("destination.head_object key=%s not found, will copy", src_obj.key)
            return True
        log_client_error(f"destination.head_object key={src_obj.key}", exc, traceback_on_error=traceback_on_error)
        raise

    dst_size = int(head.get("ContentLength", -1))
    dst_etag = strip_quotes(head.get("ETag", ""))
    needs_update = not (dst_size == src_obj.size and dst_etag == src_obj.etag and src_obj.etag != "")
    LOGGER.debug(
        "compare key=%s src(size=%s etag=%s) dst(size=%s etag=%s) needs_update=%s",
        src_obj.key,
        src_obj.size,
        src_obj.etag,
        dst_size,
        dst_etag,
        needs_update,
    )
    return needs_update


def build_extra_args_from_source_get(get_resp: Dict) -> Dict:
    extra: Dict = {}
    passthrough_fields = (
        "ContentType",
        "ContentDisposition",
        "ContentEncoding",
        "ContentLanguage",
        "CacheControl",
        "Expires",
    )
    for field in passthrough_fields:
        value = get_resp.get(field)
        if value is not None:
            extra[field] = value

    metadata = get_resp.get("Metadata")
    if metadata:
        extra["Metadata"] = metadata
    return extra


def copy_one_object(
    src_client,
    dst_client,
    args: argparse.Namespace,
    transfer_cfg: TransferConfig,
    src_obj: S3ObjectInfo,
) -> str:
    LOGGER.debug("processing key=%s size=%s", src_obj.key, src_obj.size)
    if not args.force and not destination_needs_update(
        dst_client,
        args.dst_bucket,
        src_obj,
        full_response=args.full_response,
        traceback_on_error=args.traceback_on_error,
    ):
        return "skipped"

    if args.dry_run:
        LOGGER.info("[DRY-RUN] copy key=%s", src_obj.key)
        return "copy_dry_run"

    get_resp = src_client.get_object(Bucket=args.src_bucket, Key=src_obj.key)
    log_api_response(f"source.get_object(key={src_obj.key})", get_resp, args.full_response)
    extra_args = build_extra_args_from_source_get(get_resp)

    with contextlib.closing(get_resp["Body"]) as body:
        if extra_args:
            dst_client.upload_fileobj(
                Fileobj=body,
                Bucket=args.dst_bucket,
                Key=src_obj.key,
                ExtraArgs=extra_args,
                Config=transfer_cfg,
            )
        else:
            dst_client.upload_fileobj(
                Fileobj=body,
                Bucket=args.dst_bucket,
                Key=src_obj.key,
                Config=transfer_cfg,
            )
    LOGGER.debug("upload completed key=%s", src_obj.key)

    if args.copy_tags:
        src_tag_resp = src_client.get_object_tagging(Bucket=args.src_bucket, Key=src_obj.key)
        log_api_response(f"source.get_object_tagging(key={src_obj.key})", src_tag_resp, args.full_response)
        tag_set = src_tag_resp.get("TagSet", [])
        if tag_set:
            dst_tag_resp = dst_client.put_object_tagging(
                Bucket=args.dst_bucket,
                Key=src_obj.key,
                Tagging={"TagSet": tag_set},
            )
            log_api_response(f"destination.put_object_tagging(key={src_obj.key})", dst_tag_resp, args.full_response)

    return "copied"


def chunked(iterable: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def delete_extra_objects(
    dst_client,
    args: argparse.Namespace,
    source_keys: Set[str],
) -> int:
    paginator = dst_client.get_paginator("list_objects_v2")
    to_delete: List[str] = []

    for page_no, page in enumerate(paginator.paginate(Bucket=args.dst_bucket, Prefix=args.prefix), start=1):
        LOGGER.debug(
            "destination.list_objects_v2 page=%s key_count=%s is_truncated=%s next_token=%s",
            page_no,
            page.get("KeyCount"),
            page.get("IsTruncated"),
            page.get("NextContinuationToken"),
        )
        log_api_response(f"destination.list_objects_v2(page={page_no})", page, args.full_response)
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key not in source_keys:
                to_delete.append(key)

    if not to_delete:
        return 0

    if args.dry_run:
        LOGGER.info("[DRY-RUN] would delete %s objects from destination", len(to_delete))
        return len(to_delete)

    deleted = 0
    for keys in chunked(to_delete, 1000):
        resp = dst_client.delete_objects(
            Bucket=args.dst_bucket,
            Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
        )
        log_api_response("destination.delete_objects", resp, args.full_response)
        deleted += len(resp.get("Deleted", []))
    return deleted


def main() -> int:
    args = parse_args()

    setup_logging(
        level_name=args.log_level,
        log_file=args.log_file,
        debug_botocore=args.debug_botocore,
    )
    LOGGER.info(
        "Credential mode: source=%s destination=%s",
        credential_mode(args.src_ak, args.src_sk, args.src_profile),
        credential_mode(args.dst_ak, args.dst_sk, args.dst_profile),
    )
    if has_text(args.src_ak):
        LOGGER.info("Source access key in use: %s", mask_access_key(args.src_ak))
    if has_text(args.dst_ak):
        LOGGER.info("Destination access key in use: %s", mask_access_key(args.dst_ak))
    LOGGER.info(
        "Sync config: src_bucket=%s dst_bucket=%s src_region=%s dst_region=%s prefix=%s exact_key=%s workers=%s dry_run=%s",
        args.src_bucket,
        args.dst_bucket,
        args.src_region or "<auto>",
        args.dst_region or "<auto>",
        args.prefix or "<ALL>",
        args.exact_key or "<NONE>",
        args.workers,
        args.dry_run,
    )
    LOGGER.info(
        "S3 endpoint config: src_endpoint=%s dst_endpoint=%s src_style=%s dst_style=%s",
        args.src_endpoint or "<aws-default>",
        args.dst_endpoint or "<aws-default>",
        args.src_addressing_style,
        args.dst_addressing_style,
    )
    LOGGER.info("Building AWS clients...")
    try:
        src_client = build_source_client(args)
        dst_client = build_destination_client(args)
    except Exception as exc:
        LOGGER.error("Build clients failed: %s", exc)
        if args.traceback_on_error:
            LOGGER.error("Traceback:\n%s", traceback.format_exc())
        return 1

    transfer_cfg = TransferConfig(
        multipart_threshold=args.multipart_threshold_mb * 1024 * 1024,
        multipart_chunksize=args.multipart_chunk_mb * 1024 * 1024,
        use_threads=True,
    )

    try:
        if has_text(args.exact_key):
            LOGGER.info(
                "Source selection mode: exact-key bucket=%s key=%s",
                args.src_bucket,
                args.exact_key,
            )
            source_objects = [
                get_single_object_info(
                    src_client,
                    args.src_bucket,
                    args.exact_key,
                    full_response=args.full_response,
                    traceback_on_error=args.traceback_on_error,
                )
            ]
        else:
            LOGGER.info(
                "Source selection mode: prefix bucket=%s prefix=%s",
                args.src_bucket,
                args.prefix or "<ALL>",
            )
            source_objects = list_bucket_objects(
                src_client,
                args.src_bucket,
                args.prefix,
                full_response=args.full_response,
            )
    except ClientError as exc:
        operation = "source.head_object" if has_text(args.exact_key) else "source.list_objects_v2"
        log_client_error(
            f"{operation} bucket={args.src_bucket}",
            exc,
            traceback_on_error=args.traceback_on_error,
        )
        return 1
    except Exception as exc:
        LOGGER.error("Load source objects failed: %s", exc)
        if args.traceback_on_error:
            LOGGER.error("Traceback:\n%s", traceback.format_exc())
        return 1
    total = len(source_objects)
    LOGGER.info("Source objects found: %s", total)

    if total == 0:
        if args.delete:
            deleted = delete_extra_objects(dst_client, args, set())
            LOGGER.info("Delete step complete, objects deleted=%s", deleted)
        return 0

    copied = 0
    skipped = 0
    failed = 0

    LOGGER.info("Starting copy with workers=%s ...", args.workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_obj = {
            executor.submit(copy_one_object, src_client, dst_client, args, transfer_cfg, src_obj): src_obj
            for src_obj in source_objects
        }

        for index, future in enumerate(concurrent.futures.as_completed(future_to_obj), start=1):
            src_obj = future_to_obj[future]
            try:
                result = future.result()
            except ClientError as exc:
                failed += 1
                log_client_error(
                    f"copy key={src_obj.key}",
                    exc,
                    traceback_on_error=args.traceback_on_error,
                )
            except Exception as exc:
                failed += 1
                LOGGER.error("copy key=%s failed: %s", src_obj.key, exc)
                if args.traceback_on_error:
                    LOGGER.error("Traceback:\n%s", traceback.format_exc())
            else:
                if result == "copied":
                    copied += 1
                elif result == "skipped":
                    skipped += 1
                elif result == "copy_dry_run":
                    copied += 1

            if index % args.progress_every == 0 or index == total:
                LOGGER.info(
                    "Progress: %s/%s processed (copied=%s, skipped=%s, failed=%s)",
                    index,
                    total,
                    copied,
                    skipped,
                    failed,
                )

    deleted = 0
    if args.delete:
        LOGGER.info("Running delete phase (--delete enabled) ...")
        source_keys = {obj.key for obj in source_objects}
        deleted = delete_extra_objects(dst_client, args, source_keys)

    LOGGER.info(
        "Summary: source=%s, copied=%s, skipped=%s, failed=%s, deleted=%s",
        total,
        copied,
        skipped,
        failed,
        deleted,
    )

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
