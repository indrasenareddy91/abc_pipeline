"""Storage helpers for DATA_ROOT, which is either an absolute local folder or s3://bucket[/prefix].

All functions take paths RELATIVE to DATA_ROOT (e.g. "raw/day1"). `path()` gives the full path for Spark
(plain local path, or s3://... which Glue reads natively)."""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from . import config


def _is_s3(root: str) -> bool:
    return root.startswith("s3://") or root.startswith("s3a://")


def _split(root: str, rel: str) -> tuple[str, str]:
    """-> (bucket, key) for an s3 root + relative path."""
    no_scheme = root.split("://", 1)[1]
    bucket, _, prefix = no_scheme.partition("/")
    key = "/".join(p for p in (prefix.strip("/"), rel.strip("/")) if p)
    return bucket, key


def _s3():
    import boto3  # only needed on AWS
    return boto3.client("s3")


def path(*parts: str) -> str:
    """Full path for Spark readers/writers."""
    root = config.data_root()
    rel = "/".join(str(p).strip("/") for p in parts if str(p).strip("/"))
    return f"{root}/{rel}" if rel else root


def list_files(prefix: str) -> list[str]:
    """File names (not folders) directly under prefix, sorted. Hidden/underscore files are skipped."""
    root = config.data_root()
    if _is_s3(root):
        bucket, key = _split(root, prefix)
        key = key.rstrip("/") + "/"
        names = []
        for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key, Delimiter="/"):
            names += [o["Key"][len(key):] for o in page.get("Contents", [])]
    else:
        folder = Path(root) / prefix
        names = [p.name for p in folder.iterdir() if p.is_file()] if folder.is_dir() else []
    return sorted(n for n in names if n and not n.startswith((".", "_")))


def exists(prefix: str) -> bool:
    """True if anything exists at/under prefix."""
    root = config.data_root()
    if _is_s3(root):
        bucket, key = _split(root, prefix)
        resp = _s3().list_objects_v2(Bucket=bucket, Prefix=key.rstrip("/") + "/", MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    return (Path(root) / prefix).exists()


def read_bytes(rel: str) -> bytes:
    root = config.data_root()
    if _is_s3(root):
        bucket, key = _split(root, rel)
        return _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    return (Path(root) / rel).read_bytes()


def write_text(rel: str, text: str) -> None:
    root = config.data_root()
    if _is_s3(root):
        bucket, key = _split(root, rel)
        _s3().put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))
        return
    target = Path(root) / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def delete(prefix: str) -> None:
    """Remove a folder/prefix and everything under it (used to clear a stale batch partition)."""
    root = config.data_root()
    if _is_s3(root):
        bucket, key = _split(root, prefix)
        s3 = _s3()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key.rstrip("/") + "/"):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
        return
    target = Path(root) / prefix
    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        os.remove(target)


def list_partitions(prefix: str, name: str) -> list[str]:
    """Values of `name=<value>` sub-folders under prefix, sorted (e.g. batch ids of silver_changes)."""
    root = config.data_root()
    tag = f"{name}="
    if _is_s3(root):
        bucket, key = _split(root, prefix)
        key = key.rstrip("/") + "/"
        dirs = []
        for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key, Delimiter="/"):
            dirs += [p["Prefix"][len(key):].rstrip("/") for p in page.get("CommonPrefixes", [])]
    else:
        folder = Path(root) / prefix
        dirs = [p.name for p in folder.iterdir() if p.is_dir()] if folder.is_dir() else []
    return sorted(d[len(tag):] for d in dirs if d.startswith(tag))
