"""Project configuration: locates the `project/` folder and loads project/config/pipeline.yaml.

Where the project folder comes from (first hit wins):
  1. env PROJECT_DIR
  2. a `project/` folder next to the engine package (local checkout: cdt/project)
  3. a project.zip on sys.path (AWS Glue --extra-py-files): extracted once to a temp folder
Environment: RUN_MODE (local|aws), DATA_ROOT (absolute local path or s3://bucket), WAREHOUSE.

Usage:  from engine import config;  config.settings()["as_of_date"];  config.tables();  config.PROJECT_DIR
"""
from __future__ import annotations

import os
import sys
import tempfile
import zipfile
from datetime import date
from pathlib import Path

import yaml

_ENGINE_DIR = Path(__file__).resolve().parent
_cache: dict[str, dict] = {}
_extracted: Path | None = None

DEFAULT_PLACEHOLDERS = ["", "n/a", "na", "null", "none", "nan", "unknown", "-", "--", "?", "nil"]


def _project_from_zip() -> Path | None:
    """Find project.zip (or any zip holding project/config/pipeline.yaml) on sys.path and extract it."""
    global _extracted
    if _extracted is not None:
        return _extracted
    for entry in sys.path:
        if not entry.endswith(".zip") or not os.path.isfile(entry):
            continue
        with zipfile.ZipFile(entry) as zf:
            names = set(zf.namelist())
            if "project/config/pipeline.yaml" in names:
                prefix = "project/"
            elif "config/pipeline.yaml" in names:
                prefix = ""
            else:
                continue
            target = Path(tempfile.mkdtemp(prefix="cdt_project_"))
            zf.extractall(target)
            _extracted = target / prefix if prefix else target
            return _extracted
    return None


def project_dir() -> Path:
    env = os.environ.get("PROJECT_DIR")
    if env:
        return Path(env).resolve()
    beside = _ENGINE_DIR.parent / "project"
    if (beside / "config" / "pipeline.yaml").exists():
        return beside
    found = _project_from_zip()
    if found:
        return found
    raise FileNotFoundError("project folder not found: set PROJECT_DIR or ship project.zip with --extra-py-files")


def data_root() -> str:
    """Absolute local path or s3://bucket[/prefix] (no trailing slash)."""
    root = os.environ.get("DATA_ROOT") or str(_ENGINE_DIR.parent / "data")
    if root.startswith("s3://") or root.startswith("s3a://"):
        return root.rstrip("/")
    return str(Path(root).resolve())


def run_mode() -> str:
    return os.environ.get("RUN_MODE", "local")


def config(reload: bool = False) -> dict:
    """Whole pipeline.yaml as a dict (cached per project folder)."""
    path = project_dir() / "config" / "pipeline.yaml"
    key = str(path)
    if reload or key not in _cache:
        with open(path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
        cfg.setdefault("settings", {})
        cfg.setdefault("mappings", {})
        cfg.setdefault("tables", {})
        _cache[key] = cfg
    return _cache[key]


load_config = config


def settings() -> dict:
    return config()["settings"]


def setting(name: str, default=None):
    value = settings().get(name)
    return default if value is None else value


def tables() -> dict:
    """{table_name: spec} in config order (parents first)."""
    return config()["tables"]


def mappings() -> dict:
    return config()["mappings"]


# ---------- SPEC v2 table features: file lists, lookups, derived, row_rules, expectations ----------
EXPECTATION_TYPES = {  # type -> keys it needs
    "not_null": ["column"], "unique": ["column"], "range": ["column"], "accepted_values": ["column", "values"],
    "exists_in": ["column", "table"], "row_count_min": ["value"], "condition": ["name", "condition"]}
ROW_RULE_ACTIONS = ("quarantine", "flag")


def file_patterns(table: str, spec: dict | None) -> list[str]:
    """`file:` is a glob string or a list of globs (all matching files load into the same table)."""
    value = (spec or {}).get("file", f"{table}.*")
    return [str(v) for v in value] if isinstance(value, (list, tuple)) else [str(value)]


def lookup_on(lk: dict) -> dict:
    """lookups[].on -> {child_column: parent_column}. Accepts a dict, one column name, or a list of names.
    Note: PyYAML (YAML 1.1) reads an unquoted key `on:` as boolean True, so both spellings are accepted."""
    on = lk.get("on", lk.get(True))
    if isinstance(on, str):
        return {on: on}
    if isinstance(on, (list, tuple)):
        return {str(c): str(c) for c in on}
    return {str(k): str(v) for k, v in (on or {}).items()}


def lookup_columns(lk: dict) -> list[tuple[str, str]]:
    """lookups[].columns -> [(parent_column, new_column_name)]. Items: 'sla_days' or {sla_days: courier_sla}."""
    out = []
    for item in lk.get("columns") or []:
        if isinstance(item, dict):
            out += [(str(k), str(v)) for k, v in item.items()]
        else:
            out.append((str(item), str(item)))
    return out


def validate_table(table: str, spec: dict | None) -> None:
    """Fail fast (before any data is touched) with a clear message when the v2 sections are mis-written."""
    spec = spec or {}
    where = f"pipeline.yaml tables.{table}"
    tables_ = tables()
    for i, lk in enumerate(spec.get("lookups") or []):
        if not isinstance(lk, dict) or lk.get("table") not in tables_:
            raise ValueError(f"{where}.lookups[{i}]: 'table' must name a table defined in pipeline.yaml")
        if not lookup_on(lk) or not lookup_columns(lk):
            raise ValueError(f"{where}.lookups[{i}]: needs 'on' ({{child_column: parent_column}}) and 'columns'")
    for name, d in (spec.get("derived") or {}).items():
        if not isinstance(d, dict) or not d.get("expr"):
            raise ValueError(f"{where}.derived.{name}: needs {{expr: <Spark SQL expression>, type: <type>}}")
    seen = set()
    for i, r in enumerate(spec.get("row_rules") or []):
        if not isinstance(r, dict) or not r.get("id") or not r.get("when"):
            raise ValueError(f"{where}.row_rules[{i}]: needs id, when, action, reason")
        if r.get("action", "quarantine") not in ROW_RULE_ACTIONS:
            raise ValueError(f"{where}.row_rules[{i}] ({r['id']}): action must be one of {ROW_RULE_ACTIONS}")
        if r["id"] in seen:
            raise ValueError(f"{where}.row_rules: id '{r['id']}' is used twice")
        seen.add(r["id"])
    for i, e in enumerate(spec.get("expectations") or []):
        typ = (e or {}).get("type")
        if typ not in EXPECTATION_TYPES:
            raise ValueError(f"{where}.expectations[{i}]: type must be one of {sorted(EXPECTATION_TYPES)}")
        missing = [k for k in EXPECTATION_TYPES[typ] if e.get(k) is None]
        if typ == "range" and e.get("min") is None and e.get("max") is None:
            missing.append("min/max")
        if missing:
            raise ValueError(f"{where}.expectations[{i}] ({typ}): missing {missing}")
        if typ == "exists_in" and e["table"] not in tables_:
            raise ValueError(f"{where}.expectations[{i}] (exists_in): table '{e['table']}' is not defined")
        if e.get("severity", "error") not in ("error", "warn"):
            raise ValueError(f"{where}.expectations[{i}]: severity must be error or warn")


def validate() -> None:
    for table, spec in tables().items():
        validate_table(table, spec)


def placeholders() -> list[str]:
    return [str(p).strip().lower() for p in setting("placeholders", DEFAULT_PLACEHOLDERS)]


def as_of_date() -> date:
    value = setting("as_of_date")
    if value is None:
        return date.today()
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def batch_date(batch_id: str) -> date | None:
    """Business date of a batch from settings.batch_dates (None if not configured)."""
    value = (setting("batch_dates", {}) or {}).get(batch_id)
    if value is None:
        return None
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def job_args(required: list[str] = ("batch_id",),
             optional: list[str] = ("JOB_NAME", "run_id", "data_root", "project_dir")) -> dict:
    """Job arguments. Glue: getResolvedOptions (optional ones only when passed). Local: argparse --name value.
    data_root / project_dir, when given, are exported as DATA_ROOT / PROJECT_DIR. run_id defaults to a new id."""
    import uuid
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore
        present = [n for n in optional if f"--{n}" in sys.argv]
        args = dict(getResolvedOptions(sys.argv, list(required) + present))
    except ImportError:
        import argparse
        p = argparse.ArgumentParser()
        for n in list(required) + list(optional):
            p.add_argument(f"--{n}", required=n in required)
        args = {k: v for k, v in vars(p.parse_known_args()[0]).items() if v is not None}
    # Glue passes settings as job arguments, not env vars: every "--UPPER_CASE value" pair becomes an env var
    # (e.g. --WAREHOUSE redshift --REDSHIFT_WORKGROUP wg); an env var that is already set wins.
    argv = sys.argv[1:]
    for i, tok in enumerate(argv[:-1]):
        name = tok[2:] if tok.startswith("--") else ""
        if name and name.replace("_", "").isalnum() and name.isupper() and not argv[i + 1].startswith("--"):
            os.environ.setdefault(name, argv[i + 1])
    if args.get("data_root"):
        os.environ["DATA_ROOT"] = args["data_root"]
    if args.get("project_dir"):
        os.environ["PROJECT_DIR"] = args["project_dir"]
    args.setdefault("run_id", uuid.uuid4().hex[:12])
    return args


def __getattr__(name: str):  # lazy module attributes: config.PROJECT_DIR, config.DATA_ROOT, ...
    if name == "PROJECT_DIR":
        return project_dir()
    if name == "DATA_ROOT":
        return data_root()
    if name == "RUN_MODE":
        return run_mode()
    if name == "WAREHOUSE":
        return os.environ.get("WAREHOUSE", "postgres")
    raise AttributeError(name)
