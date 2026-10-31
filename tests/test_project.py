"""The 5 tests that matter on hackathon day: they run on the ACTIVE project (project/ once it has tables and sample
data, else examples/shopclean; override with PROJECT_DIR) through the real pipeline.
    python -m pytest -q tests/test_project.py
1. every batch runs (data checks and warehouse checks fail the run on bad data)
2. row counts reconcile: raw = exact duplicates + merged versions + quarantined + accepted
3. business keys are unique in the clean (silver) tables
4. rerunning a batch changes nothing (idempotent, no duplicate rows)
5. every planted problem in ISSUES.csv was handled (only for generated data)"""
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def _project() -> Path:
    if os.environ.get("PROJECT_DIR"):
        return Path(os.environ["PROJECT_DIR"])
    p = ROOT / "project"
    cfg = yaml.safe_load((p / "config" / "pipeline.yaml").read_text()) or {}
    return p if cfg.get("tables") and list((p / "sample_data" / "raw").glob("*/*")) else ROOT / "examples" / "shopclean"


PROJ = _project()
CFG = yaml.safe_load((PROJ / "config" / "pipeline.yaml").read_text())
BATCHES = sorted(d.name for d in (PROJ / "sample_data" / "raw").iterdir() if d.is_dir())
STEPS = "bronze,silver,gold" if (PROJ / "sql" / "02_load").exists() else "bronze,silver"


def _run(batch: str, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(ROOT / "jobs" / "run_local.py"), "--batch", batch, "--steps", STEPS],
                          env=env, capture_output=True, text=True, timeout=900)


def _silver(root: Path) -> dict:
    return {t: pd.read_parquet(root / "silver" / t) for t in CFG["tables"] if (root / "silver" / t).exists()}


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    root = tmp_path_factory.mktemp("project_data")
    env = {**os.environ, "PROJECT_DIR": str(PROJ), "DATA_ROOT": str(root)}
    results = {b: _run(b, env) for b in BATCHES}
    return root, env, results


def test_1_every_batch_runs(run):
    _, _, results = run
    for b, r in results.items():
        assert r.returncode == 0, f"batch {b} failed:\n{r.stdout[-3000:]}\n{r.stderr[-3000:]}"


def test_2_row_counts_reconcile(run):
    root, _, _ = run
    rc = pd.concat([pd.read_parquet(f) for f in (root / "ops" / "row_counts").rglob("*.parquet")])
    assert len(rc), "no row_counts written"
    for _, r in rc.iterrows():
        parts = r["exact_duplicates"] + r["key_duplicates"] + r["quarantined"] + r["accepted"]
        assert r["raw_rows"] == parts, f"{r['table']}: raw {r['raw_rows']} != {parts} (dupes+merged+quarantined+accepted)"


def test_3_keys_unique_in_silver(run):
    root, _, _ = run
    for t, df in _silver(root).items():
        key = CFG["tables"][t].get("key")
        if key:
            assert df[key].notna().all(), f"{t}.{key} has empty keys in silver"
            assert df[key].is_unique, f"{t}.{key} is not unique in silver"


def test_4_rerun_changes_nothing(run):
    root, env, _ = run
    before = {t: len(df) for t, df in _silver(root).items()}
    r = _run(BATCHES[-1], env)
    assert r.returncode == 0, r.stdout[-2000:]
    after = {t: len(df) for t, df in _silver(root).items()}
    assert before == after, f"rerun changed silver row counts: {before} -> {after}"


def test_5_planted_problems_handled(run):
    root, _, _ = run
    if not (PROJ / "sample_data" / "ISSUES.csv").exists():
        pytest.skip("no ISSUES.csv (data was given, not generated)")
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "check_issues.py"), str(PROJ), str(root)],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout
