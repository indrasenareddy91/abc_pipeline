"""Auto reports (SPEC v2 C): plain-English Markdown + CSV twins built ONLY from the warehouse ops tables, so they work
for any project (no table, column or KPI names are hard-coded).

  profiling_report.md        what the raw files looked like (ingestion_log, profile_columns, row_counts)
  data_quality_report.md     issue -> fix -> rows (rules_log), quarantine by reason, expectations, gold checks
  reconciliation_report.md   received = duplicates + quarantined + accepted per table/batch; silver -> warehouse
  data_journey.md            step_io as a readable flow per table (raw file -> bronze -> silver -> warehouse -> KPIs)

build_reports(wh, batch_id)  -> {report_name: Report(markdown, {csv_name: DataFrame})}   (no file IO; dashboard uses it)
write_reports(wh, batch_id)  -> writes DATA_ROOT/reports/<batch>/ and, for the newest batch, reports/latest/
Lineage helpers (journey_for_table, ...) are shared with the dashboard "Data journey" page.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass, field

import pandas as pd

REPORT_NAMES = ["profiling_report", "data_quality_report", "reconciliation_report", "data_journey"]
REPORT_TITLES = {"profiling_report": "Profiling report", "data_quality_report": "Data quality report",
                 "reconciliation_report": "Reconciliation report", "data_journey": "Data journey"}
WAREHOUSE_PREFIX = "warehouse."
OK, BAD, WARN = "✅", "❌", "⚠️"


@dataclass
class Report:
    markdown: str
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)


# ================================================================================================ small helpers
def safe_query(wh, sql: str) -> pd.DataFrame:
    """Query; a missing table/column gives an empty frame (the step that fills it may not have run)."""
    try:
        df = wh.query(sql)
    except Exception:  # noqa: BLE001 - reports must never crash on a missing ops table
        return pd.DataFrame()
    return df if df is not None else pd.DataFrame()


def _int(v) -> int:
    try:
        if v is None or pd.isna(v):
            return 0
    except (TypeError, ValueError):
        pass
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _num(v) -> str:
    """1234 -> '1,234'; missing -> '–'."""
    try:
        if v is None or pd.isna(v):
            return "–"
    except (TypeError, ValueError):
        pass
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return str(v)


def _pct(part, whole) -> str:
    whole = _int(whole)
    return "–" if whole == 0 else f"{100 * _int(part) / whole:.1f}%"


def _cell(v, limit: int = 160) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(v).replace("\r", " ").replace("\n", " ").replace("|", "\\|").strip()
    return s if len(s) <= limit else s[:limit - 1] + "…"


def md_table(df: pd.DataFrame, max_rows: int | None = None, limit: int = 160) -> str:
    """DataFrame -> GitHub Markdown table (no extra dependency)."""
    if df is None or df.empty:
        return "_Nothing to show._\n"
    shown = df if max_rows is None else df.head(max_rows)
    cols = [str(c) for c in shown.columns]
    lines = ["| " + " | ".join(_cell(c) for c in cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for rec in shown.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(_cell(v, limit) for v in rec) + " |")
    if max_rows is not None and len(df) > max_rows:
        lines.append(f"\n_{len(df) - max_rows:,} more row(s) in the CSV file._")
    return "\n".join(lines) + "\n"


def readable_counts(v, limit: int = 6) -> str:
    """Profile value lists in any of the shapes the engine writes -> 'a (3), b (1)'.
    Accepts "'a':3; 'b':1", '{"a": 3}', '[["a", 3]]'; anything else is returned as-is."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(v)
    pairs: list[tuple[str, str]] = []
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            pairs = [(str(k), str(n)) for k, n in obj.items()]
        elif isinstance(obj, list):
            for it in obj:
                if isinstance(it, (list, tuple)) and len(it) == 2:
                    pairs.append((str(it[0]), str(it[1])))
                elif isinstance(it, dict) and len(it) >= 2:
                    vals = list(it.values())
                    pairs.append((str(vals[0]), str(vals[1])))
    except (ValueError, TypeError):
        for m in re.finditer(r"'((?:[^']|'')*)'\s*:\s*(\d+)", s):
            pairs.append((m.group(1).replace("''", "'"), m.group(2)))
    if not pairs:
        return s
    shown = ", ".join(f"{'(blank)' if k == '' else k} ({n})" for k, n in pairs[:limit])
    return shown + (f", … {len(pairs) - limit} more" if len(pairs) > limit else "")


def count_items(v) -> int:
    """How many distinct formats/values a profile list holds (0 if unknown)."""
    if v is None:
        return 0
    s = str(v)
    try:
        obj = json.loads(s)
        return len(obj) if isinstance(obj, (dict, list)) else 0
    except (ValueError, TypeError):
        return len(re.findall(r"'(?:[^']|'')*'\s*:\s*\d+", s))


def _batch_filter(wh, batch_id: str) -> str:
    return f"batch_id = {wh.quote(batch_id)}"


def _newest_checks(checks: pd.DataFrame) -> pd.DataFrame:
    """check_results of the newest run only (a re-run writes a new set)."""
    if checks.empty:
        return checks
    c = checks.copy()
    c["checked_at"] = pd.to_datetime(c["checked_at"], errors="coerce")
    newest = c.sort_values("checked_at").iloc[-1]["run_id"]
    return c[c["run_id"] == newest].sort_values("check_name")


def _icon(status) -> str:
    s = str(status or "").upper()
    return {"PASS": f"{OK} PASS", "FAIL": f"{BAD} FAIL", "WARN": f"{WARN} WARN"}.get(s, s)


def _source_tables(*frames: pd.DataFrame) -> list[str]:
    names: set[str] = set()
    for f in frames:
        if not f.empty and "table_name" in f.columns:
            names |= set(f["table_name"].dropna().astype(str))
    return sorted(names)


def _header(title: str, batch_id: str, sources: str, intro: str) -> str:
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (f"# {title} — batch `{batch_id}`\n\n{intro}\n\n"
            f"_Generated {now} from the warehouse tables {sources}._\n\n")


# ================================================================================================ lineage helpers
def wh_table(location) -> str | None:
    s = str(location or "")
    return s[len(WAREHOUSE_PREFIX):] if s.startswith(WAREHOUSE_PREFIX) else None


def journey_for_table(step_io: pd.DataFrame, table: str, kpi_catalog: pd.DataFrame | None = None) -> dict:
    """Everything step_io knows about one source table, in pipeline order.
    -> {raw: rows, bronze: rows, silver: rows, staging: rows, curated: [(table, rows)], downstream: [(table, rows)],
        kpis: [{table, rows, kpi_id, title}]}  (rows = step_io records as dicts)."""
    out = {"raw": [], "bronze": [], "silver": [], "staging": [], "curated": [], "downstream": [], "kpis": []}
    if step_io is None or step_io.empty:
        return out
    s = step_io.copy()
    s["step"] = s["step"].astype(str)
    own = s[s["table_name"].astype(str) == table]
    out["bronze"] = own[own["step"] == "bronze"].to_dict("records")
    out["raw"] = [{"file": r.get("input_location"), "rows": r.get("rows_in")} for r in out["bronze"]]
    out["silver"] = own[own["step"] == "silver"].to_dict("records")
    stg_rows = own[(own["step"] == "gold") & own["output_location"].astype(str).str.startswith(WAREHOUSE_PREFIX)]
    out["staging"] = stg_rows.to_dict("records")
    edges = s[s["input_location"].map(wh_table).notna() & s["output_location"].map(wh_table).notna()]
    edges = edges.assign(src=edges["input_location"].map(wh_table), dst=edges["output_location"].map(wh_table))
    rows_of = dict(zip(edges["dst"], edges["rows_out"]))
    start = {wh_table(r.get("output_location")) for r in out["staging"]} - {None}
    direct = sorted(set(edges[edges["src"].isin(start)]["dst"]))
    seen, frontier = set(start) | set(direct), list(direct)
    while frontier:  # everything reachable downstream
        nxt = sorted(set(edges[edges["src"].isin(frontier)]["dst"]) - seen)
        seen |= set(nxt)
        frontier = nxt
    kpi_tables = set(edges[edges["step"] == "kpis"]["dst"])
    titles = {}
    if kpi_catalog is not None and not kpi_catalog.empty:
        for _, r in kpi_catalog.iterrows():
            for t in re.split(r"[,;\s]+", str(r.get("table_name") or "")):
                if t:
                    titles[t.lower()] = (str(r.get("kpi_id") or ""), str(r.get("title") or ""))
    out["curated"] = [(t, rows_of.get(t)) for t in direct if t not in kpi_tables]
    out["downstream"] = [(t, rows_of.get(t)) for t in sorted(seen - start - set(direct) - kpi_tables)]
    near = start | {t for t, _ in out["curated"]}  # the table's own warehouse tables
    direct_kpis = set(edges[edges["src"].isin(near) & (edges["step"] == "kpis")]["dst"])
    out["kpis"] = [{"table": t, "rows": rows_of.get(t), "kpi_id": titles.get(t, ("", ""))[0],
                    "title": titles.get(t, ("", ""))[1], "direct": t in direct_kpis}
                   for t in sorted((seen | set(direct)) & kpi_tables)]
    return out


def kpi_lines(kpis: list[dict]) -> list[str]:
    """Group KPI tables by KPI: 'KPI 01 — title (`kpi01_x`, `kpi01_y`)'; tables without a catalog entry last."""
    groups: dict[str, list[str]] = {}
    names: dict[str, str] = {}
    for k in kpis:
        key = k["kpi_id"] or "~"
        groups.setdefault(key, []).append(f"`{k['table']}`")
        names[key] = k["title"]
    out = []
    for key in sorted(groups):
        tabs = ", ".join(groups[key])
        out.append(f"KPI {key} — {names[key]} ({tabs})" if key != "~" else f"other result tables ({tabs})")
    return out


# ================================================================================================ the reports
def profiling_report(wh, batch_id: str) -> Report:
    b = _batch_filter(wh, batch_id)
    ing = safe_query(wh, f"SELECT table_name, file_name, rows_in, columns_in, sha256 FROM ingestion_log WHERE {b} "
                         "ORDER BY table_name, file_name")
    prof = safe_query(wh, f"SELECT * FROM profile_columns WHERE {b} ORDER BY table_name")
    rc = safe_query(wh, f"SELECT * FROM row_counts WHERE {b} ORDER BY table_name")
    md = _header("Profiling report", batch_id, "`ingestion_log`, `profile_columns` and `row_counts`",
                 "What the raw files looked like **before any cleaning**: how many rows and columns arrived, how many "
                 "values were blank or placeholders (like `n/a`, `-`), how many different values and formats each "
                 "column had, and how many rows were repeated.")
    tables: dict[str, pd.DataFrame] = {}
    names = _source_tables(ing, prof, rc)
    if not names:
        return Report(md + "No profile data for this batch yet — run the pipeline first.\n", {})

    # ---- summary
    summary = []
    for t in names:
        fi = ing[ing["table_name"].astype(str) == t] if not ing.empty else ing
        pr = prof[prof["table_name"].astype(str) == t] if not prof.empty else prof
        r = rc[rc["table_name"].astype(str) == t].iloc[0] if not rc.empty and (rc["table_name"] == t).any() else {}
        received = _int(fi["rows_in"].sum()) if len(fi) else _int(r.get("raw_rows") if len(r) else 0)
        with_blanks = int(((pd.to_numeric(pr["blank"], errors="coerce").fillna(0)
                            + pd.to_numeric(pr.get("placeholder", 0), errors="coerce").fillna(0)) > 0).sum()) \
            if len(pr) else 0
        mixed = int((pr["shapes"].map(count_items) > 1).sum()) if len(pr) and "shapes" in pr else 0
        summary.append({"Table": t, "Files": len(fi), "Rows received": received, "Columns": len(pr),
                        "Exact duplicate rows": _int(r.get("exact_duplicates")) if len(r) else None,
                        "Repeated keys (older copies)": _int(r.get("key_duplicates")) if len(r) else None,
                        "Columns with blanks": with_blanks, "Columns with mixed formats": mixed})
    sdf = pd.DataFrame(summary)
    tables["summary"] = sdf
    md += "## Summary\n\n" + md_table(sdf) + "\n"

    if not ing.empty:
        files = ing.copy()
        files["columns"] = files["columns_in"].fillna("").astype(str).map(lambda s: len([c for c in s.split(",") if c]))
        files["sha256"] = files["sha256"].fillna("").astype(str).str[:12]
        fdf = files[["table_name", "file_name", "rows_in", "columns", "sha256"]].rename(columns={
            "table_name": "Table", "file_name": "File", "rows_in": "Rows", "columns": "Columns",
            "sha256": "Fingerprint (sha256, first 12)"})
        tables["files"] = fdf
        md += ("## Files received\n\nThe fingerprint proves which exact file was loaded: the same file always gives "
               "the same value.\n\n" + md_table(fdf) + "\n")

    # ---- per table / column
    col_rows = []
    for t in names:
        pr = prof[prof["table_name"].astype(str) == t] if not prof.empty else prof
        md += f"## {t}\n\n"
        if pr.empty:
            md += "_No column profile for this table._\n\n"
            continue
        p = pr.copy()
        fi = ing[ing["table_name"].astype(str) == t] if not ing.empty else ing
        order = [c for c in str(fi["columns_in"].iloc[0] if len(fi) else "").split(",") if c]
        p["_pos"] = p["column_name"].map({c: i for i, c in enumerate(order)}).fillna(len(order))
        p = p.sort_values(["_pos", "column_name"])
        for c in ("rows_total", "blank", "placeholder", "distinct_values", "untrimmed"):
            p[c] = pd.to_numeric(p[c], errors="coerce").fillna(0).astype(int) if c in p else 0
        p["empty"] = p["blank"] + p["placeholder"]
        p["formats"] = p["shapes"].map(count_items) if "shapes" in p else 0
        view = pd.DataFrame({
            "Column": p["column_name"], "Rows": p["rows_total"],
            "Blank / placeholder %": [_pct(e, n) for e, n in zip(p["empty"], p["rows_total"])],
            "Blank": p["blank"], "Placeholders": p["placeholder"], "Distinct": p["distinct_values"],
            "Untrimmed": p["untrimmed"], "Formats seen": p["shapes"].map(readable_counts) if "shapes" in p else "",
            "Most common values": p["top_values"].map(readable_counts) if "top_values" in p else ""})
        md += md_table(view) + "\n"
        notes = []
        for _, r in p.iterrows():
            bits = []
            if r["empty"]:
                bits.append(f"{r['empty']:,} empty or placeholder value(s) ({_pct(r['empty'], r['rows_total'])})")
            if r["formats"] > 1:
                bits.append(f"{r['formats']} different formats")
            if r["untrimmed"]:
                bits.append(f"{r['untrimmed']:,} value(s) with extra spaces")
            if bits:
                notes.append(f"- `{r['column_name']}`: " + "; ".join(bits))
        if notes:
            md += "**What stands out**\n\n" + "\n".join(notes) + "\n\n"
        else:
            md += "Nothing unusual: no blanks, one format per column, no extra spaces.\n\n"
        col_rows.append(view.assign(Table=t))
    if col_rows:
        cdf = pd.concat(col_rows, ignore_index=True)
        tables["columns"] = cdf[["Table"] + [c for c in cdf.columns if c != "Table"]]
    return Report(md, tables)


def data_quality_report(wh, batch_id: str) -> Report:
    b = _batch_filter(wh, batch_id)
    rules = safe_query(wh, f"SELECT table_name, rule_id, column_name, issue, action, rows_affected FROM rules_log "
                           f"WHERE {b}")
    quar = safe_query(wh, f"SELECT table_name, rule_id, reason, business_key, record_key FROM quarantine WHERE {b}")
    rc = safe_query(wh, f"SELECT * FROM row_counts WHERE {b}")
    exp = safe_query(wh, f"SELECT * FROM dq_expectations WHERE {b}")
    checks = _newest_checks(safe_query(wh, f"SELECT * FROM check_results WHERE {b}"))
    md = _header("Data quality report", batch_id,
                 "`rules_log`, `quarantine`, `row_counts`, `dq_expectations` and `check_results`",
                 "For every table: **what was wrong → how we fixed it → how many rows**, which rows we refused to load "
                 "(quarantine) and why, and the automatic checks run on the cleaned data. We never guess: a value we "
                 "cannot trust is either set to empty and flagged, or the whole row is quarantined with a reason.")
    tables: dict[str, pd.DataFrame] = {}
    names = _source_tables(rules, quar, rc, exp)
    if not names and checks.empty:
        return Report(md + "No data-quality information for this batch yet — run the pipeline first.\n", {})
    if not rules.empty:
        rules = rules.copy()
        rules["rows_affected"] = pd.to_numeric(rules["rows_affected"], errors="coerce").fillna(0).astype(int)

    # ---- summary
    summary = []
    for t in names:
        r = rc[rc["table_name"].astype(str) == t] if not rc.empty else rc
        ru = rules[(rules["table_name"].astype(str) == t) & (rules["rows_affected"] > 0)] if not rules.empty else rules
        ex = exp[exp["table_name"].astype(str) == t] if not exp.empty else exp
        received, accepted = (_int(r["raw_rows"].sum()), _int(r["accepted"].sum())) if len(r) else (None, None)
        quarantined = _int(r["quarantined"].sum()) if len(r) else int((quar["table_name"] == t).sum()) \
            if not quar.empty else 0
        passed = int((ex["status"].astype(str).str.upper() == "PASS").sum()) if len(ex) else 0
        summary.append({"Table": t, "Rows received": received, "Accepted": accepted, "Quarantined": quarantined,
                        "Quarantined %": _pct(quarantined, received) if received else "–",
                        "Rules applied": len(ru), "Values / rows touched by rules": _int(ru["rows_affected"].sum())
                        if len(ru) else 0,
                        "Expectations passed": f"{passed} / {len(ex)}" if len(ex) else "none recorded"})
    sdf = pd.DataFrame(summary)
    tables["summary"] = sdf
    md += "## Summary\n\n" + md_table(sdf) + "\n"
    if not checks.empty:
        passed = int((checks["status"].astype(str).str.upper() == "PASS").sum())
        md += (f"**Warehouse checks:** {passed} of {len(checks)} passed"
               f"{' ' + OK if passed == len(checks) else ' ' + BAD}.\n\n")

    rule_rows, q_rows, e_rows = [], [], []
    for t in names:
        md += f"## {t}\n\n### What was wrong → how we fixed it\n\n"
        ru = rules[(rules["table_name"].astype(str) == t) & (rules["rows_affected"] > 0)] if not rules.empty else rules
        if len(ru):
            v = ru.sort_values("rows_affected", ascending=False)
            v = pd.DataFrame({"What was wrong": v["issue"], "How we fixed it": v["action"], "Column": v["column_name"],
                              "Rows affected": v["rows_affected"], "Rule": v["rule_id"]})
            md += md_table(v, limit=220) + "\n"
            rule_rows.append(v.assign(Table=t))
        else:
            md += "No cleaning rule had to change anything in this table.\n\n"

        md += "### Rows rejected (quarantine) by reason\n\n"
        qt = quar[quar["table_name"].astype(str) == t] if not quar.empty else quar
        if len(qt):
            qt = qt.copy()
            qt["id"] = qt["business_key"].fillna("").astype(str)
            grp = []
            for (reason, rule), g in qt.groupby(["reason", "rule_id"], dropna=False):
                ids = [i for i in g["id"].tolist() if i][:5]
                grp.append({"Reason": reason, "Rule": rule, "Rows": len(g), "Example ids": ", ".join(ids)})
            gdf = pd.DataFrame(grp).sort_values("Rows", ascending=False)
            md += md_table(gdf, limit=220) + "\n"
            q_rows.append(gdf.assign(Table=t))
        else:
            md += "No rows were quarantined.\n\n"

        md += "### Expectations (checks on the cleaned table)\n\n"
        ex = exp[exp["table_name"].astype(str) == t] if not exp.empty else exp
        if len(ex):
            rank = {"FAIL": 0, "WARN": 1, "PASS": 2}  # problems first
            ev = ex.assign(_r=ex["status"].astype(str).str.upper().map(rank).fillna(3)) \
                .sort_values(["_r", "expectation", "column_name"])
            ev = pd.DataFrame({"Result": ev["status"].map(_icon), "Expectation": ev["expectation"],
                               "Column": ev.get("column_name"), "Severity": ev.get("severity"),
                               "Failing rows": ev.get("failing_rows"), "Detail": ev.get("detail")})
            md += md_table(ev) + "\n"
            e_rows.append(ev.assign(Table=t))
        else:
            md += "_No expectations recorded for this table in this batch._\n\n"

    if rule_rows:
        tables["rules"] = pd.concat(rule_rows, ignore_index=True)
    if q_rows:
        tables["quarantine_by_reason"] = pd.concat(q_rows, ignore_index=True)
    if e_rows:
        tables["expectations"] = pd.concat(e_rows, ignore_index=True)
    md += ("## Warehouse checks (gold)\n\nRun after loading the warehouse; if any fails, the run fails.\n\n")
    if not checks.empty:
        cv = pd.DataFrame({"Result": checks["status"].map(_icon), "Check": checks["check_name"],
                           "Detail": checks["detail"].fillna("").astype(str).map(lambda s: s.splitlines()[0]
                                                                                    if s else "")})
        md += md_table(cv) + "\n"
        tables["checks"] = cv
    else:
        md += "_No checks recorded for this batch yet._\n"
    return Report(md, tables)


def _silver_rows_default(table: str) -> int | None:
    try:
        from engine.gold import parquet_rows

        return parquet_rows(f"silver/{table}")
    except Exception:  # noqa: BLE001 - storage not reachable (e.g. dashboard without DATA_ROOT)
        return None


def reconciliation_report(wh, batch_id: str, silver_rows=_silver_rows_default) -> Report:
    rc = safe_query(wh, "SELECT * FROM row_counts ORDER BY batch_id, table_name")
    qn = safe_query(wh, "SELECT batch_id, table_name, COUNT(*) AS n FROM quarantine GROUP BY batch_id, table_name")
    sio = safe_query(wh, f"SELECT * FROM step_io WHERE {_batch_filter(wh, batch_id)}")
    md = _header("Reconciliation report", batch_id, "`row_counts`, `quarantine`, `step_io` and the curated tables",
                 "Proof that **no row was lost or invented**. Every row received ends up in exactly one place:\n\n"
                 "> received = exact duplicates dropped + older copies of the same key dropped + quarantined + "
                 "accepted\n>\n> accepted = new + changed + unchanged\n\n"
                 f"{OK} = the numbers add up, {BAD} = they do not (the run fails on {BAD}).")
    tables: dict[str, pd.DataFrame] = {}
    if rc.empty:
        return Report(md + "No row counts yet — run the pipeline first.\n", {})

    qmap = {(str(r["batch_id"]), str(r["table_name"])): _int(r["n"]) for _, r in qn.iterrows()} if not qn.empty else {}
    rows = []
    for _, r in rc.iterrows():
        recv, ed, kd, qq, acc = (_int(r[c]) for c in ("raw_rows", "exact_duplicates", "key_duplicates", "quarantined",
                                                      "accepted"))
        ins, upd, unch = (_int(r[c]) for c in ("inserted", "updated", "unchanged"))
        qrows = qmap.get((str(r["batch_id"]), str(r["table_name"])), 0)
        rows.append({"Batch": r["batch_id"], "Table": r["table_name"], "Received": recv, "Exact duplicates": ed,
                     "Older copies of same key": kd, "Quarantined": qq, "Accepted": acc,
                     "Received adds up": OK if recv == ed + kd + qq + acc else BAD,
                     "New": ins, "Changed": upd, "Unchanged": unch,
                     "Accepted adds up": OK if acc == ins + upd + unch else BAD,
                     "Quarantine table rows": qrows, "Quarantine matches": OK if qrows == qq else BAD})
    rdf = pd.DataFrame(rows)
    tables["row_counts"] = rdf
    bad = int((rdf[["Received adds up", "Accepted adds up", "Quarantine matches"]] == BAD).any(axis=1).sum())
    md += "## Every row accounted for (all batches loaded so far)\n\n"
    md += (f"**All {len(rdf)} table/batch combinations add up {OK}**\n\n" if bad == 0
           else f"**{bad} table/batch combination(s) do NOT add up {BAD}**\n\n")
    md += md_table(rdf) + "\n"

    # ---- silver -> warehouse staging (this batch)
    this = rc[rc["batch_id"].astype(str) == batch_id]
    stg = sio[(sio["step"].astype(str) == "gold")
              & sio["input_location"].astype(str).str.startswith("silver_changes/")] if not sio.empty else sio
    s_rows = []
    for _, r in this.iterrows():
        t = str(r["table_name"])
        expected = _int(r["inserted"]) + _int(r["updated"])
        e = stg[stg["table_name"].astype(str) == t] if len(stg) else stg
        changes = _int(e["rows_in"].iloc[0]) if len(e) else None
        loaded = _int(e["rows_out"].iloc[0]) if len(e) else None
        ok = changes is not None and expected == changes == loaded
        s_rows.append({"Table": t, "New + changed in silver (row_counts)": expected,
                       "Rows in silver_changes": changes, "Rows loaded to staging": loaded,
                       "Warehouse table": e["output_location"].iloc[0].replace(WAREHOUSE_PREFIX, "") if len(e) else "",
                       "Match": OK if ok else (BAD if changes is not None else "– (no step_io yet)")})
    sdf = pd.DataFrame(s_rows)
    tables["silver_to_staging"] = sdf
    md += (f"## Silver → warehouse staging (batch `{batch_id}`)\n\nOnly the rows this batch inserted or changed in "
           "silver are loaded (incremental), so these three numbers must be equal.\n\n" + md_table(sdf) + "\n")

    # ---- current silver vs curated tables
    md += ("## Clean data vs curated warehouse tables\n\nCurrent clean (silver) rows compared with the warehouse "
           "tables built from them. A **fact** table must match its silver table row for row; a **dimension** keeps "
           "history (several versions per key) and an UNKNOWN member, so we compare the number of distinct business "
           "keys instead.\n\n")
    v_rows = []
    try:
        from engine import config

        specs = config.tables() or {}
        fact_sources = (config.settings() or {}).get("gold_fact_sources") or {}
    except Exception:  # noqa: BLE001 - no project config available (e.g. dashboard without PROJECT_DIR)
        specs, fact_sources = {}, {}
    if not fact_sources:
        try:
            from engine.gold import DEFAULT_FACT_SOURCES

            fact_sources = DEFAULT_FACT_SOURCES
        except Exception:  # noqa: BLE001
            fact_sources = {}
    gold_edges = sio[(sio["step"].astype(str) == "gold") & sio["input_location"].astype(str)
                     .str.startswith(WAREHOUSE_PREFIX + "stg_")] if not sio.empty else sio
    fact_for = {v: k for k, v in fact_sources.items()}
    for t in sorted(set(this["table_name"].astype(str))):
        n_silver = silver_rows(t)
        key = (specs.get(t) or {}).get("key") if isinstance(specs.get(t), dict) else None
        targets = sorted(set(gold_edges[gold_edges["input_location"] == f"{WAREHOUSE_PREFIX}stg_{t}"]
                             ["output_location"].map(wh_table))) if len(gold_edges) else []
        fact = fact_for.get(t)
        if fact and fact not in targets:
            targets.append(fact)
        for target in targets:
            cols = set(c.lower() for c in safe_query(wh, f"SELECT * FROM {target} WHERE 1 = 0").columns)
            if not cols:
                continue
            if target == fact:
                n = safe_query(wh, f"SELECT COUNT(*) AS n FROM {target}")
                n_wh, how = (_int(n["n"].iloc[0]) if not n.empty else None), "all rows (fact)"
                match = OK if n_silver is not None and n_wh == n_silver else (BAD if n_silver is not None else "–")
            elif key and key.lower() in cols and ("is_current" in cols or target.startswith("dim_")):
                cur = " AND is_current" if "is_current" in cols else ""
                n = safe_query(wh, f"SELECT COUNT(DISTINCT {key}) AS n FROM {target} WHERE {key} IS NOT NULL "
                                   f"AND CAST({key} AS VARCHAR(256)) <> 'UNKNOWN'{cur}")
                n_wh = _int(n["n"].iloc[0]) if not n.empty else None
                how = f"distinct {key}" + (" (current versions)" if cur else "")
                match = OK if n_silver is not None and n_wh == n_silver else (WARN if n_silver is not None else "–")
            else:
                continue
            v_rows.append({"Silver table": t, "Silver rows (current)": n_silver, "Warehouse table": target,
                           "Compared": how, "Warehouse count": n_wh, "Match": match})
    vdf = pd.DataFrame(v_rows)
    if len(vdf):
        tables["silver_vs_warehouse"] = vdf
        md += md_table(vdf) + f"\n{WARN} on a dimension = worth a look (it may also receive keys from other tables), " \
                              "not an error.\n\n"
    else:
        md += "_No comparable warehouse tables found (lineage not recorded yet or storage not reachable)._\n\n"

    checks = _newest_checks(safe_query(wh, f"SELECT * FROM check_results WHERE {_batch_filter(wh, batch_id)}"))
    if not checks.empty:
        rel = checks[checks["check_name"].astype(str).str.contains("row|reconcil|match|equal|count|quarantine",
                                                                   case=False)]
        if len(rel):
            cv = pd.DataFrame({"Result": rel["status"].map(_icon), "Check": rel["check_name"],
                               "Detail": rel["detail"].fillna("").astype(str).map(
                                   lambda s: s.splitlines()[0] if s else "")})
            tables["checks"] = cv
            md += "## Automatic reconciliation checks (gold)\n\n" + md_table(cv) + "\n"
    return Report(md, tables)


def _flow_line(n: int, title: str, where: str, text: str) -> str:
    return f"{n}. **{title}**" + (f" `{where}`" if where else "") + f" — {text}\n"


def data_journey(wh, batch_id: str) -> Report:
    sio = safe_query(wh, f"SELECT * FROM step_io WHERE {_batch_filter(wh, batch_id)}")
    cat = safe_query(wh, "SELECT kpi_id, title, table_name FROM kpi_catalog")
    md = _header("Data journey", batch_id, "`step_io` and `kpi_catalog`",
                 "How every source table moved through the pipeline in this batch, with the row count at every step: "
                 "**raw file → bronze (as received) → silver (clean) / quarantine (rejected) → warehouse staging → "
                 "curated tables → KPIs**.")
    if sio.empty:
        return Report(md + "No step records (`step_io`) for this batch yet — run the pipeline first.\n", {})
    order = {"bronze": 0, "silver": 1, "gold": 2, "kpis": 3}
    s = sio.copy()
    s["_o"] = s["step"].astype(str).map(order).fillna(9)
    s = s.sort_values(["_o", "table_name", "input_location", "output_location"]).drop(columns="_o")
    sources = sorted(set(s[s["step"].astype(str).isin(["bronze", "silver"])]["table_name"].astype(str))
                     | set(s[s["input_location"].astype(str).str.startswith("silver_changes/")]["table_name"]
                           .astype(str)))
    for t in sources:
        j = journey_for_table(s, t, cat)
        md += f"## {t}\n\n"
        n = 1
        if j["bronze"]:
            md += _flow_line(n, "Raw input", "", ", ".join(
                f"`{r.get('input_location')}` ({_num(r.get('rows_in'))} rows received)" for r in j["bronze"]))
            outs = sorted({str(r.get("output_location")) for r in j["bronze"]})
            md += _flow_line(n + 1, "Bronze", ", ".join(outs),
                             f"{_num(sum(_int(r.get('rows_out')) for r in j['bronze']))} rows stored exactly as "
                             "received (all text, nothing changed)")
            n += 2
        for r in j["silver"]:
            extra = f" ({_cell(r.get('notes'), 300)})" if r.get("notes") else ""
            if str(r.get("output_location") or "").startswith("silver_changes/"):
                md += _flow_line(n, "Changes for the warehouse", r.get("output_location"),
                                 f"{_num(r.get('rows_out'))} of the accepted rows are new or changed{extra}")
            else:
                md += _flow_line(n, "Silver", r.get("output_location"),
                                 f"{_num(r.get('rows_in'))} rows in → {_num(r.get('rows_out'))} clean rows accepted, "
                                 f"{_num(r.get('rows_rejected'))} rejected to `quarantine/{t}/batch_id={batch_id}/`"
                                 f"{extra}")
            n += 1
        for r in j["staging"]:
            md += _flow_line(n, "Warehouse staging", wh_table(r.get("output_location")),
                             f"{_num(r.get('rows_out'))} new or changed rows loaded from `{r.get('input_location')}`")
        n += 1 if j["staging"] else 0
        if j["curated"]:
            md += _flow_line(n, "Curated tables", "", ", ".join(f"`{c}` ({_num(k)} rows)" for c, k in j["curated"]))
            n += 1
        if j["downstream"]:
            md += _flow_line(n, "Then feeds", "", ", ".join(f"`{c}` ({_num(k)} rows)" for c, k in j["downstream"]))
            n += 1
        direct = [k for k in j["kpis"] if k["direct"]]
        indirect = [k for k in j["kpis"] if not k["direct"]]
        if direct:
            md += _flow_line(n, "Used directly in", "", "\n" + "".join(f"   - {x}\n" for x in kpi_lines(direct)))
            n += 1
        if indirect:
            md += _flow_line(n, "Also reaches (through the tables above)", "",
                             ", ".join(sorted({f"KPI {k['kpi_id']}" if k["kpi_id"] else f"`{k['table']}`"
                                               for k in indirect})))
        md += "\n"
    view = s[["step", "table_name", "input_location", "rows_in", "output_location", "rows_out", "rows_rejected",
              "notes"]].rename(columns={"step": "Step", "table_name": "Table", "input_location": "Input",
                                        "rows_in": "Rows in", "output_location": "Output", "rows_out": "Rows out",
                                        "rows_rejected": "Rejected", "notes": "Notes"})
    md += ("## Every step: input → output\n\nOne line per input → output. For warehouse tables (`warehouse.<table>`) "
           "*Rows out* is the number of rows the table holds after this batch (history included).\n\n"
           + md_table(view, limit=120))
    return Report(md, {"steps": view})


# ================================================================================================ entry points
BUILDERS = {"profiling_report": profiling_report, "data_quality_report": data_quality_report,
            "reconciliation_report": reconciliation_report, "data_journey": data_journey}


def build_reports(wh, batch_id: str) -> dict[str, Report]:
    return {name: fn(wh, batch_id) for name, fn in BUILDERS.items()}


def write_reports(wh, batch_id: str, is_latest_batch: bool = True) -> list[str]:
    """Write DATA_ROOT/reports/<batch>/<name>.md + <name>__<table>.csv; same files to reports/latest/ when this is
    the newest batch (re-running an old batch never replaces the latest reports). Returns relative paths written."""
    from engine import io as storage

    reports = build_reports(wh, batch_id)
    folders = [f"reports/{batch_id}"] + (["reports/latest"] if is_latest_batch else [])
    written = []
    for folder in folders:
        if storage.exists(folder):
            storage.delete(folder)  # a re-run replaces the folder (no stale CSVs from an older run)
        for name, rep in reports.items():
            storage.write_text(f"{folder}/{name}.md", rep.markdown)
            written.append(f"{folder}/{name}.md")
            for tname, df in rep.tables.items():
                storage.write_text(f"{folder}/{name}__{tname}.csv", df.to_csv(index=False))
                written.append(f"{folder}/{name}__{tname}.csv")
        storage.write_text(f"{folder}/README.md", _index(batch_id, reports))
        written.append(f"{folder}/README.md")
    return written


def _index(batch_id: str, reports: dict[str, Report]) -> str:
    lines = [f"# Reports — batch `{batch_id}`\n"]
    for name, rep in reports.items():
        csvs = ", ".join(f"[{name}__{t}.csv]({name}__{t}.csv)" for t in rep.tables) or "no tables"
        lines.append(f"- [{REPORT_TITLES[name]}]({name}.md) — CSV: {csvs}")
    return "\n".join(lines) + "\n"
