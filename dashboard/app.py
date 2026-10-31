"""ABC Insurance data pipeline dashboard (Streamlit).

Reads ONLY the warehouse (engine.warehouse.get_warehouse()). Every query is cached for 60 s and a missing
table shows "No data yet" instead of an error, so the app works before the first pipeline run.

Run (from the repo root):  streamlit run dashboard/app.py
"""
from __future__ import annotations

import decimal
import hmac
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

try:  # interactive charts; the dashboard still works without plotly (falls back to tables)
    import plotly.graph_objects as go
except ImportError:  # pragma: no cover
    go = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(page_title="ABC Insurance", page_icon="🛡️", layout="wide")

NO_DATA = "No data yet — run the pipeline."
FAILED_WORDS = ("fail", "error")
PASS_WORDS = ("pass", "ok", "success", "succeeded")


# ======================================================================================
# Warehouse access
# ======================================================================================
@st.cache_resource(show_spinner=False)
def _warehouse():
    from engine.warehouse import get_warehouse  # imported lazily so a broken engine shows a clear message
    return get_warehouse()


def _is_missing(err: str) -> bool:
    e = err.lower()
    return ("does not exist" in e or "not found" in e or "undefinedtable" in e or "undefinedcolumn" in e
            or "no such table" in e)


def _to_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """Decimal columns (psycopg) -> float so charts and sums work. Text columns are left alone (ids keep zeros)."""
    for c in df.columns:
        if df[c].dtype == object:
            s = df[c].dropna()
            if len(s) and s.map(lambda v: isinstance(v, decimal.Decimal)).all():
                df[c] = df[c].astype(float)
    return df


@st.cache_data(ttl=60, show_spinner=False)
def _cached_query(sql: str) -> tuple[pd.DataFrame | None, str]:
    try:
        df = _warehouse().query(sql)
    except Exception as exc:  # noqa: BLE001 - any warehouse problem becomes a message, never a crash
        return None, f"{type(exc).__name__}: {exc}"
    if df is None:
        return pd.DataFrame(), ""
    return _to_numbers(df.copy()), ""


def q(sql: str, quiet: bool = False) -> pd.DataFrame | None:
    """Run a query. Returns None when the table/column does not exist (pipeline not run yet)."""
    df, err = _cached_query(sql)
    if err and not _is_missing(err) and not quiet:
        st.warning(f"Query problem: {err[:300]}")
    return df


def has_rows(df: pd.DataFrame | None) -> bool:
    return df is not None and len(df) > 0


def lit(value) -> str:
    """Safe SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


# ======================================================================================
# Formatting
# ======================================================================================
MONEY_RE = re.compile(r"amount|revenue|price|value|_inr|monetary|net_|^net$|spend|turnover")
NOT_MONEY_RE = re.compile(r"pct|percent|share|count|rate|rank|score|distinct|growth|days|_n$|^n$|tile|txns|qty|"
                          r"rows|version|key|values")


def rupees(x) -> str:
    """₹ with Indian thousands grouping: 1234567.5 -> ₹12,34,567.50"""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return str(x)
    sign = "-" if v < 0 else ""
    whole, frac = f"{abs(v):.2f}".split(".")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        head = ",".join(re.findall(r"\d{1,2}", head[::-1]))[::-1]  # pairs from the right
        whole = head + "," + tail
    return f"{sign}₹{whole}.{frac}"


def is_money(df: pd.DataFrame, c: str) -> bool:
    name = str(c).lower()
    return (pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_bool_dtype(df[c])
            and bool(MONEY_RE.search(name)) and not NOT_MONEY_RE.search(name))


def pretty(df: pd.DataFrame) -> pd.DataFrame:
    """Display copy: money columns as ₹ strings, other floats rounded."""
    out = df.copy()
    for c in out.columns:
        if is_money(out, c):
            out[c] = out[c].map(rupees)
        elif pd.api.types.is_float_dtype(out[c]):
            out[c] = out[c].round(2)
    return out


def table(df: pd.DataFrame, height: int | None = None) -> None:
    kw = {"hide_index": True, "use_container_width": True}
    if height:
        kw["height"] = height
    st.dataframe(pretty(df), **kw)


def show_or_info(df: pd.DataFrame | None, msg: str = NO_DATA) -> bool:
    if not has_rows(df):
        st.info(msg)
        return False
    return True


def status_icon(s) -> str:
    s = str(s or "").lower()
    if any(w in s for w in FAILED_WORDS):
        return "❌ failed"
    if s in ("running", "started", "start", "in_progress"):
        return "⏳ running"
    return "✅ ok"


def check_icon(s) -> str:
    s = str(s or "").lower()
    return "✅ pass" if s in PASS_WORDS or s.startswith("pass") else "❌ fail"


def csv_button(df: pd.DataFrame, name: str, key: str) -> None:
    st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode("utf-8"), file_name=f"{name}.csv",
                       mime="text/csv", key=key)


def find_col(df: pd.DataFrame, *patterns: str, numeric: bool | None = None, exclude: tuple = ()) -> str | None:
    for p in patterns:
        for c in df.columns:
            name = str(c).lower()
            if p in name and c not in exclude:
                if numeric is None or numeric == pd.api.types.is_numeric_dtype(df[c]):
                    return c
    return None


def batches_of(*dfs: pd.DataFrame | None) -> list[str]:
    found = set()
    for d in dfs:
        if has_rows(d) and "batch_id" in d.columns:
            found |= set(d["batch_id"].dropna().astype(str))
    return sorted(found)


# ======================================================================================
# Password gate
# ======================================================================================
def password_gate() -> None:
    expected = os.environ.get("DASHBOARD_PASSWORD", "").strip()
    if not expected or st.session_state.get("authed"):
        return
    st.title("🛡️ ABC Insurance")
    pw = st.text_input("Password", type="password").strip()
    if pw and hmac.compare_digest(pw.encode(), expected.encode()):
        st.session_state.authed = True
        st.rerun()
    if pw:
        st.error("Wrong password.")
    st.stop()


# ======================================================================================
# 1. Overview
# ======================================================================================
def latest_run_per_batch(runs: pd.DataFrame) -> pd.DataFrame:
    """One line per batch: the newest run and whether any of its steps failed."""
    r = runs.copy()
    r["started_at"] = pd.to_datetime(r["started_at"], errors="coerce")
    r["finished_at"] = pd.to_datetime(r["finished_at"], errors="coerce")
    agg = (r.groupby(["batch_id", "run_id"])
           .agg(started=("started_at", "min"), finished=("finished_at", "max"),
                steps=("step", "count"),
                failed=("status", lambda s: int(s.astype(str).str.lower().str.contains("fail|error").sum())),
                running=("status", lambda s: int(s.astype(str).str.lower().isin(["running", "started", "start"]).sum())))
           .reset_index().sort_values("started"))
    last = agg.groupby("batch_id").tail(1).copy()
    last["result"] = last.apply(lambda x: "❌ failed" if x.failed else ("⏳ running" if x.running and pd.isna(x.finished)
                                                                       else "✅ ok"), axis=1)
    return last.sort_values("batch_id")


def checks_per_batch(checks: pd.DataFrame) -> dict[str, tuple[int, int]]:
    """batch -> (passed, total) for the newest run that wrote checks."""
    out = {}
    c = checks.copy()
    c["checked_at"] = pd.to_datetime(c["checked_at"], errors="coerce")
    for b, g in c.groupby("batch_id"):
        newest = g.sort_values("checked_at").iloc[-1]["run_id"]
        g = g[g.run_id == newest]
        passed = int(g["status"].map(lambda s: check_icon(s).startswith("✅")).sum())
        out[str(b)] = (passed, len(g))
    return out


def row_flow_chart(show: pd.DataFrame) -> None:
    """Sankey: received -> accepted / exact copies / older copies / quarantined (summed over the shown rows)."""
    if go is None or not len(show):
        return
    t = show[["Received", "Exact copies dropped", "Same id, older copy dropped", "Quarantined", "Accepted"]].sum()
    if int(t["Received"]) == 0:
        return
    targets = [("Accepted (clean)", t["Accepted"], "#2e7d32"), ("Exact copies", t["Exact copies dropped"], "#9e9e9e"),
               ("Older versions merged", t["Same id, older copy dropped"], "#757575"),
               ("Quarantined", t["Quarantined"], "#c62828")]
    targets = [x for x in targets if int(x[1]) > 0]
    labels = [f"Received {int(t['Received']):,}"] + [f"{n} {int(v):,}" for n, v, _ in targets]
    fig = go.Figure(go.Sankey(
        node=dict(label=labels, pad=18, thickness=16, color=["#1565c0"] + [c for _, _, c in targets]),
        link=dict(source=[0] * len(targets), target=list(range(1, len(targets) + 1)),
                  value=[int(v) for _, v, _ in targets])))
    fig.update_layout(height=280, margin=dict(l=10, r=10, t=10, b=10))
    st.caption("Every received row, and where it went (all batches shown above).")
    st.plotly_chart(fig, use_container_width=True)


def donut(df: pd.DataFrame, names: str, values: str, title: str = "") -> None:
    if go is None or not len(df):
        return
    fig = go.Figure(go.Pie(labels=df[names].astype(str), values=df[values], hole=0.55, textinfo="label+value"))
    fig.update_layout(height=320, margin=dict(l=10, r=10, t=30 if title else 10, b=10), title=title or None,
                      showlegend=False)
    st.plotly_chart(fig, use_container_width=True)


def kpi_filters(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Generic filters for any KPI table: low-cardinality text columns -> multiselect; a date/month column -> range."""
    date_cols = [c for c in df.columns if any(k in c.lower() for k in ("date", "month", "day", "week"))]
    text_cols = [c for c in df.columns if c not in date_cols and df[c].dtype == object
                 and 1 < df[c].nunique() <= 30][:4]
    if not (text_cols or date_cols):
        return df
    with st.expander("Filters", expanded=False):
        out = df
        cols = st.columns(max(1, min(4, len(text_cols) + (1 if date_cols else 0))))
        for i, c in enumerate(text_cols):
            pick = cols[i % len(cols)].multiselect(c, sorted(df[c].dropna().astype(str).unique()), key=f"{key}_ms_{c}")
            if pick:
                out = out[out[c].astype(str).isin(pick)]
        if date_cols:
            d = date_cols[0]
            vals = sorted(df[d].dropna().astype(str).unique())
            if len(vals) > 1:
                lo, hi = cols[-1].select_slider(d, options=vals, value=(vals[0], vals[-1]), key=f"{key}_rng_{d}")
                out = out[(out[d].astype(str) >= lo) & (out[d].astype(str) <= hi)]
        st.caption(f"{len(out):,} of {len(df):,} rows")
    return out


def kpi_cards(df: pd.DataFrame) -> None:
    """A KPI with one row (a headline number) is shown as big metric cards."""
    if len(df) != 1:
        return
    nums = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])][:4]
    if not nums:
        return
    for col_, c in zip(st.columns(len(nums)), nums):
        v = df[c].iloc[0]
        col_.metric(c.replace("_", " "), f"{v:,.2f}" if isinstance(v, float) else f"{v:,}")


def page_overview() -> None:
    st.header("Overview")
    st.markdown(
        "**The story in one paragraph.** Raw files arrive in batches (Day 1, Day 2, …). We first keep an untouched "
        "copy of every file (**bronze**). Then we normalise and clean each column — formats, dates, money, phone "
        "numbers, spellings — fixing what the data can prove, flagging what is still usable, and putting rows we "
        "cannot trust aside in **quarantine** with a plain reason (**silver**). The clean rows are loaded into a "
        "**star schema** in the warehouse (**gold**), the business **KPIs** are computed from it, and automatic "
        "checks prove that no row was lost.")

    runs = q("SELECT run_id, batch_id, step, status, started_at, finished_at, message FROM pipeline_runs "
             "ORDER BY started_at DESC LIMIT 1000")
    rc = q("SELECT * FROM row_counts")
    checks = q("SELECT run_id, batch_id, check_name, status, checked_at FROM check_results")

    if not (has_rows(runs) or has_rows(rc)):
        st.info(NO_DATA)
    else:
        last = latest_run_per_batch(runs) if has_rows(runs) else pd.DataFrame()
        chk = checks_per_batch(checks) if has_rows(checks) else {}
        st.subheader("Each batch at a glance")
        for b in batches_of(runs, rc):
            row = last[last.batch_id.astype(str) == b] if len(last) else last
            when = ""
            if len(row):
                r = row.iloc[0]
                when = f" · last run {r.result} · started {str(r.started)[:19]}"
            st.markdown(f"**Batch `{b}`**{when}")
            sub = rc[rc.batch_id.astype(str) == b] if has_rows(rc) else pd.DataFrame()
            c1, c2, c3, c4 = st.columns(4)
            if len(sub):
                c1.metric("Rows received", f"{int(sub.raw_rows.sum()):,}")
                c2.metric("Rows clean (accepted)", f"{int(sub.accepted.sum()):,}")
                c3.metric("Rows quarantined", f"{int(sub.quarantined.sum()):,}")
            else:
                c1.metric("Rows received", "–")
                c2.metric("Rows clean (accepted)", "–")
                c3.metric("Rows quarantined", "–")
            if b in chk:
                p, t = chk[b]
                c4.metric("Checks passed", f"{p} / {t}", delta="all good" if p == t else f"{t - p} failed",
                          delta_color="normal" if p == t else "inverse")
            else:
                c4.metric("Checks passed", "–")

    st.subheader("Where every row went")
    st.caption("Every row we received ends up in exactly one place: dropped as an exact copy, replaced by a newer "
               "copy of the same id, quarantined, or accepted. ✅ means the numbers add up.")
    if show_or_info(rc):
        show = rc.copy()
        for c in ["raw_rows", "exact_duplicates", "key_duplicates", "quarantined", "accepted", "inserted", "updated",
                  "unchanged"]:
            if c not in show.columns:
                show[c] = 0
            show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)
        show["adds up"] = ((show.exact_duplicates + show.key_duplicates + show.quarantined + show.accepted
                            == show.raw_rows)
                           & (show.inserted + show.updated + show.unchanged == show.accepted)).map({True: "✅", False: "❌"})
        show["quarantined %"] = (100 * show.quarantined / show.raw_rows.where(show.raw_rows > 0)).round(1)
        show = show.sort_values(["batch_id", "table_name"])[
            ["batch_id", "table_name", "raw_rows", "exact_duplicates", "key_duplicates", "quarantined", "accepted",
             "inserted", "updated", "unchanged", "quarantined %", "adds up"]]
        show.columns = ["Batch", "Table", "Received", "Exact copies dropped", "Same id, older copy dropped",
                        "Quarantined", "Accepted", "↳ new", "↳ changed", "↳ unchanged", "Quarantined %", "Adds up"]
        table(show)
        row_flow_chart(show)

    arch = ROOT / "docs" / "architecture.png"
    if arch.exists():
        st.subheader("Architecture")
        st.image(str(arch), use_container_width=True)


# ======================================================================================
# 2. Data issues (EDA)
# ======================================================================================
def _readable_list(v) -> str:
    """'{"a": 3, "b": 1}' or '[["a",3],...]' -> 'a (3), b (1)'. Anything else is shown as-is."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v)
    try:
        obj = json.loads(s)
    except (ValueError, TypeError):
        return s
    if isinstance(obj, dict):
        return ", ".join(f"{k} ({n})" for k, n in obj.items())
    if isinstance(obj, list):
        parts = []
        for it in obj:
            if isinstance(it, (list, tuple)) and len(it) == 2:
                parts.append(f"{it[0]} ({it[1]})")
            elif isinstance(it, dict):
                vals = list(it.values())
                parts.append(f"{vals[0]} ({vals[1]})" if len(vals) >= 2 else str(vals[0]))
            else:
                parts.append(str(it))
        return ", ".join(parts)
    return s


def page_issues() -> None:
    st.header("Data issues — what was wrong and how we fixed it")
    prof = q("SELECT * FROM profile_columns")
    rules = q("SELECT * FROM rules_log")
    if not (has_rows(prof) or has_rows(rules)):
        st.info(NO_DATA)
        return

    batches = batches_of(prof, rules)
    tables = sorted((set(prof.table_name.astype(str)) if has_rows(prof) else set())
                    | (set(rules.table_name.astype(str)) if has_rows(rules) else set()))
    c1, c2 = st.columns(2)
    batch = c1.selectbox("Batch", ["All batches"] + batches, index=0)
    tsel = c2.selectbox("Source table", tables,
                        index=tables.index("transactions") if "transactions" in tables else 0)

    def pick(df):
        if not has_rows(df):
            return df
        d = df[df.table_name.astype(str) == tsel]
        return d if batch == "All batches" else d[d.batch_id.astype(str) == batch]

    # ---- top issues as cards
    st.subheader(f"Top issues in {tsel}")
    r = pick(rules)
    if has_rows(r):
        r = r.copy()
        r["rows_affected"] = pd.to_numeric(r["rows_affected"], errors="coerce").fillna(0).astype(int)
        r = r[r.rows_affected > 0]
    if has_rows(r):
        top = (r.groupby(["issue", "action"], dropna=False).rows_affected.sum().reset_index()
               .sort_values("rows_affected", ascending=False).head(5))
        cols = st.columns(len(top))
        for col, (_, it) in zip(cols, top.iterrows()):
            with col.container(border=True):
                st.metric("Rows affected", f"{int(it.rows_affected):,}")
                st.markdown(f"**Problem:** {it.issue}")
                st.markdown(f"**Fix:** {it.action}")
        st.markdown("**All rules applied** — what was wrong → how we fixed it → rows affected")
        cols = [c for c in ["batch_id", "column_name", "issue", "action", "rows_affected", "rule_id"] if c in r.columns]
        full = r.sort_values(["batch_id", "rows_affected"], ascending=[True, False])[cols].rename(columns={
            "batch_id": "Batch", "column_name": "Column", "issue": "What was wrong", "action": "How we fixed it",
            "rows_affected": "Rows affected", "rule_id": "Rule id"})
        table(full)
    else:
        st.info("No rule touched this table in the chosen batch." if has_rows(rules) else NO_DATA)

    # ---- profile
    st.subheader(f"Raw file profile — {tsel}")
    st.caption("Measured on the raw file, before any cleaning. **Blank %** = empty or placeholder values like "
               "'n/a' or '-'. **Shapes** = the different formats seen (for example 9999-99-99 is a date written "
               "as year-month-day); more than one shape means mixed formats that needed cleaning. **Untrimmed** = "
               "values with extra spaces.")
    p = pick(prof)
    if has_rows(p):
        p = p.copy()
        for c in ["rows_total", "blank", "placeholder", "untrimmed", "distinct_values"]:
            if c in p.columns:
                p[c] = pd.to_numeric(p[c], errors="coerce").fillna(0).astype(int)
        if {"blank", "rows_total"} <= set(p.columns):
            empty = p["blank"] + (p["placeholder"] if "placeholder" in p.columns else 0)
            p["blank %"] = (100 * empty / p["rows_total"].where(p["rows_total"] > 0)).round(1)
        for c in ["top_values", "shapes"]:
            if c in p.columns:
                p[c] = p[c].map(_readable_list)
        order = [c for c in ["batch_id", "column_name", "rows_total", "blank %", "blank", "placeholder", "untrimmed",
                             "distinct_values", "shapes", "top_values"] if c in p.columns]
        table(p[order].rename(columns={"batch_id": "Batch", "column_name": "Column", "rows_total": "Rows",
                                       "placeholder": "Placeholders", "untrimmed": "Untrimmed",
                                       "distinct_values": "Distinct", "shapes": "Shapes",
                                       "top_values": "Most common values", "blank": "Blank"}))
    else:
        st.info("No profile for this table/batch.")

    # ---- before / after
    st.subheader(f"Before → after examples — {tsel}")
    st.caption("Each pair shows the value exactly as it arrived (left) and the cleaned value we stored (right). "
               "Taken from the rows the most recent batch added or changed. Empty right side = the value was "
               "unusable, so we blanked it and flagged the row.")
    before_after(tsel)


def _clean_partner(raw_col: str, cols: list[str]) -> str | None:
    base = raw_col[:-4]
    if base in cols:
        return base
    for c in cols:  # e.g. date_raw -> txn_date
        if c != raw_col and c.endswith("_" + base) and not c.endswith("_raw"):
            return c
    return None


def before_after(tname: str) -> None:
    stg = q(f"SELECT * FROM stg_{re.sub(r'[^A-Za-z0-9_]', '', tname)} LIMIT 5000", quiet=True)
    if not has_rows(stg):
        st.info("No staging rows for this table yet.")
        return
    cols = list(stg.columns)
    pairs = [(c, _clean_partner(c, cols)) for c in cols if c.endswith("_raw")]
    pairs = [(a, b) for a, b in pairs if b]
    if not pairs:
        st.info("This table keeps no raw copies of columns.")
    else:
        for raw_c, clean_c in pairs:
            d = pd.DataFrame({"raw": stg[raw_c].astype("string").fillna(""),
                              "clean": stg[clean_c].astype("string").fillna("")})
            d["clean"] = d["clean"].str.replace(r" 00:00:00$", "", regex=True)
            changed = d[d.raw.str.strip() != d.clean]
            st.markdown(f"**`{raw_c}` → `{clean_c}`** — {len(changed):,} of {len(d):,} rows changed")
            if len(changed):
                g = (changed.groupby(["raw", "clean"]).size().rename("rows").reset_index()
                     .sort_values("rows", ascending=False).head(15))
                g.columns = ["As received", "After cleaning", "Rows"]
                table(g)
    if "quality_flags" in stg.columns:
        flags = stg["quality_flags"].fillna("").astype(str).str.split(",").explode().str.strip()
        flags = flags[flags != ""]
        if len(flags):
            st.markdown("**Warning flags kept on accepted rows** (row kept, but marked so analysts know)")
            fc = flags.value_counts().rename_axis("flag").reset_index(name="rows")
            st.bar_chart(fc, x="flag", y="rows")


# ======================================================================================
# 3. Quarantine
# ======================================================================================
def page_quarantine() -> None:
    st.header("Quarantine — rows we refused to load")
    st.caption("Rows we cannot trust (e.g. blank Customer ID or Policy ID). Kept with the reason, never guessed.")
    qq = q("SELECT batch_id, table_name, record_key, business_key, rule_id, reason, raw_row, created_at "
           "FROM quarantine ORDER BY batch_id, table_name, record_key LIMIT 5000")
    if not has_rows(qq):
        st.info("Nothing in quarantine yet (or the pipeline has not run).")
        return
    c1, c2, c3 = st.columns(3)
    tables = c1.multiselect("Table", sorted(qq.table_name.astype(str).unique()))
    batches = c2.multiselect("Batch", sorted(qq.batch_id.astype(str).unique()))
    rules = c3.multiselect("Rule", sorted(qq.rule_id.astype(str).unique()))
    v = qq
    if tables:
        v = v[v.table_name.astype(str).isin(tables)]
    if batches:
        v = v[v.batch_id.astype(str).isin(batches)]
    if rules:
        v = v[v.rule_id.astype(str).isin(rules)]
    st.metric("Rows shown", f"{len(v):,}")
    if not len(v):
        return

    st.subheader("Rows by reason")
    by = v.groupby(["table_name", "reason"]).size().rename("rows").reset_index().sort_values("rows", ascending=False)
    c1, c2 = st.columns([2, 1])
    with c1:
        st.bar_chart(by, x="reason", y="rows", color="table_name", horizontal=True)
    with c2:
        donut(by.groupby("reason", as_index=False)["rows"].sum(), "reason", "rows")
    table(by.rename(columns={"table_name": "Table", "reason": "Reason", "rows": "Rows"}))

    st.subheader("Rejected rows")
    table(v[["batch_id", "table_name", "business_key", "reason", "rule_id", "record_key"]].rename(columns={
        "batch_id": "Batch", "table_name": "Table", "business_key": "Id", "reason": "Reason", "rule_id": "Rule",
        "record_key": "File # row"}))
    csv_button(v, "quarantine", "dl_quarantine")

    st.subheader("Look at the original records")
    st.caption("Exactly as it was in the file (first 30 of the filtered rows).")
    for _, r in v.head(30).iterrows():
        with st.expander(f"{r.table_name} · {r.business_key if pd.notna(r.business_key) and str(r.business_key) else '(no id)'} · {r.reason}"):
            st.caption(f"Source: {r.record_key} · batch {r.batch_id} · rule {r.rule_id}")
            try:
                st.json(json.loads(r.raw_row))
            except (TypeError, ValueError):
                st.code(str(r.raw_row))


# ======================================================================================
# 4. Curated model
# ======================================================================================
MODEL_TABLES = ["fact_policy", "dim_customer", "dim_policy_type", "dim_address", "dim_date"]
STAR_DOT = """digraph { rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#eef3fb", fontname="Helvetica"];
fact_policy [fillcolor="#ffe8b3", label="fact_policy\\none row per customer policy"];
dim_customer [label="dim_customer\\n(history kept: SCD2)"]; dim_policy_type [label="dim_policy_type"];
dim_address [label="dim_address"]; dim_date [label="dim_date\\npolicy start"];
dim_customer -> fact_policy; dim_policy_type -> fact_policy; dim_address -> fact_policy; dim_date -> fact_policy; }"""


def page_model() -> None:
    st.header("Curated model — the star schema")
    st.caption("fact_policy = one row per customer policy · dim_customer keeps history (SCD2) · key -1 = UNKNOWN")
    st.graphviz_chart(STAR_DOT)

    union = " UNION ALL ".join(f"SELECT '{t}' AS table_name, COUNT(*) AS row_count FROM {t}" for t in MODEL_TABLES)
    counts = q(union, quiet=True)
    if counts is None:  # at least one table missing -> count one by one
        rows = []
        for t in MODEL_TABLES:
            d = q(f"SELECT COUNT(*) AS n FROM {t}", quiet=True)
            if has_rows(d):
                rows.append({"table_name": t, "row_count": int(d.iloc[0, 0])})
        counts = pd.DataFrame(rows)
    st.subheader("Rows per table")
    if not show_or_info(counts):
        return
    cols = st.columns(len(counts))
    for col, (_, r) in zip(cols, counts.iterrows()):
        col.metric(r.table_name, f"{int(r.row_count):,}")

    st.subheader("Sample rows")
    t = st.selectbox("Table", list(counts.table_name))
    sample = q(f"SELECT * FROM {t} LIMIT 20")
    if show_or_info(sample, "This table is empty."):
        table(sample)

    st.subheader("History example (SCD Type 2)")
    multi = q("SELECT customer_id, COUNT(*) AS versions FROM dim_customer WHERE customer_key <> -1 "
              "GROUP BY customer_id HAVING COUNT(*) > 1 ORDER BY customer_id LIMIT 200")
    if not show_or_info(multi, "No customer has more than one version yet (load Day 2 to see history)."):
        return
    ids = list(multi.customer_id.astype(str))
    cid = st.selectbox(f"Customer with history ({len(ids)} found)", ids, index=0)
    versions = q(f"SELECT * FROM dim_customer WHERE customer_id = {lit(cid)} ORDER BY version")
    if has_rows(versions):
        first = [c for c in ["customer_key", "customer_id", "version", "valid_from", "valid_to", "is_current",
                             "marital_status", "customer_segment", "full_name", "batch_id"] if c in versions.columns]
        st.markdown("Each row is one version. Only one has `is_current = true`.")
        table(versions[first])
    pol = q("SELECT f.policy_id, f.policy_start_dt, f.customer_key, d.version, d.marital_status, d.valid_from, "
            "d.valid_to FROM fact_policy f JOIN dim_customer d ON f.customer_key = d.customer_key "
            f"WHERE f.customer_id = {lit(cid)} ORDER BY f.policy_start_dt LIMIT 20")
    if has_rows(pol):
        st.caption("Their policies point to the current version:")
        table(pol)


# ======================================================================================
# 5. KPIs
# ======================================================================================
KPI_DEFAULT = {
    1: ("Policy KPIs (per policy)", ["kpi01_policy_kpis"]),
    2: ("Customer KPIs (per customer)", ["kpi02_customer_kpis"]),
    3: ("Status and late payment summary", ["kpi03_status_summary"]),
    4: ("Incremental load reconciliation", ["kpi04_incremental_counts", "kpi04_customer_changes"]),
}


def kpi_number(v) -> int | None:
    m = re.search(r"\d+", str(v))
    return int(m.group()) if m else None


def kpi_list(cat: pd.DataFrame | None) -> dict[int, dict]:
    out = {n: {"title": t, "tables": list(ts), "question": "", "logic": "", "assumptions": ""}
           for n, (t, ts) in KPI_DEFAULT.items()}
    if has_rows(cat):
        for _, r in cat.iterrows():
            n = kpi_number(r.get("kpi_id"))
            if n is None:
                continue
            e = out.setdefault(n, {"title": f"KPI {n}", "tables": [], "question": "", "logic": "", "assumptions": ""})
            for f in ["title", "question", "logic", "assumptions"]:
                if pd.notna(r.get(f)) and str(r.get(f)).strip():
                    e[f] = str(r.get(f))
            extra = [t for t in re.split(r"[,;\s]+", str(r.get("table_name") or "")) if t]
            e["tables"] = list(dict.fromkeys(extra + e["tables"]))
    return dict(sorted(out.items()))


def kpi_chart(name: str, df: pd.DataFrame) -> None:
    """A simple chart where it helps. Column names are detected, so a renamed column never breaks the page."""
    if len(df) < 1:
        return
    num = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_bool_dtype(df[c])]
    txt = [c for c in df.columns if c not in num]
    if not num:
        if name.startswith("kpi05") and find_col(df, "reason"):  # count of each risk reason
            rc = find_col(df, "reason")
            s = df[rc].fillna("").astype(str).str.split(r"[,;|]").explode().str.strip()
            s = s[s != ""].value_counts().rename_axis("reason").reset_index(name="transactions")
            st.bar_chart(s, x="reason", y="transactions", horizontal=True)
        return
    month = find_col(df, "month", "period")
    if name.startswith("kpi02") and month:
        group = find_col(df, "region", "branch", exclude=(month,), numeric=False)
        metric = st.radio("Show", [c for c in num if "key" not in c.lower()], horizontal=True, key=f"m_{name}")
        st.line_chart(df.sort_values(month), x=month, y=metric, color=group)
        return
    if name.startswith("kpi06"):
        seg = find_col(df, "segment", numeric=False)
        if seg:
            cnt = find_col(df, "customers", "count", numeric=True)
            data = (df[[seg, cnt]] if cnt and df[seg].is_unique
                    else df[seg].value_counts().rename_axis(seg).reset_index(name="customers"))
            st.bar_chart(data, x=seg, y=data.columns[1])
            return
    if name.startswith("kpi04"):
        flag = find_col(df, "risk", "flag", "status", numeric=False)
        if flag:
            st.bar_chart(df[flag].fillna("(none)").astype(str).value_counts().rename_axis(flag)
                         .reset_index(name="accounts"), x=flag, y="accounts")
        return
    if name.startswith("kpi05"):
        rc = find_col(df, "reason", numeric=False)
        if rc:
            s = df[rc].fillna("").astype(str).str.split(r"[,;|]").explode().str.strip()
            s = s[s != ""].value_counts().rename_axis("reason").reset_index(name="transactions")
            st.bar_chart(s, x="reason", y="transactions", horizontal=True)
        return
    if name.startswith("kpi11") or name.startswith("kpi12") or name.startswith("kpi13"):
        return  # shown as tables on the Day 2 page as well; charts add little
    # share / value bars (KPI 1, 3, 7, 8, 9, 10 and anything else)
    label = None
    for p in ("customer_name", "customer_id", "product_name", "product_type", "category", "branch_name", "branch",
              "file", "table_name", "product", "region", "kyc", "segment", "name"):
        label = find_col(df, p, numeric=False)
        if label:
            break
    label = label or (txt[0] if txt else None)
    if not label:
        return
    prefer = [c for c in num if re.search(r"share|pct|percent|rate", c.lower())] or \
             [c for c in num if re.search(r"net_value|net_revenue", c.lower())] or \
             [c for c in num if re.search(r"net|value|revenue|amount", c.lower())] or num
    ys = prefer[:1]  # one clear measure per chart
    color = None
    if name.startswith("kpi07"):
        color = find_col(df, "region", numeric=False, exclude=(label,))
        ys = ys[:1]
    data = df.copy()
    data[label] = data[label].astype(str)
    if data[label].duplicated().any() and not color:
        data = data.groupby(label, as_index=False)[ys].sum()
    data = data.head(40)
    if color:
        st.bar_chart(data, x=label, y=ys[0], color=color, horizontal=True)
    else:
        st.bar_chart(data, x=label, y=ys, horizontal=True, stack=False)


def page_kpis() -> None:
    st.header("KPIs")
    cat = q("SELECT * FROM kpi_catalog", quiet=True)
    kpis = kpi_list(cat)
    labels = {n: f"KPI {n} — {e['title']}" for n, e in kpis.items()}
    n = st.selectbox("Choose a KPI", list(kpis), format_func=lambda k: labels[k])
    e = kpis[n]
    with st.container(border=True):
        if e["question"]:
            st.markdown(f"**Question:** {e['question']}")
        if e["logic"]:
            st.markdown(f"**How we calculate it:** {e['logic']}")
        if e["assumptions"]:
            st.markdown(f"**Assumptions:** {e['assumptions']}")
        if not (e["question"] or e["logic"] or e["assumptions"]):
            st.caption("Description appears here after the pipeline has run (from kpi_catalog).")
    any_data = False
    for t in e["tables"]:
        df = q(f"SELECT * FROM {t} LIMIT 5000", quiet=True)
        if df is None:
            continue
        any_data = True
        st.subheader(t)
        if not len(df):
            st.info("The KPI ran but returned no rows.")
            continue
        kpi_cards(df)
        df = kpi_filters(df, f"f_{t}")
        try:
            kpi_chart(t, df)
        except Exception as exc:  # noqa: BLE001 - a chart must never hide the table
            st.caption(f"(chart skipped: {exc})")
        table(df)
        csv_button(df, t, f"dl_{t}")
    if not any_data:
        st.info(NO_DATA)


# ======================================================================================
# 6. Day 2 (incremental)
# ======================================================================================
COLORS = {"Active": "#2e7d32", "Lapsed": "#c62828", "Expired": "#9e9e9e", "Slightly late": "#f9a825",
          "Moderately late": "#ef6c00", "Severely late": "#c62828", "On time": "#2e7d32"}
LATE_ORDER = ["On time", "Slightly late", "Moderately late", "Severely late"]


def _bar(df: pd.DataFrame, x: str, y: str, title: str, color: str | None = None, order: list | None = None,
         horizontal: bool = False) -> None:
    if go is None or not len(df):
        return
    if color:
        fig = go.Figure()
        for k, g in df.groupby(color):
            fig.add_bar(x=g[y] if horizontal else g[x], y=g[x] if horizontal else g[y], name=str(k),
                        orientation="h" if horizontal else "v", marker_color=COLORS.get(str(k)))
        fig.update_layout(barmode="stack")
    else:
        fig = go.Figure(go.Bar(x=df[y] if horizontal else df[x], y=df[x] if horizontal else df[y],
                               orientation="h" if horizontal else "v", text=df[y], textposition="auto",
                               marker_color=[COLORS.get(str(v), "#1565c0") for v in df[x]]))
    if order:
        fig.update_xaxes(categoryorder="array", categoryarray=order)
    fig.update_layout(title=title, height=320, margin=dict(l=10, r=10, t=40, b=10), legend_title_text="")
    st.plotly_chart(fig, use_container_width=True)


def _hist(s: pd.Series, title: str, bins: int = 20) -> None:
    if go is None or not len(s.dropna()):
        return
    fig = go.Figure(go.Histogram(x=s.dropna(), nbinsx=bins, marker_color="#1565c0"))
    fig.update_layout(title=title, height=300, margin=dict(l=10, r=10, t=40, b=10), bargap=0.05)
    st.plotly_chart(fig, use_container_width=True)


def page_insurance() -> None:
    st.header("🛡️ ABC Insurance — policy KPIs")
    pol = q("SELECT * FROM kpi01_policy_kpis", quiet=True)
    cus = q("SELECT * FROM kpi02_customer_kpis", quiet=True)
    if not has_rows(pol):
        st.info(NO_DATA)
        return
    c1, c2, c3 = st.columns(3)
    regions = c1.multiselect("Region", sorted(pol.region.dropna().astype(str).unique()))
    types = c2.multiselect("Policy type", sorted(pol.policy_type.dropna().astype(str).unique()))
    stats = c3.multiselect("Status", sorted(pol.policy_status.dropna().astype(str).unique()))
    f = pol
    if regions:
        f = f[f.region.astype(str).isin(regions)]
    if types:
        f = f[f.policy_type.astype(str).isin(types)]
    if stats:
        f = f[f.policy_status.astype(str).isin(stats)]
    if not len(f):
        st.info("No policies match the filters.")
        return

    m = st.columns(6)
    m[0].metric("Policies", f"{len(f):,}")
    m[1].metric("Customers", f"{f.customer_id.nunique():,}")
    m[2].metric("Active", f"{(f.policy_active_flag == 1).mean() * 100:.0f}%")
    m[3].metric("Lapsed", f"{(f.policy_status == 'Lapsed').sum():,}")
    m[4].metric("Missed payments", f"{int(f.is_missed_payment.sum()):,}")
    m[5].metric("Avg premium paid", f"{f.total_premium_paid_pct.mean():.1f}%")

    a, b = st.columns(2)
    with a:
        donut(f.policy_status.value_counts().rename_axis("status").reset_index(name="policies"),
              "status", "policies", "Policy status")
    with b:
        late = f.late_category.fillna("Not due").value_counts().rename_axis("category").reset_index(name="policies")
        _bar(late, "category", "policies", "Late payment category", order=LATE_ORDER + ["Not due"])

    a, b = st.columns(2)
    with a:
        rs = f.groupby(["region", "policy_status"]).size().reset_index(name="policies")
        _bar(rs, "region", "policies", "Status by region", color="policy_status")
    with b:
        pt = (f.groupby("policy_type_name").agg(premium=("premium_amt", "sum")).reset_index()
              .sort_values("premium"))
        _bar(pt, "policy_type_name", "premium", "Premium by policy type", horizontal=True)

    a, b, c = st.columns(3)
    with a:
        _hist(f.age_at_issuance, "Age at policy issuance (years)")
    with b:
        _hist(f.total_premium_paid_pct, "Total premium paid %")
    with c:
        paid = f[f.actual_premium_paid_dt.notna()]
        _hist(paid.days_late, "Days late (paid premiums)")

    if has_rows(cus):
        cf = cus[cus.customer_id.isin(f.customer_id)]
        a, b = st.columns(2)
        with a:
            _hist(cf.customer_tenure_years, "Customer tenure (years)")
        with b:
            mc = cf.missed_payments_count.fillna(0).astype(int).value_counts().sort_index()
            _bar(mc.rename_axis("missed").reset_index(name="customers").astype({"missed": str}),
                 "missed", "customers", "Customers by missed payments")

    with st.expander("Policy details"):
        table(f)
        csv_button(f, "kpi01_policy_kpis", "dl_ins_pol")


def page_day2() -> None:
    st.header("Day over day — incremental load")
    inc = q("SELECT * FROM kpi04_incremental_counts ORDER BY batch_id, table_name", quiet=True)
    chg = q("SELECT * FROM kpi04_customer_changes ORDER BY batch_id, customer_id", quiet=True)
    if not has_rows(inc):
        st.info(NO_DATA)
        return
    t = inc[inc.table_name == "customer_policies"] if "customer_policies" in set(inc.table_name) else inc
    last = t.iloc[-1]
    m = st.columns(4)
    m[0].metric(f"New ({last.batch_id})", f"{int(last.new_records):,}")
    m[1].metric("Updated", f"{int(last.updated_records):,}")
    m[2].metric("Quarantined", f"{int(last.quarantined_rows):,}")
    m[3].metric("Duplicates", f"{int(last.duplicate_rows):,}")
    if go is not None:
        fig = go.Figure()
        for col, color in [("new_records", "#2e7d32"), ("updated_records", "#1565c0"),
                           ("quarantined_rows", "#c62828"), ("duplicate_rows", "#9e9e9e")]:
            fig.add_bar(x=t.batch_id, y=t[col], name=col.replace("_", " "), marker_color=color)
        fig.update_layout(barmode="group", height=320, title="Policy records per batch",
                          margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)
    if has_rows(chg):
        latest = chg[chg.batch_id == chg.batch_id.max()]
        a, b = st.columns([1, 2])
        with a:
            donut(latest.change_type.value_counts().rename_axis("type").reset_index(name="customers"),
                  "type", "customers", f"Customers in {latest.batch_id.iloc[0]}")
        with b:
            show = latest.copy()
            show["marital status"] = (show.old_marital_status.fillna("—").astype(str) + " → "
                                      + show.new_marital_status.fillna("—").astype(str))
            table(show[["customer_id", "change_type", "version", "marital status"]])


# ======================================================================================
# 7. Pipeline runs & checks
# ======================================================================================
def page_runs() -> None:
    st.header("Pipeline runs & checks")
    runs = q("SELECT run_id, batch_id, step, status, started_at, finished_at, message FROM pipeline_runs "
             "ORDER BY started_at DESC LIMIT 1000")
    st.subheader("Runs")
    if show_or_info(runs):
        r = runs.copy()
        r["started_at"] = pd.to_datetime(r.started_at, errors="coerce")
        r["finished_at"] = pd.to_datetime(r.finished_at, errors="coerce")
        r["seconds"] = (r.finished_at - r.started_at).dt.total_seconds().round(1)
        summary = (r.groupby("run_id")
                   .agg(batch=("batch_id", "first"), started=("started_at", "min"), finished=("finished_at", "max"),
                        steps=("step", "count"),
                        result=("status", lambda s: "❌ failed" if s.astype(str).str.lower().str.contains(
                            "fail|error").any() else "✅ ok"))
                   .reset_index().sort_values("started", ascending=False))
        summary["minutes"] = ((summary.finished - summary.started).dt.total_seconds() / 60).round(1)
        table(summary[["run_id", "batch", "result", "started", "finished", "minutes", "steps"]])
        run_id = st.selectbox("Run details", list(summary.run_id))
        steps = r[r.run_id == run_id].sort_values("started_at")
        steps = steps.assign(result=steps.status.map(status_icon))
        st.markdown("**Steps of this run** (timeline)")
        if steps.seconds.notna().any():
            st.bar_chart(steps, x="step", y="seconds", horizontal=True)
        table(steps[["step", "result", "started_at", "finished_at", "seconds", "message"]])
        checks = q(f"SELECT check_name, status, detail, checked_at FROM check_results WHERE run_id = {lit(run_id)} "
                   "ORDER BY checked_at, check_name")
        st.subheader("Automatic checks for this run")
        st.caption("If any check fails, the run fails. They prove: no row lost, ids unique, every link resolves, "
                   "one current version per dimension key, KPI 1 matches a hand calculation.")
        if show_or_info(checks, "No checks recorded for this run."):
            c = checks.assign(result=checks.status.map(check_icon))
            passed = int(c.result.str.startswith("✅").sum())
            (st.success if passed == len(c) else st.error)(f"{passed} of {len(c)} checks passed")
            table(c[["result", "check_name", "detail", "checked_at"]])

    st.subheader("Files received (ingestion log)")
    st.caption("sha256 = fingerprint of the file; the same file always gives the same value, so we can prove "
               "what we loaded.")
    ing = q("SELECT * FROM ingestion_log ORDER BY batch_id, table_name")
    if show_or_info(ing):
        cols = [c for c in ["batch_id", "table_name", "file_name", "rows_in", "columns_in", "sha256", "loaded_at",
                            "run_id"] if c in ing.columns]
        table(ing[cols])


# ======================================================================================
# 8. Run pipeline
# ======================================================================================
def page_run() -> None:
    st.header("Run the pipeline")
    arn = os.environ.get("STATE_MACHINE_ARN", "").strip()
    if not arn:
        st.markdown("Running **locally**. Start a batch from a terminal in the project folder:")
        st.code("python jobs/run_local.py --batch day1\npython jobs/run_local.py --batch day2", language="bash")
        st.caption("Then press **Refresh data** in the sidebar. On AWS (STATE_MACHINE_ARN set) this page gets "
                   "a start button.")
        return
    try:
        import boto3
        sfn = boto3.client("stepfunctions", region_name=os.environ.get("AWS_REGION") or None)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Cannot create the AWS client: {exc}")
        return
    st.caption(f"State machine: `{arn}`")
    c1, c2 = st.columns([2, 1])
    batch = c1.text_input("Batch (folder under raw/)", "day1").strip()
    if c2.button(f"▶ Start pipeline for batch {batch}", type="primary", disabled=not re.fullmatch(r"[\w.-]+", batch)):
        try:
            res = sfn.start_execution(stateMachineArn=arn, input=json.dumps({"batch": batch}))
            st.success(f"Started: {res['executionArn'].split(':')[-1]}")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Could not start: {exc}")
    st.subheader("Recent executions")
    if st.button("Refresh list"):
        st.rerun()
    try:
        ex = sfn.list_executions(stateMachineArn=arn, maxResults=15).get("executions", [])
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not list executions: {exc}")
        return
    if not ex:
        st.info("No executions yet.")
        return
    icon = {"SUCCEEDED": "✅", "FAILED": "❌", "RUNNING": "⏳", "TIMED_OUT": "⌛", "ABORTED": "⛔"}
    table(pd.DataFrame([{"execution": e["name"], "status": f"{icon.get(e['status'], '')} {e['status']}",
                         "started": e.get("startDate"), "stopped": e.get("stopDate")} for e in ex]))


# ======================================================================================
# Data journey / Pipeline map / Reports  (step_io, dq_expectations, generated reports)
# ======================================================================================
STEP_TITLES = {"bronze": "Bronze — files stored exactly as received",
               "silver": "Silver — cleaned, checked, rejects to quarantine",
               "gold": "Gold — loaded into the warehouse (staging, dimensions, facts)",
               "kpis": "KPIs — result tables built from the curated tables"}


def _reports_mod():
    try:
        from engine import reports
        return reports
    except Exception:  # noqa: BLE001 - engine not importable -> pages degrade to plain tables
        return None


def _parse_raw(v) -> dict:
    try:
        obj = json.loads(v)
        return obj if isinstance(obj, dict) else {"raw": v}
    except (TypeError, ValueError):
        return {"raw": v}


def _project_table_spec(tname: str) -> dict:
    try:
        from engine import config
        return (config.tables() or {}).get(tname) or {}
    except Exception:  # noqa: BLE001 - no project config available
        return {}


def classify_rules(r: pd.DataFrame, tname: str) -> pd.Series:
    """'derived column' / 'row rule' / 'cleaning' per rules_log row (SPEC v2 A: derived action starts with
    'calculated'; row rules have rule_id <table>.<id> for an id listed under row_rules)."""
    spec = _project_table_spec(tname)
    rr_ids = {f"{tname}.{x.get('id')}" for x in (spec.get("row_rules") or []) if isinstance(x, dict)}
    derived = set((spec.get("derived") or {}).keys()) if isinstance(spec.get("derived"), dict) else set()

    def kind(row):
        action = str(row.get("action") or "").lower()
        if action.startswith("calculated") or (derived and str(row.get("column_name")) in derived
                                                 and "calculat" in action):
            return "derived column"
        if str(row.get("rule_id")) in rr_ids:
            return "row rule"
        return "cleaning"
    return r.apply(kind, axis=1)


def _flow_header(n: int, title: str, caption: str = "") -> None:
    st.markdown(f"### {n} · {title}")
    if caption:
        st.caption(caption)


def page_journey() -> None:
    st.header("Data journey — follow one table through the pipeline")
    st.caption("Pick a source table and a batch. Read top to bottom: what arrived, what was wrong, how we fixed it, "
               "where the clean rows went, what we refused and why, how we checked it, and which KPIs use it.")
    sio = q("SELECT * FROM step_io", quiet=True)
    rc = q("SELECT * FROM row_counts", quiet=True)
    ing = q("SELECT batch_id, table_name, file_name, rows_in, columns_in, sha256 FROM ingestion_log", quiet=True)
    if not (has_rows(sio) or has_rows(rc) or has_rows(ing)):
        st.info(NO_DATA)
        return
    batches = batches_of(sio, rc, ing)
    src_tables = set()
    for d in (rc, ing):
        if has_rows(d):
            src_tables |= set(d.table_name.astype(str))
    if has_rows(sio):
        src_tables |= set(sio[sio.step.astype(str).isin(["bronze", "silver"])].table_name.astype(str))
    tables = sorted(src_tables)
    if not tables or not batches:
        st.info(NO_DATA)
        return
    c1, c2 = st.columns(2)
    tname = c1.selectbox("Source table", tables, key="journey_table")
    batch = c2.selectbox("Batch", batches, index=len(batches) - 1, key="journey_batch")
    b, t = lit(batch), lit(tname)
    s_b = sio[sio.batch_id.astype(str) == batch] if has_rows(sio) else pd.DataFrame()
    rep = _reports_mod()
    cat = q("SELECT kpi_id, title, table_name FROM kpi_catalog", quiet=True)
    j = rep.journey_for_table(s_b, tname, cat) if rep is not None and len(s_b) else None

    # 1 raw input
    _flow_header(1, "Raw input — what we received", "Every file is kept untouched (bronze), so it can be replayed.")
    fi = ing[(ing.batch_id.astype(str) == batch) & (ing.table_name.astype(str) == tname)] if has_rows(ing) else None
    if has_rows(fi):
        c = st.columns(3)
        c[0].metric("Files", len(fi))
        c[1].metric("Rows received", f"{int(pd.to_numeric(fi.rows_in, errors='coerce').fillna(0).sum()):,}")
        c[2].metric("Columns", len([x for x in str(fi.columns_in.iloc[0] or '').split(",") if x]))
        table(fi[["file_name", "rows_in", "columns_in"]].rename(columns={
            "file_name": "File", "rows_in": "Rows", "columns_in": "Columns as received"}))
    else:
        st.info("No file record for this table in this batch.")
    raw = q(f"SELECT record_key, reason, raw_row FROM quarantine WHERE batch_id = {b} AND table_name = {t} "
            "ORDER BY record_key LIMIT 8", quiet=True)
    if has_rows(raw):
        st.markdown("**Sample of raw rows with problems** (exactly as in the file; the reason is why we refused it)")
        sample = pd.DataFrame([{"File # row": r.record_key, "Problem": r.reason, **_parse_raw(r.raw_row)}
                               for r in raw.itertuples()])
        st.dataframe(sample.astype(str), hide_index=True, use_container_width=True)
    else:
        st.caption("No raw row of this table was rejected in this batch — the problems that were fixed are below.")

    # 2 what was wrong
    _flow_header(2, "What was wrong", "Measured on the raw file before cleaning, plus every problem a rule found.")
    prof = q(f"SELECT * FROM profile_columns WHERE batch_id = {b} AND table_name = {t}", quiet=True)
    rules = q(f"SELECT rule_id, column_name, issue, action, rows_affected FROM rules_log WHERE batch_id = {b} "
              f"AND table_name = {t}", quiet=True)
    if has_rows(prof):
        p = prof.copy()
        for c in ["rows_total", "blank", "placeholder", "untrimmed", "distinct_values"]:
            p[c] = pd.to_numeric(p[c], errors="coerce").fillna(0).astype(int) if c in p else 0
        p["formats"] = p["shapes"].map(rep.count_items) if rep is not None and "shapes" in p else 0
        bad = p[(p.blank + p.placeholder > 0) | (p.untrimmed > 0) | (p.formats > 1)]
        if len(bad):
            show = pd.DataFrame({
                "Column": bad.column_name, "Empty / placeholder": bad.blank + bad.placeholder,
                "Extra spaces": bad.untrimmed, "Formats": bad.formats,
                "Formats seen": bad.shapes.map(rep.readable_counts if rep is not None else _readable_list)})
            st.markdown(f"**{len(bad)} of {len(p)} columns look messy in the raw file**")
            table(show)
        else:
            st.success(f"All {len(p)} columns look tidy in the raw file.")
    if has_rows(rules):
        r = rules.copy()
        r["rows_affected"] = pd.to_numeric(r.rows_affected, errors="coerce").fillna(0).astype(int)
        r = r[r.rows_affected > 0].sort_values("rows_affected", ascending=False)
        if len(r):
            st.markdown("**Problems found**")
            for _, it in r.head(8).iterrows():
                st.markdown(f"- {it.issue} — **{int(it.rows_affected):,}** row(s)")
            if len(r) > 8:
                st.caption(f"… and {len(r) - 8} more (all listed in step 3).")
    if not (has_rows(prof) or has_rows(rules)):
        st.info("No profile or rule information for this table and batch.")

    # 3 how we cleaned it
    _flow_header(3, "How we cleaned it", "Each rule: what was wrong → what we did → how many rows.")
    if has_rows(rules):
        r = rules.copy()
        r["rows_affected"] = pd.to_numeric(r.rows_affected, errors="coerce").fillna(0).astype(int)
        r = r[r.rows_affected > 0]
        r["kind"] = classify_rules(r, tname) if len(r) else []
        for kind, label in (("cleaning", "Cleaning rules"), ("derived column", "Derived (calculated) columns"),
                            ("row rule", "Row rules (business checks on whole rows)")):
            part = r[r.kind == kind].sort_values("rows_affected", ascending=False) if len(r) else r
            if len(part):
                st.markdown(f"**{label}**")
                table(part[["issue", "action", "column_name", "rows_affected", "rule_id"]].rename(columns={
                    "issue": "What was wrong", "action": "What we did", "column_name": "Column",
                    "rows_affected": "Rows", "rule_id": "Rule id"}))
        if not len(r):
            st.success("No rule had to change anything in this table.")
    else:
        st.info("No rules recorded for this table and batch.")

    # 4 where the clean data went
    _flow_header(4, "Where the clean data went", "Storage paths and warehouse tables, with row counts.")
    if j and (j["silver"] or j["staging"] or j["curated"]):
        steps = []
        for r in j["bronze"]:
            steps.append({"Step": "bronze", "From": r.get("input_location"), "Rows in": r.get("rows_in"),
                          "To": r.get("output_location"), "Rows out": r.get("rows_out")})
        for r in j["silver"]:
            steps.append({"Step": "silver", "From": r.get("input_location"), "Rows in": r.get("rows_in"),
                          "To": r.get("output_location"), "Rows out": r.get("rows_out")})
        for r in j["staging"]:
            steps.append({"Step": "warehouse staging", "From": r.get("input_location"), "Rows in": r.get("rows_in"),
                          "To": r.get("output_location"), "Rows out": r.get("rows_out")})
        stg = {rep.wh_table(r.get("output_location")) for r in j["staging"]}
        for name, n in j["curated"]:
            steps.append({"Step": "curated table", "From": ", ".join(sorted(f"warehouse.{x}" for x in stg if x)),
                          "Rows in": None, "To": f"warehouse.{name}", "Rows out": n})
        for name, n in j["downstream"]:
            steps.append({"Step": "then feeds", "From": "(tables above)", "Rows in": None,
                          "To": f"warehouse.{name}", "Rows out": n})
        table(pd.DataFrame(steps))
        st.caption("For warehouse tables *Rows out* = rows the table holds after this batch (history included).")
    else:
        rr = rc[(rc.batch_id.astype(str) == batch) & (rc.table_name.astype(str) == tname)] if has_rows(rc) else None
        if has_rows(rr):
            x = rr.iloc[0]
            st.markdown(f"- `silver/{tname}/` — **{int(x.accepted):,}** clean rows accepted "
                        f"({int(x.inserted):,} new, {int(x.updated):,} changed, {int(x.unchanged):,} unchanged)\n"
                        f"- warehouse `stg_{tname}` — the {int(x.inserted) + int(x.updated):,} new or changed rows")
        else:
            st.info("No step records yet for this table and batch.")

    # 5 rejected
    _flow_header(5, "What was rejected and why", "Rows we could not trust go to quarantine with a plain reason. "
                                                   "We never guess.")
    by = q(f"SELECT reason, rule_id, COUNT(*) AS n FROM quarantine WHERE batch_id = {b} AND table_name = {t} "
           "GROUP BY reason, rule_id ORDER BY n DESC", quiet=True)
    if has_rows(by):
        st.metric("Rows quarantined", f"{int(by.n.sum()):,}")
        table(by.rename(columns={"reason": "Reason", "rule_id": "Rule", "n": "Rows"}))
    else:
        st.success("Nothing from this table was quarantined in this batch.")

    # 6 validation
    _flow_header(6, "Validation", "Expectations on the cleaned table (silver) and warehouse checks (gold).")
    exp = q(f"SELECT expectation, column_name, severity, status, failing_rows, detail FROM dq_expectations "
            f"WHERE batch_id = {b} AND table_name = {t}", quiet=True)
    if has_rows(exp):
        e = exp.assign(result=exp.status.map(lambda s: {"PASS": "✅ pass", "FAIL": "❌ fail", "WARN": "⚠️ warn"}
                                             .get(str(s).upper(), str(s))))
        ok = int((e.status.astype(str).str.upper() == "PASS").sum())
        (st.success if ok == len(e) else st.warning)(f"{ok} of {len(e)} expectations passed")
        table(e[["result", "expectation", "column_name", "severity", "failing_rows", "detail"]])
    else:
        st.info("No expectation results recorded for this table and batch.")
    chk = q(f"SELECT run_id, check_name, status, detail, checked_at FROM check_results WHERE batch_id = {b}",
            quiet=True)
    if has_rows(chk):
        c = chk.copy()
        c["checked_at"] = pd.to_datetime(c.checked_at, errors="coerce")
        c = c[c.run_id == c.sort_values("checked_at").iloc[-1].run_id]
        mine = c[c.check_name.astype(str).str.contains(tname, case=False, regex=False)]
        passed = int(c.status.map(lambda s: check_icon(s).startswith("✅")).sum())
        st.markdown(f"**Warehouse checks for this batch:** {passed} of {len(c)} passed"
                    + (" ✅" if passed == len(c) else " ❌"))
        if len(mine):
            table(mine.assign(result=mine.status.map(check_icon))[["result", "check_name", "detail"]])

    # 7 used in
    _flow_header(7, "Used in", "KPI tables built from this table's warehouse tables (from the recorded lineage).")
    if j and j["kpis"]:
        direct = [k for k in j["kpis"] if k["direct"]]
        indirect = [k for k in j["kpis"] if not k["direct"]]
        if direct:
            st.markdown("**Directly**\n\n" + "\n".join(f"- {x}" for x in rep.kpi_lines(direct)))
        if indirect:
            st.markdown("**Through other tables** (e.g. the fact table): "
                        + ", ".join(sorted({f"KPI {k['kpi_id']}" if k["kpi_id"] else k["table"] for k in indirect})))
    elif has_rows(cat):
        st.info("Lineage not recorded yet for this batch. KPI tables in the catalog: "
                + ", ".join(sorted(set(str(x) for x in cat.table_name))))
    else:
        st.info(NO_DATA)


def page_pipeline_map() -> None:
    st.header("Pipeline map — every step's inputs → outputs")
    st.caption("One line per input → output of every step, with row counts. Read top to bottom: bronze → silver → "
               "gold → KPIs.")
    sio = q("SELECT * FROM step_io", quiet=True)
    if not has_rows(sio):
        st.info(NO_DATA)
        return
    batches = batches_of(sio)
    batch = st.selectbox("Batch", batches, index=len(batches) - 1, key="map_batch")
    s = sio[sio.batch_id.astype(str) == batch].copy()
    order = {k: i for i, k in enumerate(STEP_TITLES)}
    s["_o"] = s.step.astype(str).map(order).fillna(9)
    s = s.sort_values(["_o", "table_name", "input_location", "output_location"])
    c = st.columns(4)
    for col, step_name in zip(c, STEP_TITLES):
        part = s[s.step.astype(str) == step_name]
        col.metric(step_name.capitalize(), f"{len(part)} step(s)")
    # source-level flow chart (KPI edges summarised so the picture stays readable)
    rep = _reports_mod()
    srcs = sorted(set(s[s.step.astype(str).isin(["bronze", "silver"])].table_name.astype(str)))
    if rep is not None and srcs:
        lines = ['digraph { rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#eef3fb", '
                 'fontname="Helvetica", fontsize=10];']
        for t in srcs:
            jj = rep.journey_for_table(s, t)
            chain = [f"raw: {t}"] + [f"bronze/{t}"] * bool(jj["bronze"]) + [f"silver/{t}"] * bool(jj["silver"])
            chain += [rep.wh_table(r.get("output_location")) for r in jj["staging"]]
            for a, z in zip(chain, chain[1:]):
                lines.append(f'"{a}" -> "{z}";')
            last = chain[-1]
            for name, _ in jj["curated"]:
                lines.append(f'"{last}" -> "{name}";')
            if jj["kpis"]:
                lines.append(f'"{t}_kpis" [label="{len(jj["kpis"])} KPI tables", fillcolor="#ffe8b3"];')
                for name, _ in (jj["curated"] or [(last, None)])[:3]:
                    lines.append(f'"{name}" -> "{t}_kpis";')
        lines.append("}")
        try:
            st.graphviz_chart("\n".join(lines))
        except Exception as exc:  # noqa: BLE001 - the tables below carry the same information
            st.caption(f"(diagram skipped: {exc})")
    for step_name, title in STEP_TITLES.items():
        part = s[s.step.astype(str) == step_name]
        if not len(part):
            continue
        st.subheader(title)
        table(part[["table_name", "input_location", "rows_in", "output_location", "rows_out", "rows_rejected",
                    "notes"]].rename(columns={"table_name": "Table", "input_location": "Input", "rows_in": "Rows in",
                                              "output_location": "Output", "rows_out": "Rows out",
                                              "rows_rejected": "Rejected", "notes": "Notes"}))
    csv_button(s.drop(columns="_o"), f"step_io_{batch}", "dl_step_io")


def _stored_reports(batch: str) -> dict[str, dict[str, str]] | None:
    """Reports gold wrote to DATA_ROOT/reports/<batch>/ -> {name: {'md': text, csv_name: text}}; None if unreachable."""
    if not os.environ.get("DATA_ROOT"):
        return None
    try:
        from engine import io as storage
        files = storage.list_files(f"reports/{batch}")
        if not files:
            return None
        out: dict[str, dict[str, str]] = {}
        for f in files:
            stem, ext = f.rsplit(".", 1) if "." in f else (f, "")
            name, _, part = stem.partition("__")
            if ext not in ("md", "csv") or name == "README":
                continue
            out.setdefault(name, {})["md" if ext == "md" else part] = \
                storage.read_bytes(f"reports/{batch}/{f}").decode("utf-8")
        return out or None
    except Exception:  # noqa: BLE001 - storage not reachable from the dashboard -> generate live
        return None


def page_reports() -> None:
    st.header("Reports")
    st.caption("Profiling, data quality, reconciliation and data journey reports — written by the pipeline after "
               "every batch to `reports/<batch>/` (and `reports/latest/`) as Markdown with CSV twins.")
    rc = q("SELECT DISTINCT batch_id FROM row_counts", quiet=True)
    sio = q("SELECT DISTINCT batch_id FROM step_io", quiet=True)
    batches = batches_of(rc, sio)
    rep = _reports_mod()
    if not batches or rep is None:
        st.info(NO_DATA)
        return
    c1, c2 = st.columns(2)
    batch = c1.selectbox("Batch", batches, index=len(batches) - 1, key="reports_batch")
    name = c2.selectbox("Report", rep.REPORT_NAMES, format_func=lambda n: rep.REPORT_TITLES[n], key="reports_name")
    stored = _stored_reports(batch)
    if stored and name in stored and "md" in stored[name]:
        st.caption(f"Showing the file written by the pipeline: `reports/{batch}/{name}.md`")
        md, csvs = stored[name]["md"], {k: v for k, v in stored[name].items() if k != "md"}
    else:
        try:
            r = rep.BUILDERS[name](_warehouse(), batch)
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not build the report: {exc}")
            return
        st.caption("Generated live from the warehouse (the report files are not reachable from the dashboard).")
        md, csvs = r.markdown, {k: v.to_csv(index=False) for k, v in r.tables.items()}
    cols = st.columns(1 + max(1, len(csvs)))
    cols[0].download_button("⬇️ Markdown", md.encode("utf-8"), file_name=f"{name}_{batch}.md",
                            mime="text/markdown", key=f"dl_md_{name}")
    for col, (part, text) in zip(cols[1:], sorted(csvs.items())):
        col.download_button(f"⬇️ {part}.csv", text.encode("utf-8"), file_name=f"{name}__{part}_{batch}.csv",
                            mime="text/csv", key=f"dl_csv_{name}_{part}")
    with st.container(border=True):
        st.markdown(md)


# ======================================================================================
# Main
# ======================================================================================
PAGES = {
    "Insurance dashboard": page_insurance,
    "Overview": page_overview,
    "Data journey": page_journey,
    "Pipeline map": page_pipeline_map,
    "Data issues (EDA)": page_issues,
    "Quarantine": page_quarantine,
    "Curated model": page_model,
    "KPIs": page_kpis,
    "Day over day": page_day2,
    "Pipeline runs & checks": page_runs,
    "Reports": page_reports,
    "Run pipeline": page_run,
}


def main() -> None:
    password_gate()
    st.sidebar.title("🛡️ ABC Insurance")
    choice = st.sidebar.radio("Page", list(PAGES), key="page")
    st.sidebar.divider()
    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()
    st.sidebar.caption(f"mode: {os.environ.get('RUN_MODE', 'local')} · warehouse: "
                       f"{os.environ.get('WAREHOUSE', 'postgres')} · data cached 60 s")
    try:
        _warehouse()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Cannot connect to the warehouse: {type(exc).__name__}: {exc}")
        st.stop()
    PAGES[choice]()


main()
