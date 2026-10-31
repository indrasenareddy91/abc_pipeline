"""Data-quality expectations on a cleaned (silver) table - config `tables.<t>.expectations` (SPEC v2 A).

  - {type: not_null, column: c}                      rows where c is NULL fail
  - {type: unique, column: c}                        rows whose (non-NULL) c appears more than once fail
                                                     (column may also be a list -> combined uniqueness)
  - {type: range, column: c, min: 1, max: 1000}      non-NULL values below min / above max fail (numbers or dates)
  - {type: accepted_values, column: c, values: [..]} non-NULL values outside the list fail
  - {type: exists_in, column: c, table: p, parent_column: pc}   non-NULL values missing in p.pc fail
                                                     (parent_column defaults to p's key)
  - {type: row_count_min, value: n}                  the table has fewer than n rows
  - {type: condition, name: x, condition: "<Spark SQL boolean>"}  rows where it is FALSE fail (NULL = not judged)
  every one may add  severity: error (default; FAIL stops the run after outputs are written) | warn (-> WARN)

evaluate(...) returns one result dict per expectation (columns = lineage.EXPECTATION_COLUMNS). Row-level checks are
counted in ONE aggregate per table; unique / exists_in need one extra job each."""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _c(name: str) -> Column:
    return F.col(f"`{name}`")


def _lit_as(value, dtype) -> Column:
    if isinstance(value, (date, datetime)):
        value = value.isoformat()
    return F.lit(value).cast(dtype)


def _text(v) -> str:
    return str(v).lower() if isinstance(v, bool) else str(v)


def _examples(values) -> str:
    vals = [v for v in (values or []) if v is not None]
    return f" (e.g. {', '.join(str(v) for v in vals)})" if vals else ""


def _label(e: dict) -> str:
    return str(e.get("name") or e["type"])


def _columns(e: dict) -> list[str]:
    c = e.get("column")
    if c is None:
        return []
    return [str(x) for x in c] if isinstance(c, (list, tuple)) else [str(c)]


def evaluate(table: str, expectations: list[dict], df: DataFrame | None, key: str | None,
             parent_df=lambda table: None, parent_key=lambda table: None) -> list[dict]:
    """Run every expectation of one table on df (silver state; None = no rows yet)."""
    results: list[dict] = []
    if not expectations:
        return results
    spark_df = df
    total = spark_df.count() if spark_df is not None else 0
    cols = set(spark_df.columns) if spark_df is not None else set()
    ex_of = (lambda: _c(key).cast("string")) if key and key in cols else (lambda: F.lit(None).cast("string"))

    pending = []   # (index, expectation, bad condition, example column) evaluated in one aggregate
    out: dict[int, tuple[int, str]] = {}
    for i, e in enumerate(expectations):
        typ, cnames = e["type"], _columns(e)
        missing = [c for c in cnames if c not in cols]
        if typ == "row_count_min":
            need = int(e["value"])
            out[i] = (max(need - total, 0), f"table has {total} rows (minimum {need})")
            continue
        if spark_df is None:
            out[i] = (0, "silver table has no rows yet - nothing to check")
            continue
        if missing:
            out[i] = (total, f"column(s) {missing} not found in the silver table (check pipeline.yaml)")
            continue
        if typ == "not_null":
            pending.append((i, e, _c(cnames[0]).isNull(), ex_of()))
        elif typ == "range":
            c = _c(cnames[0])
            dtype = spark_df.schema[cnames[0]].dataType
            bad = F.lit(False)
            if e.get("min") is not None:
                bad = bad | (c < _lit_as(e["min"], dtype))
            if e.get("max") is not None:
                bad = bad | (c > _lit_as(e["max"], dtype))
            pending.append((i, e, c.isNotNull() & bad, c.cast("string")))
        elif typ == "accepted_values":
            c = _c(cnames[0]).cast("string")
            allowed = [_text(v) for v in e["values"]]
            pending.append((i, e, c.isNotNull() & ~c.isin(allowed), c))
        elif typ == "condition":
            try:
                cond = F.expr(str(e["condition"]))
                spark_df.select(cond).schema   # analyse now: a bad expression is reported, not crashed on
            except Exception as exc:  # noqa: BLE001 - any analysis error becomes a failed expectation
                out[i] = (total, f"condition could not be evaluated: {str(exc).splitlines()[0][:300]}")
                continue
            pending.append((i, e, ~F.coalesce(cond, F.lit(True)), ex_of()))
        elif typ == "unique":
            keys = [_c(c) for c in cnames]
            notnull = F.lit(True)
            for k in keys:
                notnull = notnull & k.isNotNull()
            dup = spark_df.filter(notnull).groupBy(*keys).count().filter("count > 1")
            row = dup.agg(F.sum("count").alias("n"), F.slice(F.array_sort(F.collect_list(
                F.concat_ws("|", *[k.cast("string") for k in keys]))), 1, 5).alias("ex")).collect()[0]
            n = int(row["n"] or 0)
            name = "+".join(cnames)
            out[i] = (n, f"{n} rows share their {name} with another row{_examples(row['ex'])}" if n
                      else f"{name} is unique across {total} rows")
        elif typ == "exists_in":
            ptable = str(e["table"])
            pcol = str(e.get("parent_column") or parent_key(ptable) or cnames[0])
            pdf = parent_df(ptable)
            c = _c(cnames[0])
            if pdf is None or pcol not in pdf.columns:
                known = spark_df.sparkSession.createDataFrame([], "__pk string")
            else:
                known = pdf.select(F.col(f"`{pcol}`").cast("string").alias("__pk")).dropna().distinct()
            orphans = spark_df.filter(c.isNotNull()).join(known, c.cast("string") == F.col("__pk"), "left_anti")
            row = orphans.agg(F.count(F.lit(1)).alias("n"),
                              F.slice(F.array_sort(F.collect_set(c.cast("string"))), 1, 5).alias("ex")).collect()[0]
            n = int(row["n"] or 0)
            out[i] = (n, f"{n} rows have {cnames[0]} values not found in {ptable}.{pcol}{_examples(row['ex'])}"
                      if n else f"every {cnames[0]} exists in {ptable}.{pcol}")

    if pending:
        aggs = []
        for i, _, bad, ex in pending:
            bad = F.coalesce(bad, F.lit(False))
            aggs += [F.sum(F.when(bad, 1).otherwise(0)).alias(f"n{i}"),
                     F.slice(F.array_sort(F.collect_set(F.when(bad, ex))), 1, 5).alias(f"e{i}")]
        row = spark_df.agg(*aggs).collect()[0].asDict()
        for i, e, _, _ in pending:
            n, ex = int(row[f"n{i}"] or 0), _examples(row[f"e{i}"])
            typ, c = e["type"], (_columns(e) or [""])[0]
            if typ == "not_null":
                text = f"{n} of {total} rows have an empty {c}{ex}" if n else f"all {total} rows have {c} filled in"
            elif typ == "range":
                lo, hi = e.get("min"), e.get("max")
                rng = (f"between {lo} and {hi}" if lo is not None and hi is not None
                       else f"at least {lo}" if lo is not None else f"at most {hi}")
                text = f"{n} rows have {c} not {rng}{ex}" if n else f"every {c} is {rng}"
            elif typ == "accepted_values":
                vals = ", ".join(_text(v) for v in e["values"])
                text = f"{n} rows have {c} not in [{vals}]{ex}" if n else f"every {c} is one of [{vals}]"
            else:  # condition
                text = (f"{n} rows break the rule '{e['condition']}'{ex}" if n
                        else f"all rows satisfy '{e['condition']}'")
            out[i] = (n, text)

    for i, e in enumerate(expectations):
        n, detail = out[i]
        severity = e.get("severity", "error")
        status = "PASS" if n == 0 and not detail.startswith(("column(s)", "condition could not")) else \
            ("FAIL" if severity == "error" else "WARN")
        results.append({"table_name": table, "expectation": _label(e), "column_name": "+".join(_columns(e)),
                        "severity": severity, "status": status, "failing_rows": n, "detail": detail})
    return results
