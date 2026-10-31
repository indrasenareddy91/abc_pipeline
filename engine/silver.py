"""SILVER: clean, validate and merge ONE batch into the current silver state (incremental, order-safe, idempotent).

Per table (config order = parents first):
  1. bronze rows of THIS batch only; unreadable records -> quarantine (<t>.broken_record)
  2. normalise every column (trim, collapse spaces, invisible chars; placeholders -> NULL)
  3. exact duplicates inside the batch -> keep the first, count the rest
  4. typed parsing per column config (rules_lib); invalid -> quarantine or NULL + flag; categories via mappings
  4b. config `lookups` (columns from a parent's current silver), `derived` columns (Spark SQL expr + type, in order),
      `row_rules` (when = bad row -> quarantine as <t>.<id> or flag <id>)
  5. same key twice in the batch: quarantine all copies (same_batch_conflict: quarantine) or per-field keep-last
  6. case hook project/rules/<table>.py: apply(df, ctx)
  7. references to parent tables (flag unknown_<parent> or quarantine)
  8. merge into the state before this batch: new key -> insert; changed -> update (per-field or replace);
     identical -> unchanged

State handling (why reruns are safe): silver_changes/<t>/batch_id=<b>/ holds the full rows each batch inserted or
updated. The state BEFORE batch b = newest row per key over the change partitions of batches < b, so re-running a
batch always merges against the same base (identical outputs). The silver snapshot = newest row per key over ALL
change partitions, so re-running an OLDER batch can never overwrite values a NEWER batch set.

Outputs (under DATA_ROOT): silver/<t>/, silver_changes/<t>/batch_id=<b>/, quarantine/<t>/batch_id=<b>/,
ops/rules_log/batch_id=<b>/, ops/row_counts/batch_id=<b>/, ops/step_io/batch_id=<b>/step_io_silver.parquet (data
journey), ops/expectations/batch_id=<b>/ (config `expectations`, checked on the silver state as of this batch - so a
re-run of any batch gives the same results). Nothing is written if the circuit breaker trips; a FAILED error-severity
expectation raises only AFTER everything is written (prints ERROR EXPECTATION_FAILED ...)."""
from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config, io, lineage
from . import expectations as EXP
from . import rules_lib as R

RAW = "__r__"            # raw (as received) copy of each business column, internal
STD_TAIL = ["quality_flags", "_batch_id", "_source_file", "_source_row_number"]
KEEP_RAW_TYPES = {"category", "date", "datetime", "money", "bool"}
SPARK_TYPES = {"id": "string", "text": "string", "email": "string", "phone": "string", "category": "string",
               "date": "date", "datetime": "timestamp_ntz", "money": "decimal(14,2)", "int": "bigint",
               "number": "decimal(18,4)", "bool": "boolean"}
DERIVED_TYPES = {**SPARK_TYPES, "int": "bigint", "integer": "bigint", "long": "bigint", "bigint": "bigint",
                 "bool": "boolean", "boolean": "boolean", "string": "string", "str": "string", "double": "double",
                 "float": "double", "decimal": "decimal(18,4)", "timestamp": "timestamp_ntz"}
Q_COLS = ["batch_id", "table_name", "record_key", "business_key", "rule_id", "reason", "raw_row", "created_at"]
QUARANTINE_ACTION = "Moved the row to quarantine for review (the value is never guessed)"
ACRONYMS = {"id": "ID", "kyc": "KYC", "dob": "date of birth", "txn": "transaction", "inr": "INR"}

ISSUES = {  # plain-English issue per parser problem; {label} = column label, {ex} = examples
    "invalid_id": "{label} is not a valid ID ({ex})",
    "invalid_format": "{label} does not match the expected format ({ex})",
    "invalid_email": "{label} is not a valid email address ({ex})",
    "invalid_phone": "{label} is not a valid phone number - too short or one repeated digit ({ex})",
    "unmapped_bool": "{label} is not a yes/no value ({ex})",
    "unparseable_number": "{label} is not a number ({ex})",
    "unparseable_amount": "{label} could not be read as an amount ({ex})",
    "unparseable_date": "{label} is not a valid date ({ex})",
    "unparseable_datetime": "{label} is not a valid date and time ({ex})",
    "unmapped_value": "{label} has a value we do not recognise ({ex})",
    "out_of_range": "{label} is outside the allowed range {rng} ({ex})",
    "date_out_of_range": "{label} is outside the allowed range {rng} ({ex})",
    "datetime_out_of_range": "{label} is outside the allowed range {rng} ({ex})",
    "missing_required": "{label} is empty but it is required",
}


def col(name: str) -> Column:
    return F.col(f"`{name}`")


def add_flags(pairs: list[tuple[Column, str]], current: Column = None) -> Column:
    """quality_flags = existing flags + every flag whose condition is true (comma string, '' if none)."""
    current = F.col("quality_flags") if current is None else current
    return F.concat_ws(",", F.when(current != "", current),
                       *[F.when(F.coalesce(c, F.lit(False)), F.lit(f)) for c, f in pairs])


def has_flag(flag: str) -> Column:
    return F.array_contains(F.split(F.col("quality_flags"), ","), flag)


def _fmt(template: str, ex: list | None = None, tg: list | None = None, **kw) -> str:
    ex_s = ", ".join(str(e) for e in ex or [])
    text = template.replace("({ex})", f"(e.g. {ex_s})" if ex_s else "").replace("{ex}", ex_s)
    text = text.replace("{tg}", " / ".join(str(t) for t in tg or []))
    for k, v in kw.items():
        text = text.replace("{" + k + "}", str(v))
    return " ".join(text.split())


def _humanise(name: str) -> str:
    return " ".join(ACRONYMS.get(w, w) for w in name.split("_"))


def _singular(table: str) -> str:
    if table.endswith(("ches", "shes", "xes", "sses")):
        return table[:-2]
    if table.endswith("ies"):
        return table[:-3] + "y"
    return table[:-1] if table.endswith("s") else table


class Ctx:
    """Passed to project/rules/<table>.py hooks:  apply(df, ctx) -> df.

    ctx.reject(df, cond, rule_id, reason, column='', issue=None, action=None) -> df without those rows (quarantined)
    ctx.flag(df, cond, flag, issue=None, action=None, column='') -> df with flag added to quality_flags
    ctx.log(rule_id, column, issue, action, rows)  rows = int, or a Column condition counted on the hook's result
    ctx.silver(table) -> that table's silver state as of this batch (own table: state before this batch)
    ctx.settings, ctx.setting(name, default), ctx.batch_id, ctx.run_id, ctx.as_of_date, ctx.table, ctx.spark"""

    def __init__(self, job: "SilverJob", table: str, spec: dict):
        self._job, self.table, self.spec = job, table, spec
        self.spark, self.batch_id, self.run_id = job.spark, job.batch_id, job.run_id
        self.settings, self.as_of_date = config.settings(), config.as_of_date()
        self.key = spec.get("key")
        self.rejects: list[DataFrame] = []
        self.q_meta: dict[str, tuple[str, str, str]] = {}   # rule_id -> (column, issue, action)
        self.pending: list[dict] = []                        # log entries counted at the end of a stage
        self.logs: list[dict] = []

    def setting(self, name, default=None):
        return config.setting(name, default)

    def silver(self, table: str) -> DataFrame | None:
        return self._job.state(table)

    def label(self, column: str) -> str:
        cspec = (self.spec.get("columns") or {}).get(column) or {}
        table_label = self.spec.get("label") or _singular(self.table).capitalize()
        if cspec.get("label"):
            return f"{table_label} {cspec['label']}"
        prefix = _singular(self.table) + "_"
        name = column[len(prefix):] if column.startswith(prefix) else column
        return f"{table_label} {_humanise(name)}"

    # ----- logging -----
    def log(self, rule_id: str, column: str, issue: str, action: str, rows) -> None:
        if isinstance(rows, Column):
            self.pending.append({"rule_id": rule_id, "column": column, "issue": issue, "action": action, "cond": rows})
        elif rows:
            self._add_log(rule_id, column, issue, action, int(rows))

    def _add_log(self, rule_id, column, issue, action, rows):
        self.logs.append({"batch_id": self.batch_id, "table_name": self.table, "rule_id": rule_id,
                          "column_name": column, "issue": issue, "action": action, "rows_affected": int(rows)})

    def entry(self, rule_id, column, issue, action, cond=None, flag=None, example=None, target=None, **fmt):
        """Queue a log line; its row count (and examples) are computed in one aggregate per stage."""
        self.pending.append({"rule_id": rule_id, "column": column, "issue": issue, "action": action, "cond": cond,
                             "flag": flag, "example": example, "target": target, "fmt": fmt})

    def evaluate(self, df: DataFrame) -> list[int]:
        """Count every pending entry on df in ONE aggregate, write the log lines with >0 rows, clear the queue."""
        entries, self.pending = self.pending, []
        if not entries:
            return []
        aggs = []
        for i, e in enumerate(entries):
            cond = F.coalesce(e["cond"], F.lit(False)) if e.get("cond") is not None else has_flag(e["flag"])
            aggs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(f"n{i}"))
            for tag in ("example", "target"):
                if e.get(tag) is not None:
                    aggs.append(F.slice(F.array_sort(F.collect_set(F.when(cond, e[tag]))), 1, 5).alias(f"{tag[0]}{i}"))
        row = df.agg(*aggs).collect()[0].asDict()
        counts = []
        for i, e in enumerate(entries):
            n = int(row[f"n{i}"] or 0)
            counts.append(n)
            if n:
                ex, tg, kw = row.get(f"e{i}"), row.get(f"t{i}"), e.get("fmt") or {}
                self._add_log(e["rule_id"], e["column"], _fmt(e["issue"], ex, tg, **kw),
                              _fmt(e["action"], ex, tg, **kw), n)
        return counts

    # ----- flag / reject -----
    def flag(self, df: DataFrame, cond: Column, flag: str, issue: str = None, action: str = None,
             column: str = "") -> DataFrame:
        if issue:
            self.entry(f"{self.table}.{flag}", column, issue, action or f"Kept the row and flagged it '{flag}'",
                       flag=flag)
        return df.withColumn("quality_flags", add_flags([(cond, flag)]))

    def reject(self, df: DataFrame, cond: Column, rule_id: str, reason, column: str = "", issue: str = None,
               action: str = None, example: Column = None) -> DataFrame:
        issue = issue or rule_id.split(".")[-1].replace("_", " ").capitalize()
        return self._reject(df, cond, F.lit(rule_id), reason, example, {rule_id: (column, issue, action)})

    def _reject(self, df, cond, rule_col, reason, example, meta: dict) -> DataFrame:
        """Send rows where cond is true to quarantine (with their raw record) and return the rest."""
        cond = F.coalesce(cond, F.lit(False))
        reason = reason if isinstance(reason, Column) else F.lit(str(reason))
        bkey = F.coalesce(col(self.key).cast("string"), F.lit("")) if self.key in df.columns else F.lit("")
        bad = df.filter(cond).select(
            F.lit(self.batch_id).alias("batch_id"), F.lit(self.table).alias("table_name"),
            F.concat_ws("#", "_source_file", F.col("_source_row_number").cast("string")).alias("record_key"),
            bkey.alias("business_key"), rule_col.alias("rule_id"), reason.alias("reason"),
            F.col("_raw_row").alias("raw_row"), F.lit(self._job.run_ts).alias("created_at"),
            (example if example is not None else F.lit(None)).cast("string").alias("_example"),
            "_source_file", "_source_row_number")
        self.rejects.append(bad)
        for rule_id, (column, issue, action) in meta.items():
            self.q_meta.setdefault(rule_id, (column, issue, action or QUARANTINE_ACTION))
        return df.filter(~cond)

    def quarantine_df(self) -> DataFrame:
        if not self.rejects:
            return self.spark.createDataFrame(
                [], "batch_id string, table_name string, record_key string, business_key string, rule_id string, "
                    "reason string, raw_row string, created_at timestamp, _example string, _source_file string, "
                    "_source_row_number int")
        q = self.rejects[0]
        for part in self.rejects[1:]:
            q = q.unionByName(part)
        return q


class SilverJob:
    def __init__(self, spark: SparkSession, batch_id: str, run_id: str):
        self.spark, self.batch_id, self.run_id = spark, batch_id, run_id
        self.run_ts = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        self.states: dict[str, DataFrame | None] = {}     # table -> state as of this batch (after processing)
        self.results: list[dict] = []

    # ---------- state ----------
    def _fold(self, table: str, include) -> DataFrame | None:
        """Newest row per key over the silver_changes partitions whose batch passes include(batch)."""
        key = (config.tables().get(table) or {}).get("key")
        batches = [b for b in io.list_partitions(f"silver_changes/{table}", "batch_id") if include(b)]
        if not batches or not key:
            return None
        df = self.spark.read.option("mergeSchema", "true").parquet(
            *[io.path(f"silver_changes/{table}/batch_id={b}") for b in batches])
        w = Window.partitionBy(col(key)).orderBy(F.col("_batch_id").desc())
        return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn", "_change")

    def state(self, table: str) -> DataFrame | None:
        """Silver state of `table` as of this batch (in-memory once the table was processed in this run)."""
        if table not in self.states:
            self.states[table] = self._fold(table, lambda b: b <= self.batch_id)
        return self.states[table]

    # ---------- per table ----------
    def process(self, table: str, spec: dict) -> dict:
        ctx = Ctx(self, table, spec)
        key = spec.get("key")
        cols_spec = {c: (s or {}) for c, s in (spec.get("columns") or {}).items()}
        prior = self._fold(table, lambda b: b < self.batch_id)
        self.states[table] = prior  # hooks asking for their own table see the state before this batch
        bronze_path = f"bronze/{table}/_batch_id={self.batch_id}"
        res = {"table": table, "ctx": ctx, "counts": dict(raw_rows=0, exact_duplicates=0, key_duplicates=0,
                                                          quarantined=0, accepted=0, inserted=0, updated=0,
                                                          unchanged=0), "changes": None}
        if not io.exists(bronze_path):
            print(f"WARN NO_BRONZE table={table} batch={self.batch_id} (no file in this batch)")
            return res
        bronze = self.spark.read.parquet(io.path(bronze_path))
        for src, dst in (spec.get("rename") or {}).items():  # bad source column names -> clean names
            if src in bronze.columns:
                bronze = bronze.withColumnRenamed(src, dst)
        biz = [c for c in bronze.columns if not c.startswith("_")]
        ph = config.placeholders()

        # 1-2. raw record JSON for quarantine, normalised values, raw copies
        df = bronze.select(
            *[R.normalise(col(c), ph).alias(c) for c in biz],
            *[col(c).alias(RAW + c) for c in biz],
            "_source_file", "_source_row_number", "_parse_error",
            F.to_json(F.struct(*[col(c) for c in biz], "_raw_text")).alias("_raw_row"),
            F.lit("").alias("quality_flags"))
        df = ctx.reject(df, F.col("_parse_error").isNotNull(), f"{table}.broken_record",
                        F.concat(F.lit("Record could not be read from the source file: "), F.col("_parse_error")),
                        column="*", issue="Source file has records that could not be read (broken JSON/CSV)",
                        action="Kept the raw text in quarantine; every other record was read (nothing lost)",
                        example=F.substring(F.col("_raw_row"), 1, 80))
        for c in biz:
            raw = col(RAW + c)
            ctx.entry(f"{table}.{c}.whitespace", c,
                      f"{ctx.label(c)} has extra spaces or invisible characters ({{ex}})",
                      "Removed the extra spaces and invisible characters",
                      cond=col(c).isNotNull() & (raw != col(c)), example=F.concat(F.lit("'"), raw, F.lit("'")))
            ctx.entry(f"{table}.{c}.placeholder", c,
                      f"{ctx.label(c)} has placeholder text instead of a value ({{ex}})", "Treated as empty (NULL)",
                      cond=col(c).isNull() & (F.trim(raw) != ""), example=raw)

        for i, when in enumerate(spec.get("drop_when") or []):  # e.g. "order_id LIKE 'Total%'"
            df = ctx.reject(df, F.expr(when), f"{table}.junk_row", F.lit(f"Not a data row: {when}"), column="*",
                            issue=f"Source file has non-data rows (totals, footers): {when}",
                            action="Removed from the data and kept in quarantine as 'junk_row' (counted)",
                            example=F.substring(F.col("_raw_row"), 1, 80))

        # 3. exact duplicates (all business columns equal after cleanup) -> keep the first
        w = Window.partitionBy(*[col(c) for c in biz]).orderBy("_source_file", "_source_row_number")
        df = df.withColumn("__dup", F.row_number().over(w) > 1).localCheckpoint()
        ctx.entry(f"{table}.exact_duplicate", "*", "The same row was sent more than once",
                  "Kept one copy and counted the others as duplicates", cond=F.col("__dup"))
        counts = ctx.evaluate(df)
        raw_rows = bronze.count()
        exact = counts[-1]
        df = df.filter(~F.col("__dup")).drop("__dup", "_parse_error")

        # 4. typed parsing
        df = self._parse(df, ctx, table, spec, cols_spec, biz)

        # 4b. lookups -> derived columns -> row rules (config driven)
        df = self._config_rules(df, ctx, table, spec)

        # 5. same key in this batch
        key_dups = 0
        if key:
            wk = Window.partitionBy(col(key))
            if spec.get("same_batch_conflict") == "quarantine":
                n = F.count(F.lit(1)).over(wk)
                df = df.withColumn("__kn", n)
                df = ctx.reject(
                    df, F.col("__kn") > 1, f"{table}.{key}.conflict_same_batch",
                    F.concat(F.lit(f"{key} "), col(key), F.lit(" appears "), F.col("__kn").cast("string"),
                             F.lit(" times in this batch with different values - cannot tell which one is right")),
                    column=key, example=col(key),
                    issue=f"The same {ctx.label(key)} appears more than once in one batch with different values ({{ex}})",
                    action="Moved all copies to quarantine (never guess which one is right)").drop("__kn")
            else:
                df, key_dups = self._keep_last(df, ctx, key)
        df = df.drop(*[c for c in df.columns if c.startswith(RAW)]).localCheckpoint()

        # 6. case hook
        hook = self._load_hook(table)
        if hook:
            df = hook(df, ctx)
            df = df.localCheckpoint()
            ctx.evaluate(df)

        # 7. references to parents
        for ref in spec.get("references", []) or []:
            df = self._reference(df, ctx, ref)

        # safety net: never treat another currency as the base currency silently
        base = config.setting("base_currency", "INR")
        for cur in [c for c in df.columns if c.endswith("_currency") and c[:-9] in cols_spec
                    and cols_spec[c[:-9]].get("type") == "money"]:
            src = cur[:-9]
            df = ctx.reject(df, F.col(cur).isNotNull() & ~F.col(cur).isin(base, "NONE"),
                            f"{table}.{src}.currency_unverified",
                            F.concat(F.lit(f"{src} has a currency symbol that is not {base}: "), F.col(cur)),
                            column=src, example=F.col(cur),
                            issue=f"{ctx.label(src)} has a currency symbol that is not {base} ({{ex}})",
                            action="Moved the row to quarantine: no rule or FX rate can verify the amount")
            mixed = F.max(F.when(F.col(cur) != "NONE", 1).otherwise(0)).over(Window.partitionBy()) == 1
            df = ctx.flag(df, (F.col(cur) == "NONE") & mixed, "currency_assumed", column=src,
                          issue=f"{ctx.label(src)} has no currency symbol while other rows have one",
                          action=f"Assumed {base} and flagged the row 'currency_assumed'")

        # final shape + types
        new = self._finalise(df, ctx, table, spec, cols_spec).localCheckpoint()
        ctx.evaluate(new)
        accepted = new.count()

        # 8. merge into the state before this batch
        merged = self._merge(new, prior, spec).localCheckpoint()
        by_change = {r["_change"]: r["n"] for r in merged.groupBy("_change").agg(F.count(F.lit(1)).alias("n")).collect()}
        changes = merged.filter(F.col("_change") != "unchanged").localCheckpoint()
        state = changes.drop("_change")
        if prior is not None:
            keep = prior.join(changes.select(col(key)), on=key, how="left_anti")
            state = keep.unionByName(state, allowMissingColumns=True)
        self.states[table] = state

        q = ctx.quarantine_df().localCheckpoint()
        q_counts = q.groupBy("rule_id").agg(
            F.count(F.lit(1)).alias("n"),
            F.slice(F.array_sort(F.collect_set("_example")), 1, 5).alias("ex")).collect()
        q_by_rule = {r["rule_id"]: r for r in q_counts}
        for rule_id, (column, issue, action) in ctx.q_meta.items():
            if rule_id in q_by_rule:
                r = q_by_rule[rule_id]
                ctx._add_log(rule_id, column, _fmt(issue, r["ex"]), _fmt(action, r["ex"]), r["n"])
        quarantined = sum(r["n"] for r in q_counts)

        res["counts"] = dict(raw_rows=raw_rows, exact_duplicates=exact, key_duplicates=key_dups,
                             quarantined=quarantined, accepted=accepted, inserted=by_change.get("insert", 0),
                             updated=by_change.get("update", 0), unchanged=by_change.get("unchanged", 0))
        res["changes"], res["quarantine"] = changes, q
        c = res["counts"]
        if c["raw_rows"] != c["exact_duplicates"] + c["key_duplicates"] + c["quarantined"] + c["accepted"] \
                or c["accepted"] != c["inserted"] + c["updated"] + c["unchanged"]:
            raise RuntimeError(f"ROW_COUNTS_DO_NOT_RECONCILE table={table} batch={self.batch_id} {c}")
        return res

    # ---------- step 4: typed parsing ----------
    def _parse(self, df, ctx, table, spec, cols_spec, biz) -> DataFrame:
        key = spec.get("key")
        as_of = config.as_of_date()
        typed = [c for c in biz if c in cols_spec and not cols_spec[c].get("derived")]
        dates = [c for c in typed if cols_spec[c].get("type") == "date"]
        decisions = {}
        if dates:  # day/month order per column, from evidence in this batch (one aggregate)
            aggs = []
            for c in dates:
                aggs += R.evidence_exprs(col(c), c)
            ev = df.agg(*aggs).collect()[0].asDict()
            default = str(config.setting("date_default_order", "dmy")).lower()
            for c in dates:
                decisions[c] = R.decide_order(ev[f"{c}__dmy"] or 0, ev[f"{c}__mdy"] or 0,
                                              cols_spec[c].get("slash_format"), default)
                decisions[c]["numeric"] = ev[f"{c}__numeric"] or 0

        # date shapes first, then parsed values, then checks: each stage references columns, not big expressions
        df = df.select("*", *[R.date_shape(col(c)).alias(f"__sh__{c}") for c in dates])
        extra, outs, info = [], [], {}  # info[c] = dict(type, on_invalid, problems=[(cond_col, issue)], ...)
        for c in typed:
            cs, typ = cols_spec[c], cols_spec[c].get("type", "text")
            s, lab = col(c), ctx.label(c)
            required = bool(cs.get("required")) or c == key
            on_invalid = cs.get("on_invalid", "quarantine" if required else "null")
            it = {"type": typ, "on_invalid": on_invalid, "required": required, "checks": []}
            if typ == "id":
                out, bad, issue = R.parse_id(s, cs)
                if cs.get("prefix") is not None or cs.get("width"):
                    ctx.entry(f"{table}.{c}.format", c, f"{lab} written in different formats ({{ex}})",
                              f"Upper-cased and padded to the standard format {cs.get('prefix', '')} + "
                              f"{cs.get('width', 0)} digits", cond=out.isNotNull() & (out != s), example=s)
            elif typ == "email":
                out, bad, issue = R.parse_email(s, cs)
                ctx.entry(f"{table}.{c}.case", c, f"{lab} has capital letters or spaces ({{ex}})",
                          "Converted to lower case and removed spaces", cond=out.isNotNull() & (out != s), example=s)
            elif typ == "phone":
                out, bad, issue = R.parse_phone(s, cs)
                ctx.entry(f"{table}.{c}.format", c, f"{lab} written in different formats ({{ex}})",
                          f"Kept the last {cs.get('digits', 10)} digits only", cond=out.isNotNull() & (out != s),
                          example=s)
            elif typ == "bool":
                out, bad, issue = R.parse_bool(s, cs)
                ctx.entry(f"{table}.{c}.mapped", c, f"{lab} written in many ways ({{ex}})", "Mapped to true / false",
                          cond=out.isNotNull() & ~F.lower(s).isin("true", "false"), example=s)
            elif typ in ("int", "number"):
                out, bad, issue = R.parse_number(s, cs, integer=typ == "int")
            elif typ == "money":
                out, bad, issue, claimed = R.parse_money(s, cs)
                outs.append(claimed.alias(f"{c}_currency"))
                ctx.entry(f"{table}.{c}.format", c,
                          f"{lab} has a currency symbol or thousands separators ({{ex}})",
                          "Removed the symbol and separators and stored the number "
                          "(the symbol is kept aside and checked against the currency)",
                          cond=out.isNotNull() & ~s.rlike(R.PLAIN_NUMBER_RE), example=s)
            elif typ == "datetime":
                out, bad, issue = R.parse_datetime(s)
                outs.append(out.alias(f"__o__{c}"))
                out = F.col(f"__o__{c}")   # reference the materialised value (keeps generated code small)
                bad = s.isNotNull() & out.isNull()
                hi = datetime.combine(as_of + timedelta(days=1), datetime.min.time())
                lo = cs.get("min", config.setting("min_date"))
                rng = out.isNotNull() & (out >= F.to_timestamp_ntz(F.lit(hi.strftime("%Y-%m-%d %H:%M:%S"))))
                if lo:
                    rng = rng | (out < F.to_timestamp_ntz(F.lit(f"{lo} 00:00:00")))
                it["checks"].append((rng, "datetime_out_of_range", f"{lo or '-'} to {as_of}"))
            elif typ == "date":
                dec = decisions[c]
                shape = F.col(f"__sh__{c}")
                out, amb, two = R.parse_date(s, dec, as_of.year, shape)
                outs.append(out.alias(f"__o__{c}"))
                out = F.col(f"__o__{c}")
                bad, issue = s.isNotNull() & out.isNull(), "unparseable_date"
                lo = cs.get("min", config.setting("min_date"))
                hi = cs.get("max", "as_of")
                hi = as_of if hi == "as_of" else hi
                rng = out.isNotNull() & F.lit(False)
                if lo:
                    rng = rng | (out < F.lit(str(lo)).cast("date"))
                if hi:
                    rng = rng | (out > F.lit(str(hi)).cast("date"))
                it["checks"].append((rng, "date_out_of_range", f"{lo or '-'} to {hi or '-'}"))
                order = "DD/MM/YYYY" if dec["conv"] == "dmy" else "MM/DD/YYYY"
                ctx.entry(f"{table}.{c}.formats", c, f"{lab} arrives in different date formats ({{ex}})",
                          "Read each format with its own exact pattern"
                          + (f"; numeric dates read as {order} because {dec['why']}" if dec["numeric"] else ""),
                          cond=out.isNotNull() & (shape != "iso"), example=s)
                it["flags"] = [(amb & out.isNotNull(), f"{c}_ambiguous")]
                ctx.entry(f"{table}.{c}.ambiguous", c, f"{lab} could be day/month or month/day ({{ex}})",
                          f"Used the default {order} order and flagged the row '{c}_ambiguous' (ask the data owner)",
                          cond=amb & out.isNotNull(), example=s)
                ctx.entry(f"{table}.{c}.two_digit_year", c, f"{lab} has two-digit years ({{ex}})",
                          f"Years above {as_of.year % 100:02d} read as 19xx, the others as 20xx",
                          cond=two & out.isNotNull(), example=s)
            elif typ == "category":
                mname = cs.get("map", c)
                if mname not in config.mappings():
                    raise ValueError(f"pipeline.yaml: column {table}.{c} uses mapping '{mname}' which is not defined")
                mspec = config.mappings()[mname] or {}
                lk = R.category_lookup(mspec)
                out, bad, issue = R.map_category(s, lk, mspec.get("valid") or [], int(mspec.get("fuzzy", 0) or 0))
                exact_known = R.category_known(s, lk)
                case_only = out.isNotNull() & (out != s) & (F.lower(out) == F.lower(s))
                typo = out.isNotNull() & (F.lower(out) != F.lower(s)) & exact_known
                ctx.entry(f"{table}.{c}.fuzzy", c, f"{lab} has a small typo not in the mapping ({{ex}})",
                          f"Corrected to the only valid value within {mspec.get('fuzzy', 0)} letter edits: {{tg}}",
                          cond=out.isNotNull() & ~exact_known, example=s, target=out)
                ctx.entry(f"{table}.{c}.case", c, f"{lab} written in different upper/lower case ({{ex}})",
                          "Standardised to: {tg}", cond=case_only, example=s, target=out)
                ctx.entry(f"{table}.{c}.mapped", c, f"{lab} has spelling mistakes or other names ({{ex}})",
                          "Mapped to the correct value: {tg}", cond=typo, example=s, target=out)
                if "on_unmapped" in cs:
                    it["on_invalid"] = cs["on_unmapped"]
                elif "on_invalid" not in cs:
                    it["on_invalid"] = "quarantine"
            else:
                out, bad, issue = R.parse_text(s, cs)
                if cs.get("case"):
                    ctx.entry(f"{table}.{c}.case", c, f"{lab} written in inconsistent upper/lower case ({{ex}})",
                              f"Converted to {cs['case']} case", cond=out.isNotNull() & (out != s), example=s)
            if typ in ("int", "number", "money") and ("min" in cs or "max" in cs):
                rng = out.isNotNull() & F.lit(False)
                if "min" in cs:
                    rng = rng | (out < F.lit(cs["min"]))
                if "max" in cs:
                    rng = rng | (out > F.lit(cs["max"]))
                it["checks"].append((rng, "out_of_range", f"{cs.get('min', '-')} to {cs.get('max', '-')}"))
            it["checks"].insert(0, (bad, issue, ""))
            info[c] = it
            if typ not in ("date", "datetime"):
                outs.append(out.alias(f"__o__{c}"))
            for i, (cond, _, _) in enumerate(it["checks"]):
                extra.append(F.coalesce(cond, F.lit(False)).alias(f"__b{i}__{c}"))

        df1 = df.select("*", *outs).localCheckpoint()
        df1 = df1.select("*", *extra).localCheckpoint()

        # log lines for NULL+flag and quarantine decisions, then the final column values
        flags, problems = [], []   # problems: (cond, rule_id, reason, example)
        for c, it in info.items():
            lab, any_bad = ctx.label(c), F.lit(False)
            for i, (_, issue, rng) in enumerate(it["checks"]):
                cond = F.col(f"__b{i}__{c}")
                any_bad = any_bad | cond
                text = ISSUES[issue].replace("{rng}", f"({rng})")
                if it["on_invalid"] == "quarantine":
                    rule = f"{table}.{c}.{issue}"
                    problems.append((cond, rule, F.concat(F.lit(f"{c} {issue.replace('_', ' ')}: '"),
                                                          F.coalesce(col(RAW + c), F.lit("")), F.lit("'")), col(RAW + c)))
                    ctx.q_meta.setdefault(rule, (c, text.replace("{label}", lab), QUARANTINE_ACTION))
                else:
                    flags.append((cond, f"{c}_{issue}"))
                    ctx.entry(f"{table}.{c}.{issue}", c, text.replace("{label}", lab),
                              f"Set the value to empty (NULL), kept the row and flagged it '{c}_{issue}'",
                              cond=cond, example=col(RAW + c))
            it["any_bad"] = any_bad
            flags += it.get("flags", [])
            if it["required"]:
                rule = f"{table}.{c}.missing_required"
                missing = F.col(f"__o__{c}").isNull() & ~any_bad
                problems.append((missing, rule, F.lit(f"{c} is required but empty"), col(RAW + c)))
                ctx.q_meta.setdefault(rule, (c, ISSUES["missing_required"].replace("{label}", lab),
                                             QUARANTINE_ACTION))
        ctx.evaluate(df1)

        sel = []
        for c in df.columns:
            if c in info:
                it = info[c]
                value = F.col(f"__o__{c}")
                if it["on_invalid"] != "quarantine":
                    value = F.when(~it["any_bad"], value)
                sel.append(value.alias(c))
                keep_raw = cols_spec[c].get("keep_raw", it["type"] in KEEP_RAW_TYPES)
                if keep_raw:
                    sel.append(col(RAW + c).alias(f"{c}_raw"))
            elif c != "quality_flags" and not c.startswith("__sh__"):
                sel.append(col(c))
        sel += [F.col(f"{c}_currency") for c in info if info[c]["type"] == "money"]
        sel.append(add_flags(flags).alias("quality_flags"))
        rule_col = F.coalesce(*[F.when(p[0], F.lit(p[1])) for p in problems], F.lit(None).cast("string")) \
            if problems else F.lit(None).cast("string")
        reason_col = F.concat_ws("; ", *[F.when(p[0], p[2]) for p in problems]) if problems else F.lit("")
        example_col = F.coalesce(*[F.when(p[0], p[3]) for p in problems], F.lit(None).cast("string")) \
            if problems else F.lit(None).cast("string")
        df2 = df1.select(*sel, rule_col.alias("__q_rule"), reason_col.alias("__q_reason"),
                         example_col.alias("__q_example"))
        df2 = ctx._reject(df2, F.col("__q_rule").isNotNull(), F.col("__q_rule"), F.col("__q_reason"),
                          F.col("__q_example"), {})
        return df2.drop("__q_rule", "__q_reason", "__q_example").localCheckpoint()

    # ---------- step 4b: lookups -> derived -> row_rules (pipeline.yaml, SPEC v2 A) ----------
    def _config_rules(self, df, ctx, table, spec) -> DataFrame:
        fills = {c: cs["default"] for c, cs in (spec.get("columns") or {}).items() if (cs or {}).get("default") is not None}
        for c, v in fills.items():  # optional column empty after cleaning -> default label (row still groups in KPIs)
            ctx.entry(f"{table}.{c}.default", c, f"{ctx.label(c)} is empty", f"Filled with '{v}'",
                      cond=F.col(f"__fill_{c}"))
            df = df.withColumn(f"__fill_{c}", col(c).isNull()) \
                   .withColumn(c, F.coalesce(col(c), F.lit(v).cast(df.schema[c].dataType)))
        if fills:
            df = df.localCheckpoint()
            ctx.evaluate(df)
            df = df.drop(*[f"__fill_{c}" for c in fills])
        cols = spec.get("columns") or {}
        for c, cs in cols.items():
            how = (cs or {}).get("fill")
            if how not in ("mean", "median", "mode") or c not in df.columns:
                continue
            if how == "mode":
                top = df.where(col(c).isNotNull()).groupBy(c).count().orderBy(F.desc("count"), col(c)).first()
                v = top[0] if top else None
            elif how == "median":
                q = df.where(col(c).isNotNull()).approxQuantile(c, [0.5], 0.0)
                v = q[0] if q else None
            else:
                v = df.agg(F.avg(col(c))).first()[0]
            if v is None:
                continue
            df = df.withColumn(f"__fill_{c}", col(c).isNull())
            ctx.entry(f"{table}.{c}.fill_{how}", c, f"{ctx.label(c)} is empty",
                      f"Filled with the column {how} ({v})", cond=F.col(f"__fill_{c}"))
            df = df.withColumn(c, F.coalesce(col(c), F.lit(v).cast(df.schema[c].dataType))) \
                   .withColumn("quality_flags", add_flags([(F.col(f"__fill_{c}"), f"{c}_filled_{how}")]))
            df = df.localCheckpoint(); ctx.evaluate(df); df = df.drop(f"__fill_{c}")
        for c, cs in cols.items():
            if (cs or {}).get("outlier") != "iqr" or c not in df.columns:
                continue
            q = df.where(col(c).isNotNull()).approxQuantile(c, [0.25, 0.75], 0.0)
            if len(q) < 2:
                continue
            lo, hi = q[0] - 1.5 * (q[1] - q[0]), q[1] + 1.5 * (q[1] - q[0])
            out = col(c).isNotNull() & ((col(c) < lo) | (col(c) > hi))
            df = ctx.flag(df, out, f"{c}_outlier", column=c,
                          issue=f"{ctx.label(c)} is far outside the normal range (IQR rule: below {lo:,.2f} or above {hi:,.2f})",
                          action="Kept the row and flagged it as an outlier (review; not deleted)")
        lookups, derived, row_rules = spec.get("lookups") or [], spec.get("derived") or {}, spec.get("row_rules") or []
        if not (lookups or derived or row_rules):
            return df
        for lk in lookups:
            df = self._lookup(df, ctx, table, lk)
        for name, d in derived.items():
            expr, typ = str(d["expr"]), str(d.get("type", "string"))
            try:
                df = df.withColumn(name, F.expr(expr).cast(DERIVED_TYPES.get(typ.lower(), typ)))
            except Exception as exc:  # noqa: BLE001 - analysis errors -> clear config message
                raise ValueError(f"pipeline.yaml tables.{table}.derived.{name}: cannot evaluate '{expr}' as {typ}: "
                                 f"{str(exc).splitlines()[0]}") from exc
            ctx.entry(f"{table}.{name}.derived", name,
                      str(d.get("reason") or f"{ctx.label(name)} is not in the source data"),
                      f"calculated {name} = {expr}", cond=col(name).isNotNull())
        if derived:
            df = df.localCheckpoint()
            ctx.evaluate(df)
        for r in row_rules:
            rid, when, action = str(r["id"]), str(r["when"]), r.get("action", "quarantine")
            reason = str(r.get("reason") or rid.replace("_", " ").capitalize())
            try:
                cond = F.expr(when)
                df.select(cond).schema
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"pipeline.yaml tables.{table}.row_rules.{rid}: cannot evaluate '{when}': "
                                 f"{str(exc).splitlines()[0]}") from exc
            column = str(r.get("column", ""))
            if action == "flag":
                df = ctx.flag(df, cond, rid, issue=reason, action=f"flagged '{rid}'", column=column)
            else:
                df = ctx.reject(df, cond, f"{table}.{rid}", reason, column=column, issue=reason,
                                action="quarantined", example=col(ctx.key).cast("string") if ctx.key else None)
        df = df.localCheckpoint()
        ctx.evaluate(df)
        return df

    def _lookup(self, df, ctx, table, lk) -> DataFrame:
        """Bring columns from the parent's silver state (as of this batch) by join keys. Several parent rows with the
        same join key -> the one with the lowest parent key is used (deterministic). No match -> NULL (logged)."""
        parent, on, cols = lk["table"], config.lookup_on(lk), config.lookup_columns(lk)
        clash = [a for _, a in cols if a in df.columns]
        if clash:
            raise ValueError(f"pipeline.yaml tables.{table}.lookups ({parent}): column(s) {clash} already exist")
        pstate = self.state(parent)
        pspec = config.tables().get(parent) or {}
        if pstate is None:  # parent has no rows yet: typed NULL columns (same schema as later batches)
            pcols = pspec.get("columns") or {}
            for src, alias in cols:
                typ = SPARK_TYPES.get(((pcols.get(src) or {}).get("type")) or "text", "string")
                df = df.withColumn(alias, F.lit(None).cast(typ))
            df = df.withColumn("__lk_hit", F.lit(None).cast("boolean"))
        else:
            missing = [c for c in list(on.values()) + [s for s, _ in cols] if c not in pstate.columns]
            if missing:
                raise ValueError(f"pipeline.yaml tables.{table}.lookups: {parent} has no column(s) {missing}")
            jk = [f"__lk{i}" for i in range(len(on))]
            pkey = pspec.get("key")
            p = pstate.select(*[col(pc).alias(j) for j, pc in zip(jk, on.values())],
                              *[col(s).alias(a) for s, a in cols],
                              (col(pkey) if pkey in pstate.columns else F.lit(None)).cast("string").alias("__lk_pk"))
            for j in jk:
                p = p.filter(F.col(j).isNotNull())
            w = Window.partitionBy(*jk).orderBy(F.col("__lk_pk").asc_nulls_last())
            p = p.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1") \
                .drop("__rn", "__lk_pk").withColumn("__lk_hit", F.lit(True))
            cond = None
            for j, child in zip(jk, on.keys()):
                eq = col(child) == F.col(j)
                cond = eq if cond is None else cond & eq
            df = df.join(p, cond, "left").drop(*jk)
        keys_present = F.lit(True)
        for child in on:
            keys_present = keys_present & col(child).isNotNull()
        names = ", ".join(a for _, a in cols)
        by = ", ".join(on.keys())
        ctx.entry(f"{table}.lookup.{parent}", names, f"{names} not in the source data - it comes from {parent}",
                  f"Looked up {names} from {parent} by {by}", cond=F.col("__lk_hit").isNotNull())
        ctx.entry(f"{table}.lookup.{parent}.not_found", by,
                  f"{ctx.label(list(on)[0])} has no matching {_singular(parent)} ({{ex}})",
                  f"Left {names} empty (NULL)", cond=keys_present & F.col("__lk_hit").isNull(),
                  example=F.concat_ws("|", *[col(c).cast("string") for c in on]))
        df = df.localCheckpoint()
        ctx.evaluate(df)   # counts need __lk_hit, so log now
        return df.drop("__lk_hit")

    # ---------- step 5 (no conflict rule): per-field keep-last inside the batch ----------
    def _keep_last(self, df, ctx, key) -> tuple[DataFrame, int]:
        ob = (config.tables()[ctx.table] or {}).get("order_by")  # e.g. updated_at: the newest version wins
        order = ([F.coalesce(F.col(ob).cast("string"), F.lit(""))] if ob else []) \
            + [F.col("_source_file"), F.col("_source_row_number")]  # ISO timestamp text sorts in time order
        w_all = Window.partitionBy(col(key)).orderBy(*order).rowsBetween(Window.unboundedPreceding,
                                                                          Window.unboundedFollowing)
        w_last = Window.partitionBy(col(key)).orderBy(*[o.desc() for o in order])
        df = df.withColumn("__rn", F.row_number().over(w_last)).withColumn("__n", F.count(F.lit(1)).over(w_all))
        df = df.localCheckpoint()
        ctx.entry(f"{ctx.table}.{key}.key_duplicate", key,
                  f"The same {ctx.label(key)} appears more than once in this batch with different values ({{ex}})",
                  "Merged into one row: the latest non-empty value of each column wins",
                  cond=F.col("__rn") > 1, example=col(key))
        key_dups = ctx.evaluate(df)[0]
        skip = {key, "quality_flags", "_source_file", "_source_row_number", "_raw_row", "__rn", "__n"}
        sel = []
        for c in df.columns:
            if c in skip or c.startswith(RAW):
                continue
            if c.endswith("_raw") and c[:-4] in df.columns:  # raw follows the value it belongs to
                v = F.last(F.when(col(c[:-4]).isNotNull(), col(c)), True).over(w_all)
                sel.append(F.coalesce(v, col(c)).alias(c))
            else:
                sel.append(F.coalesce(F.last(col(c), True).over(w_all), col(c)).alias(c))
        out = df.select(col(key), *sel, *[col(c) for c in df.columns if c.startswith(RAW)], "_source_file",
                        "_source_row_number", "_raw_row", "__rn",
                        add_flags([(F.col("__n") > 1, "key_conflict_resolved")]).alias("quality_flags"))
        return out.filter("__rn = 1").drop("__rn"), key_dups

    # ---------- step 6 ----------
    @staticmethod
    def _load_hook(table: str):
        path = config.project_dir() / "rules" / f"{table}.py"
        if not path.exists():
            return None
        spec = importlib.util.spec_from_file_location(f"cdt_rules_{table}", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return getattr(mod, "apply", None)

    # ---------- step 7 ----------
    def _reference(self, df, ctx, ref) -> DataFrame:
        c, parent = ref["column"], ref["table"]
        pcol = ref.get("parent_column") or (config.tables()[parent] or {}).get("key")
        on_missing = ref.get("on_missing", "quarantine")
        pstate = self.state(parent)
        if pstate is None:
            known = self.spark.createDataFrame([], "__pk string").withColumn("__known", F.lit(True))
        else:
            known = pstate.select(col(pcol).cast("string").alias("__pk")).dropna().distinct() \
                .withColumn("__known", F.lit(True))
        df = df.join(F.broadcast(known), col(c).cast("string") == F.col("__pk"), "left").drop("__pk")
        missing = col(c).isNotNull() & F.col("__known").isNull()
        lab = ctx.label(c)
        if on_missing == "flag":
            df = ctx.flag(df, missing, f"unknown_{parent}", column=c,
                          issue=f"{lab} points to a {_singular(parent)} that does not exist",
                          action=f"Kept the row (it still counts in totals), flagged it 'unknown_{parent}'; "
                                 f"reports show it under UNKNOWN")
        else:
            df = ctx.reject(df, missing, f"{ctx.table}.{c}.orphan",
                            F.concat(F.lit(f"no {parent} row for {c} '"), col(c), F.lit("'")), column=c,
                            example=col(c), issue=f"{lab} points to a {_singular(parent)} that does not exist ({{ex}})",
                            action="Moved the row to quarantine (it cannot be linked)")
        return df.drop("__known")

    # ---------- final columns and types ----------
    def _finalise(self, df, ctx, table, spec, cols_spec) -> DataFrame:
        wanted = list(spec.get("output_columns") or [])
        internal = {"_raw_row", "_source_file", "_source_row_number", "quality_flags", "_batch_id"}
        hidden = {n for n, d in (spec.get("derived") or {}).items() if (d or {}).get("keep") is False}
        if not wanted:
            wanted = [c for c in df.columns if c not in internal and not c.startswith("__") and c not in hidden]
        else:  # lookup + derived columns always reach silver (unless derived.keep: false)
            extra = [a for lk in spec.get("lookups") or [] for _, a in config.lookup_columns(lk)]
            extra += [n for n in (spec.get("derived") or {}) if n not in hidden]
            wanted += [c for c in extra if c not in wanted and c in df.columns]
        sel = []
        for c in wanted:
            if c == "first_seen_batch" and c not in df.columns:
                sel.append(F.lit(None).cast("string").alias(c))  # set by the merge
                continue
            if c not in df.columns:
                raise ValueError(f"pipeline.yaml {table}.output_columns lists '{c}' but no rule produced it "
                                 f"(available: {df.columns})")
            typ = cols_spec.get(c, {}).get("type")
            sel.append(col(c).cast(SPARK_TYPES[typ]).alias(c) if typ in SPARK_TYPES else col(c))
        return df.select(*sel, "quality_flags", F.lit(self.batch_id).alias("_batch_id"), "_source_file",
                         F.col("_source_row_number").cast("int").alias("_source_row_number"))

    # ---------- step 8 ----------
    def _merge(self, new: DataFrame, prior: DataFrame | None, spec: dict) -> DataFrame:
        key = spec["key"]
        mode = spec.get("on_update", "coalesce")    # coalesce: new non-null wins, null keeps old | replace
        out_cols = new.columns
        meta = {key, "_batch_id", "_source_file", "_source_row_number", "first_seen_batch", "quality_flags"}
        compare = [c for c in out_cols if c not in meta and not c.endswith("_raw")]
        if prior is None:
            sel = [F.lit(self.batch_id).alias(c) if c == "first_seen_batch" else col(c) for c in out_cols]
            return new.select(*sel, F.lit("insert").alias("_change"))
        p = prior.select(*[col(c).alias(f"__p__{c}") for c in out_cols if c in prior.columns])

        def old(c):
            return F.col(f"__p__{c}") if c in prior.columns else F.lit(None).cast(new.schema[c].dataType)

        j = new.join(p, col(key) == F.col(f"__p__{key}"), "left")
        merged = {}
        for c in out_cols:
            if c in meta - {"first_seen_batch"}:
                merged[c] = col(c)
            elif c == "first_seen_batch":
                merged[c] = F.coalesce(old(c), F.lit(self.batch_id))
            elif mode == "replace":
                merged[c] = col(c)
            elif c.endswith("_raw") and c[:-4] in out_cols:
                merged[c] = F.when(col(c[:-4]).isNotNull(), col(c)).otherwise(F.coalesce(old(c), col(c)))
            else:
                merged[c] = F.coalesce(col(c), old(c))
        exists = F.col(f"__p__{key}").isNotNull()
        changed = F.lit(False)
        for c in compare:
            changed = changed | ~merged[c].eqNullSafe(old(c))
        change = F.when(~exists, "insert").when(changed, "update").otherwise("unchanged")
        return j.select(*[merged[c].alias(c) for c in out_cols], change.alias("_change"))

    # ---------- run ----------
    def run(self) -> list[dict]:
        tables = config.tables()
        config.validate()   # v2 sections (lookups/derived/row_rules/expectations) checked before any work
        for table, spec in tables.items():
            spec = spec or {}
            if not spec.get("key"):
                raise ValueError(f"pipeline.yaml: table '{table}' needs a 'key' for the incremental merge")
            self.results.append(self.process(table, spec))

        alert, fail = float(config.setting("quarantine_alert_pct", 5)), float(config.setting("quarantine_fail_pct", 50))
        for r in self.results:
            c = r["counts"]
            pct = 100.0 * c["quarantined"] / c["raw_rows"] if c["raw_rows"] else 0.0
            if pct > fail:
                raise RuntimeError(f"CIRCUIT_BREAKER table={r['table']} batch={self.batch_id} quarantined "
                                   f"{c['quarantined']} of {c['raw_rows']} ({pct:.1f}% > {fail}%) - nothing written")
            if pct > alert:
                print(f"QUARANTINE_ALERT table={r['table']} batch={self.batch_id} quarantined={c['quarantined']} "
                      f"of {c['raw_rows']} ({pct:.1f}% > {alert}%)")
        self._write()
        return self.results

    def _write(self) -> None:
        b = self.batch_id
        logs, counts = [], []
        for r in self.results:
            t = r["table"]
            if r["changes"] is None:   # no file for this table in this batch: clear stale outputs of a rerun
                for prefix in (f"silver_changes/{t}/batch_id={b}", f"quarantine/{t}/batch_id={b}"):
                    if io.exists(prefix):
                        io.delete(prefix)
            else:
                r["changes"].coalesce(1).write.mode("overwrite").parquet(io.path(f"silver_changes/{t}/batch_id={b}"))
                r["quarantine"].orderBy("_source_file", "_source_row_number").select(*Q_COLS) \
                    .coalesce(1).write.mode("overwrite").parquet(io.path(f"quarantine/{t}/batch_id={b}"))
            snap = self._fold(t, lambda _: True)
            if snap is not None:
                key = config.tables()[t]["key"]
                snap.orderBy(col(key)).coalesce(1).write.mode("overwrite").parquet(io.path(f"silver/{t}"))
            logs += r["ctx"].logs
            counts.append({"batch_id": b, "table_name": t, **r["counts"]})
        self.spark.createDataFrame(
            [tuple(x.values()) for x in logs] or [],
            "batch_id string, table_name string, rule_id string, column_name string, issue string, action string, "
            "rows_affected long").coalesce(1).write.mode("overwrite").parquet(io.path(f"ops/rules_log/batch_id={b}"))
        self.spark.createDataFrame(
            [(c["batch_id"], c["table_name"], c["raw_rows"], c["exact_duplicates"], c["key_duplicates"],
              c["quarantined"], c["accepted"], c["inserted"], c["updated"], c["unchanged"]) for c in counts],
            "batch_id string, table_name string, raw_rows long, exact_duplicates long, key_duplicates long, "
            "quarantined long, accepted long, inserted long, updated long, unchanged long") \
            .coalesce(1).write.mode("overwrite").parquet(io.path(f"ops/row_counts/batch_id={b}"))
        lineage.write_step_io(b, "silver", self._step_io_rows())
        self._expectations()

    # ---------- data journey ----------
    def _step_io_rows(self) -> list[dict]:
        b, rows = self.batch_id, []
        for r in self.results:
            t, c = r["table"], r["counts"]
            src = f"bronze/{t}/_batch_id={b}/"
            if r["changes"] is None:
                rows.append(lineage.step_row(b, self.run_id, "silver", t, src, 0, f"silver/{t}/", 0, 0,
                                             "no file for this table in this batch", self.run_ts))
                continue
            rows.append(lineage.step_row(
                b, self.run_id, "silver", t, src, c["raw_rows"], f"silver/{t}/", c["accepted"], c["quarantined"],
                f"accepted {c['accepted']} = inserted {c['inserted']} + updated {c['updated']} + unchanged "
                f"{c['unchanged']}; {c['exact_duplicates']} exact duplicate(s) and {c['key_duplicates']} "
                f"same-key duplicate(s) merged; rejected rows -> quarantine/{t}/batch_id={b}/", self.run_ts))
            rows.append(lineage.step_row(
                b, self.run_id, "silver", t, src, c["raw_rows"], f"silver_changes/{t}/batch_id={b}/",
                c["inserted"] + c["updated"], 0,
                "rows this batch inserted or changed (what gold loads)", self.run_ts))
        return rows

    # ---------- expectations (on the silver state as of this batch) ----------
    def _expectations(self) -> None:
        b, tables = self.batch_id, config.tables()
        snaps: dict[str, DataFrame | None] = {}

        def snap(t):
            if t not in snaps:
                snaps[t] = self._fold(t, lambda x: x <= b)
            return snaps[t]

        rows = []
        for t, spec in tables.items():
            spec = spec or {}
            exps = spec.get("expectations") or []
            if not exps:
                continue
            for res in EXP.evaluate(t, exps, snap(t), spec.get("key"), parent_df=snap,
                                    parent_key=lambda p: (tables.get(p) or {}).get("key")):
                rows.append({"batch_id": b, "run_id": self.run_id, **res, "checked_at": self.run_ts})
        lineage.write_expectations(b, rows)
        failed = []
        for r in rows:
            line = (f"table={r['table_name']} expectation={r['expectation']} column={r['column_name'] or '-'} "
                    f"failing_rows={r['failing_rows']} detail={r['detail']}")
            if r["status"] == "FAIL":
                print(f"ERROR EXPECTATION_FAILED {line}")
                failed.append(f"{r['table_name']}.{r['expectation']}")
            elif r["status"] == "WARN":
                print(f"WARN EXPECTATION_WARN {line}")
        n_pass = sum(1 for r in rows if r["status"] == "PASS")
        print(f"EXPECTATIONS batch={b} pass={n_pass} warn={sum(1 for r in rows if r['status'] == 'WARN')} "
              f"fail={len(failed)}")
        if failed:
            raise RuntimeError(f"EXPECTATION_FAILED batch={b}: {', '.join(failed)} (outputs were written; "
                               f"see ops/expectations/batch_id={b}/)")


def run_silver(spark: SparkSession, batch_id: str, run_id: str) -> list[dict]:
    """Clean + merge one batch. Returns [{table, counts}] and prints STEP_* lines."""
    print(f"STEP_START step=silver batch={batch_id} run_id={run_id}")
    results = SilverJob(spark, batch_id, run_id).run()
    summary = "; ".join(
        f"{r['table']}: raw={r['counts']['raw_rows']} accepted={r['counts']['accepted']} "
        f"quarantined={r['counts']['quarantined']} inserted={r['counts']['inserted']} "
        f"updated={r['counts']['updated']}" for r in results)
    print(f"STEP_OK step=silver batch={batch_id} run_id={run_id} {summary}")
    return [{"table": r["table"], **r["counts"]} for r in results]
