"""Spark column parsers used by silver. Every function takes/returns Spark Columns (whole-column operations,
no Python row loops). A parser returns (clean_value, invalid_condition, issue_name).

Value -> type rules are driven by the column spec in pipeline.yaml (type, prefix, width, case, min, max, ...)."""
from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F

# ---------- reference data (rules are data: extend via config, not code) ----------
BOOL_MAP = {"y": True, "yes": True, "true": True, "t": True, "1": True,
            "n": False, "no": False, "false": False, "f": False, "0": False}
CURRENCY_MAP = {"₹": "INR", "inr": "INR", "rs": "INR", "rs.": "INR", "$": "USD", "usd": "USD", "us$": "USD"}
MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}

# trusted money pattern: optional symbol, optional minus, digits (with 1,234 or 1,23,456 grouping), optional decimals,
# optional trailing code. Groups: 1 = leading symbol, 2 = minus, 3 = number, 4 = trailing code.
MONEY_RE = (r"^(₹|rs\.?|inr|us\$|\$|usd)?\s*(-)?\s*(\d{1,3}(?:,\d{2,3})+(?:\.\d+)?|\d+(?:\.\d+)?)\s*(inr|usd|rs\.?)?$")
PLAIN_NUMBER_RE = r"^-?\d+(\.\d+)?$"

# date shapes (checked on the lower-cased value, in this order). Groups give the parts named in DATE_PARTS.
DATE_SHAPES = {
    "iso": r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:[t ]\d{1,2}:\d{2}(?::\d{2})?.*)?$",
    "ymd_slash": r"^(\d{4})/(\d{1,2})/(\d{1,2})$",
    "numeric": r"^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2}|\d{4})$",
    "d_mon_y": r"^(\d{1,2})[- ]([a-z]{3,9})\.?[- ,]+(\d{2}|\d{4})$",
    "mon_d_y": r"^([a-z]{3,9})\.? (\d{1,2})(?:st|nd|rd|th)?,? (\d{4})$",
}
DATE_PARTS = {  # shape -> (year group, month group, day group); numeric uses a=1, b=2, y=3
    "iso": (1, 2, 3), "ymd_slash": (1, 2, 3), "d_mon_y": (3, 2, 1), "mon_d_y": (3, 1, 2)}
DATETIME_RE = r"^(\d{4}-\d{2}-\d{2})[T ](\d{1,2}):(\d{2})(?::(\d{2}))?(?:\.(\d+))?Z?$"

INVISIBLE_RE = "[​‌‍⁠﻿]"   # zero-width characters -> removed
ODD_SPACE_RE = "[   \t\r\n]"          # non-breaking space, tabs, newlines -> normal space


def lit_map(d: dict) -> Column:
    """Small python dict -> Spark map literal (a broadcast lookup table inside the expression)."""
    items = []
    for k, v in d.items():
        items += [F.lit(k), F.lit(v)]
    return F.create_map(*items)


# ---------- generic cleanup ----------
def normalise(c: Column, placeholders: list[str]) -> Column:
    """Trim, collapse spaces, strip invisible characters; placeholder text (N/A, null, -, ...) -> NULL."""
    s = F.regexp_replace(c, INVISIBLE_RE, "")
    s = F.regexp_replace(s, ODD_SPACE_RE, " ")
    s = F.trim(F.regexp_replace(s, r" {2,}", " "))
    return F.when(s.isNull() | F.lower(s).isin(placeholders), F.lit(None).cast("string")).otherwise(s)


# ---------- typed parsers: (clean, invalid_condition, issue) ----------
def parse_id(s: Column, spec: dict):
    """IDs: upper-case, no spaces; with prefix/width: 'c7', 'C-07', '7' -> 'C007' (never truncates longer numbers)."""
    up = F.upper(F.regexp_replace(s, r"\s+", ""))
    if spec.get("prefix") is not None or spec.get("width"):
        prefix = str(spec.get("prefix", "") or "")
        width = int(spec.get("width", 0) or 0)
        rx = r"^([A-Z]*)[-_]?(\d+)$"
        p, n = F.regexp_extract(up, rx, 1), F.regexp_extract(up, rx, 2)
        ok = up.rlike(rx) & p.isin(*([prefix, ""] if prefix else [""]))
        n = F.regexp_replace(n, r"^0+(?=\d)", "")
        num = F.when(F.length(n) >= width, n).otherwise(F.lpad(n, width, "0")) if width else n
        out = F.when(s.isNotNull() & ok, F.concat(F.lit(prefix), num))
        return out, s.isNotNull() & ~ok, "invalid_id"
    pat = spec.get("pattern")
    bad = (s.isNotNull() & ~up.rlike(pat)) if pat else F.lit(False)
    return F.when(~bad, up), bad, "invalid_id"


def parse_text(s: Column, spec: dict):
    case = spec.get("case")
    out = {"upper": F.upper(s), "lower": F.lower(s), "title": F.initcap(s)}.get(case, s)
    pat = spec.get("pattern")
    bad = (out.isNotNull() & ~out.rlike(pat)) if pat else F.lit(False)
    return F.when(~bad, out), bad, "invalid_format"


def parse_email(s: Column, spec: dict):
    out = F.lower(F.regexp_replace(s, " ", ""))
    bad = out.isNotNull() & ~out.rlike(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$")
    return F.when(~bad, out), bad, "invalid_email"


def parse_phone(s: Column, spec: dict):
    """Digits only, keep the last N (default 10). Too short or one repeated digit (0000000000) -> invalid."""
    n = int(spec.get("digits", 10))
    digits = F.regexp_replace(s, r"\D", "")
    last = F.substring(digits, -n, n)
    bad = s.isNotNull() & ((F.length(digits) < n) | last.rlike(r"^(\d)\1+$"))
    return F.when(s.isNotNull() & ~bad, last), bad, "invalid_phone"


def parse_bool(s: Column, spec: dict):
    m = {**BOOL_MAP, **{str(k).lower(): bool(v) for k, v in (spec.get("values") or {}).items()}}
    out = lit_map(m)[F.lower(s)]
    return out, s.isNotNull() & out.isNull(), "unmapped_bool"


def parse_number(s: Column, spec: dict, integer: bool = False):
    t = F.regexp_replace(s, ",", "")
    pat = r"^[+-]?\d+(\.0+)?$" if integer else r"^[+-]?(\d+\.?\d*|\.\d+)$"
    ok = t.rlike(pat)
    out = F.when(ok, t.cast("decimal(30,4)").cast("bigint" if integer else "decimal(18,4)"))
    return out, s.isNotNull() & ~ok, "unparseable_number"


def parse_money(s: Column, spec: dict):
    """Whitelist pattern, then cast to DECIMAL(14,2). Also returns the CLAIMED currency of the symbol:
    INR / USD / NONE (no symbol) / CONFLICT (two different symbols)."""
    low = F.lower(s)
    ok = low.rlike(MONEY_RE)
    c1, neg, num, c2 = (F.regexp_extract(low, MONEY_RE, i) for i in (1, 2, 3, 4))
    value = F.regexp_replace(num, ",", "").cast("decimal(16,4)")
    value = F.when(neg == "-", -value).otherwise(value).cast("decimal(14,2)")
    cmap = lit_map(CURRENCY_MAP)
    k1, k2 = cmap[c1], cmap[c2]
    claimed = (F.when((c1 != "") & (c2 != "") & (k1 != k2), "CONFLICT")
               .when(c1 != "", k1).when(c2 != "", k2).otherwise("NONE"))
    return F.when(ok, value), s.isNotNull() & ~ok, "unparseable_amount", F.when(ok, claimed)


# ---------- dates ----------
def date_shape(s: Column) -> Column:
    """Name of the first matching date shape, or 'unrecognised' (NULL input -> NULL)."""
    low = F.lower(s)
    expr = F.when(s.isNull(), F.lit(None).cast("string"))
    for name, rx in DATE_SHAPES.items():
        expr = expr.when(low.rlike(rx), name)
    return expr.otherwise("unrecognised")


def numeric_parts(s: Column):
    """(a, b) as ints for numeric a/b/yyyy dates (NULL for other shapes)."""
    low, rx = F.lower(s), DATE_SHAPES["numeric"]
    is_num = low.rlike(rx)
    a = F.when(is_num, F.regexp_extract(low, rx, 1).cast("int"))
    b = F.when(is_num, F.regexp_extract(low, rx, 2).cast("int"))
    return a, b


def evidence_exprs(s: Column, alias: str) -> list[Column]:
    """Aggregates proving day/month order in one column: '28/08' => dmy, '08/28' => mdy."""
    a, b = numeric_parts(s)
    return [F.sum(F.when((a > 12) & (b <= 12), 1).otherwise(0)).alias(f"{alias}__dmy"),
            F.sum(F.when((b > 12) & (a <= 12), 1).otherwise(0)).alias(f"{alias}__mdy"),
            F.sum(F.when(a.isNotNull(), 1).otherwise(0)).alias(f"{alias}__numeric")]


def decide_order(dmy_ev: int, mdy_ev: int, locked: str | None, default: str) -> dict:
    """Column-level decision for numeric dates. Returns conv, mixed, flag (ambiguous values) and a plain-English why."""
    if locked:
        return {"conv": locked, "mixed": False, "flag": False,
                "why": f"the format was confirmed as {locked.upper()} for this source"}
    if dmy_ev and not mdy_ev:
        return {"conv": "dmy", "mixed": False, "flag": False,
                "why": f"{dmy_ev} value(s) like 28/08 prove the day comes first"}
    if mdy_ev and not dmy_ev:
        return {"conv": "mdy", "mixed": False, "flag": False,
                "why": f"{mdy_ev} value(s) like 08/28 prove the month comes first"}
    mixed = bool(dmy_ev and mdy_ev)
    default_txt = "DD/MM" if default == "dmy" else "MM/DD"
    why = ("the column mixes both orders, so clear values follow themselves and unclear ones use the default "
           f"{default_txt} (flagged)") if mixed else \
        f"no value proves the order, so the default {default_txt} was used and unclear values were flagged"
    return {"conv": default, "mixed": mixed, "flag": True, "why": why}


def parse_date(s: Column, decision: dict, as_of_year: int, shape: Column = None):
    """Parse each shape with its own parts -> 'yyyy-MM-dd' -> to_date (one explicit format).
    `shape` = date_shape(s), ideally an already materialised column. Returns (date, ambiguous, two_digit_year)."""
    low = F.lower(s)
    shape = date_shape(s) if shape is None else shape
    y = m = d = F.lit(None).cast("string")
    for name, (gy, gm, gd) in DATE_PARTS.items():
        rx = DATE_SHAPES[name]
        hit = shape == name
        y = F.when(hit, F.regexp_extract(low, rx, gy)).otherwise(y)
        m = F.when(hit, F.regexp_extract(low, rx, gm)).otherwise(m)
        d = F.when(hit, F.regexp_extract(low, rx, gd)).otherwise(d)
    # numeric a/b/yyyy: order decided per column (decide_order)
    a, b = numeric_parts(s)
    is_num = shape == "numeric"
    conv = F.lit(decision["conv"] == "dmy")
    day_first = ((a > 12) & (b <= 12)) | (~((b > 12) & (a <= 12)) & conv) if decision["mixed"] else conv
    rxn = DATE_SHAPES["numeric"]
    y = F.when(is_num, F.regexp_extract(low, rxn, 3)).otherwise(y)
    d = F.when(is_num, F.when(day_first, a).otherwise(b).cast("string")).otherwise(d)
    m = F.when(is_num, F.when(day_first, b).otherwise(a).cast("string")).otherwise(m)
    # month names -> numbers
    m = F.when(m.rlike("^[a-z]"), lit_map(MONTHS)[F.substring(m, 1, 3)].cast("string")).otherwise(m)
    # two-digit years: pivot on the as-of date (yy above this year's yy -> 19yy, else 20yy)
    two = F.length(y) == 2
    y = F.when(two, F.when(y.cast("int") > (as_of_year % 100), F.concat(F.lit("19"), y))
               .otherwise(F.concat(F.lit("20"), y))).otherwise(y)
    iso = F.concat_ws("-", y, F.lpad(m, 2, "0"), F.lpad(d, 2, "0"))
    out = F.when(shape.isNotNull() & (shape != "unrecognised") & m.isNotNull(), F.to_date(iso, "yyyy-MM-dd"))
    ambiguous = (F.lit(decision["flag"]) & is_num & (a <= 12) & (b <= 12) & (a != b)) if decision["flag"] \
        else F.lit(False)
    return out, ambiguous, two & shape.isin("numeric", "d_mon_y")


def parse_datetime(s: Column):
    """ISO date-time only: 2026-10-01 18:42[:00][.123][Z], 'T' or space -> TIMESTAMP_NTZ (business wall time)."""
    ok = s.rlike(DATETIME_RE)
    g = [F.regexp_extract(s, DATETIME_RE, i) for i in range(1, 6)]
    sec = F.when(g[3] == "", "00").otherwise(g[3])
    base = F.concat(g[0], F.lit(" "), F.lpad(g[1], 2, "0"), F.lit(":"), g[2], F.lit(":"), sec)
    frac = F.rpad(F.substring(g[4], 1, 6), 6, "0")
    out = F.when(ok & (g[4] == ""), F.to_timestamp_ntz(base, F.lit("yyyy-MM-dd HH:mm:ss"))) \
        .when(ok, F.to_timestamp_ntz(F.concat(base, F.lit("."), frac), F.lit("yyyy-MM-dd HH:mm:ss.SSSSSS")))
    return out, s.isNotNull() & out.isNull(), "unparseable_datetime"


# ---------- categories ----------
def cat_key_py(v) -> str:
    """Loose matching key: lower case, letters and digits only ('Blue-Dart', 'blue dart' -> 'bluedart')."""
    return re.sub(r"[^a-z0-9]", "", str(v).lower())


def cat_key(s: Column) -> Column:
    return F.concat(F.lit("~"), F.regexp_replace(F.lower(s), "[^a-z0-9]", ""))


def category_lookup(mapping_spec: dict) -> dict:
    """Config mapping -> {lower(raw): clean, '~'+loose_key: clean}. Exact (case-insensitive) keys win; the loose key
    (punctuation/spacing ignored) is added only when it points to ONE clean value, so 'A+' and 'A' never merge."""
    m, loose = {}, {}
    pairs = [(v, v) for v in (mapping_spec.get("valid", []) or [])] + list((mapping_spec.get("values") or {}).items())
    for raw, clean in pairs:
        clean = None if clean is None else str(clean)
        m[str(raw).strip().lower()] = clean
        loose.setdefault("~" + cat_key_py(raw), set()).add(clean)
    for k, targets in loose.items():
        if len(targets) == 1 and k != "~":
            m.setdefault(k, next(iter(targets)))
    return m


def category_known(s: Column, lookup: dict) -> Column:
    if not lookup:
        return F.lit(False)
    return F.lower(s).isin([k for k in lookup if not k.startswith("~")]) | \
        cat_key(s).isin([k for k in lookup if k.startswith("~")])


def fuzzy_category(s: Column, valid: list[str], max_dist: int) -> Column:
    """Closest valid value if exactly ONE is within max_dist edits (Spark levenshtein), else NULL. Column op only."""
    if not valid or not max_dist:
        return F.lit(None).cast("string")
    hits = [F.when(F.levenshtein(F.lower(s), F.lit(str(v).lower())) <= int(max_dist), F.lit(str(v))) for v in valid]
    n = sum(F.when(h.isNotNull(), 1).otherwise(0) for h in hits)
    return F.when(n == 1, F.coalesce(*hits))


def map_category(s: Column, lookup: dict, valid: list[str] | None = None, fuzzy: int = 0):
    """Exact mapping first; unmapped values may be fixed by fuzzy match (one unique close valid value)."""
    m = lit_map(lookup) if lookup else None
    exact = F.coalesce(m[F.lower(s)], m[cat_key(s)]) if lookup else F.lit(None).cast("string")
    known = category_known(s, lookup)
    near = fuzzy_category(s, valid or [], fuzzy)
    out = F.when(known, exact).otherwise(near)
    return out, s.isNotNull() & ~known & near.isNull(), "unmapped_value"
