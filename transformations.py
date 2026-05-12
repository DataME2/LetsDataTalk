"""
Reusable PySpark transformation utilities.

Each function corresponds to one or more M-code helpers from the original
Power Query and is parameterised so multiple silver datasets can share it.
"""
from __future__ import annotations

from typing import Iterable, Sequence

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from config import (
    FEE_CATEGORY_DEFAULT,
    FEE_CATEGORY_RULES,
    INVOICE_HEADER_COLS,
)

# ---------------------------------------------------------------------------
# Metadata enrichment
# ---------------------------------------------------------------------------
def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Stamp every row with the source file path and ingestion timestamp.

    Mirrors the standard bronze pattern: keep enough provenance to reprocess
    or troubleshoot from the raw drop. Uses Auto Loader's `_metadata` column
    rather than `input_file_name()` (deprecated for streaming + Volumes).
    """
    return (
        df
        .withColumn("_source_file",  F.col("_metadata.file_path"))
        .withColumn("_ingest_ts",    F.current_timestamp())
    )


def extract_snapshot_ts(file_col: Column = F.col("_source_file")) -> Column:
    """Pull the 14-digit timestamp from snapshot filenames.

    Filename pattern (per the user's drops):
      `RegistrationInvoices_2024Season-...__YYYYMMDDHHmmss.csv`
    The trailing 14 digits encode when the upstream system exported the file,
    which is how silver picks "latest snapshot wins".
    """
    raw = F.regexp_extract(file_col, r"_(\d{14})\.csv$", 1)
    return F.to_timestamp(raw, "yyyyMMddHHmmss").alias("snapshot_ts")


def extract_season(file_col: Column = F.col("_source_file")) -> Column:
    """Pull the 4-digit season year out of the filename, e.g. 2024Season → 2024."""
    return F.regexp_extract(file_col, r"_(\d{4})Season", 1).cast("int").alias("season")


# ---------------------------------------------------------------------------
# Type casting (M's Replaced Errors equivalent)
# ---------------------------------------------------------------------------
def safe_cast(col_name: str, target_type: str, default=None) -> Column:
    """Equivalent to M's `Table.ReplaceErrorValues` after a type cast.

    `try_cast` returns NULL on failure (instead of raising), then `coalesce`
    swaps NULL for the default. Use `default=None` if you want plain NULL.
    """
    casted = F.expr(f"try_cast(`{col_name}` AS {target_type})")
    if default is None:
        return casted.alias(col_name)
    return F.coalesce(casted, F.lit(default).cast(target_type)).alias(col_name)


# ---------------------------------------------------------------------------
# Text.Clean — strip control characters
# ---------------------------------------------------------------------------
# Power Query's `Text.Clean` removes characters whose code point is < 0x20
# (control chars). PySpark equivalent: regexp_replace with the same range.
def text_clean(col: Column) -> Column:
    """Drop ASCII control characters (< U+0020) — Power Query's Text.Clean."""
    return F.regexp_replace(col, r"[\x00-\x1F]", "")


def apply_text_clean(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    """Run text_clean over a set of columns in place."""
    out = df
    for c in columns:
        out = out.withColumn(c, text_clean(F.col(c)))
    return out


# ---------------------------------------------------------------------------
# Null replacement helper
# ---------------------------------------------------------------------------
def replace_nulls(df: DataFrame, mapping: dict[str, object]) -> DataFrame:
    """
    Replace NULL with the given default per column.

    Equivalent to a chain of `Table.ReplaceValue(..., null, "...", ..., {col})`
    in M. Skips columns that aren't in the dataframe so the helper stays
    forgiving when schemas evolve.
    """
    out = df
    for col_name, default in mapping.items():
        if col_name in out.columns:
            out = out.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(default))
                 .otherwise(F.col(col_name))
            )
    return out


# ---------------------------------------------------------------------------
# Latest snapshot wins (M doesn't do this — needed because we accept multiple
# snapshot CSVs for the same season; the upstream system re-exports daily)
# ---------------------------------------------------------------------------
def keep_latest_snapshot(
    df: DataFrame,
    business_keys: Sequence[str],
    snapshot_col: str = "snapshot_ts",
) -> DataFrame:
    """
    Within each business key, keep the row from the most recent snapshot.

    This is essential because the source system drops dated CSVs over time
    and rows for the same Reference / FFA Number can appear in many of them
    with progressively updated values (e.g. payment received).
    """
    w = Window.partitionBy(*business_keys).orderBy(F.col(snapshot_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Fee categorisation (replaces the giant if/else AddColumn from M)
# ---------------------------------------------------------------------------
def categorize_fee(line_desc_clean: Column) -> Column:
    """
    Map a cleaned line description to one of five fee categories.

    Walks `FEE_CATEGORY_RULES` in declared order; first match wins. Falls
    through to FEE_CATEGORY_DEFAULT. Matches the original M chain exactly.
    """
    lower = F.lower(line_desc_clean)

    # Build the chain in REVERSE so the resulting expression matches rules
    # in the original DECLARED order (first-match-wins, like the M if/else):
    #   when(rule_1, cat_1).otherwise(
    #     when(rule_2, cat_2).otherwise(
    #       ...otherwise(default)))
    expr = F.lit(FEE_CATEGORY_DEFAULT)
    for category, keywords in reversed(FEE_CATEGORY_RULES):
        cond = None
        for kw in keywords:
            cond = lower.contains(kw) if cond is None else cond | lower.contains(kw)
        expr = F.when(cond, F.lit(category)).otherwise(expr)

    return expr


# ---------------------------------------------------------------------------
# Invoice forward-fill (M's Table.FillDown after ghost-row removal)
# ---------------------------------------------------------------------------
def forward_fill_invoice_headers(df: DataFrame) -> DataFrame:
    """
    Replicate M's `Table.FillDown` on invoice header columns.

    Source CSV layout: each invoice spans N rows. The first row carries
    Surname/First Name/Reference/Date/amounts + line item 1; subsequent
    rows have NULL for those header columns and only the next line item.
    We need each line row to carry its parent's header values for
    grouping and pivoting later.

    Strategy:
      1. Remove "ghost rows" (where Reference is null AND every amount is
         also null — these are blank rows that the upstream export emits
         between invoices).
      2. Build an invoice group id per file via cumulative count of
         non-null Reference. Every parent row increments the group.
      3. Forward-fill the header columns within each (file, group) using
         a window with `last(..., ignoreNulls=true)`.
    """
    amount_cols = [
        "Invoice Amount", "Paid Amount", "Outstanding Amount",
        "Commitment Amount", "Pending Amount", "Payable Amount",
    ]
    all_amounts_null = None
    for c in amount_cols:
        chk = F.col(f"`{c}`").isNull()
        all_amounts_null = chk if all_amounts_null is None else all_amounts_null & chk

    # Step 1: ghost-row filter — keep parent rows OR rows that look like
    # genuine line items (Reference null but amounts also null, meaning
    # they're truly child rows of a parent we just saw).
    cleaned = df.filter(
        F.col("`Reference`").isNotNull() | all_amounts_null
    )

    # Stable per-file ordering. monotonically_increasing_id() is monotonic
    # within a single read; combined with the file path we have a usable
    # ordering for forward-fill.
    cleaned = cleaned.withColumn("_rid", F.monotonically_increasing_id())

    # Step 2: derive invoice_grp via cumulative sum of "is parent"
    is_parent = F.when(F.col("`Reference`").isNotNull(), 1).otherwise(0)
    w_grp = (
        Window
        .partitionBy("_source_file")
        .orderBy("_rid")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    cleaned = cleaned.withColumn("_invoice_grp", F.sum(is_parent).over(w_grp))

    # Step 3: forward-fill header cols within each (_source_file, _invoice_grp)
    w_fill = (
        Window
        .partitionBy("_source_file", "_invoice_grp")
        .orderBy("_rid")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    out = cleaned
    for c in INVOICE_HEADER_COLS:
        if c in out.columns:
            out = out.withColumn(
                c,
                F.last(F.col(f"`{c}`"), ignorenulls=True).over(w_fill)
            )

    # Drop helper columns; keep _source_file because metadata is useful downstream
    return out.drop("_rid", "_invoice_grp")


# ---------------------------------------------------------------------------
# Player block flattening (silver layer — M doesn't unpivot but we should)
# ---------------------------------------------------------------------------
def explode_player_slots(
    df: DataFrame,
    season: int,
    slot_indices: Iterable[int],
) -> DataFrame:
    """
    Convert the wide registration rows (with PlayerN* columns for N=1..5) into
    long form: one row per (registration, player slot) where the slot has data.

    The previous-club-name column varies by season (Player12023ClubName for
    2024, Player12024ClubName for 2025, etc.) so we resolve it dynamically.
    """
    prev_year = season - 1
    parts: list[DataFrame] = []
    keep_cols = [c for c in df.columns if not c.startswith("Player")]

    for n in slot_indices:
        prev_club_col = f"Player{n}{prev_year}ClubName"
        # Build a select list mapping the slot's columns to standard names
        select_exprs = [F.col(c) for c in keep_cols]
        select_exprs += [
            F.lit(n).alias("PlayerSlot"),
            F.col(f"Player{n}Surname").alias("PlayerSurname"),
            F.col(f"Player{n}FirstName").alias("PlayerFirstName"),
            F.col(f"Player{n}MiddleName").alias("PlayerMiddleName"),
            F.col(f"Player{n}Gender").alias("PlayerGender"),
            F.col(f"Player{n}DateofBirth").alias("PlayerDateOfBirth"),
            F.col(f"Player{n}FFANumber").alias("PlayerFFANumber"),
            F.col(f"Player{n}PreferredPlayingGroup").alias("PlayerPreferredPlayingGroup"),
            F.col(f"Player{n}PlayedLastSeason").alias("PlayerPlayedLastSeason"),
            (F.col(prev_club_col) if prev_club_col in df.columns else F.lit(None).cast("string"))
                .alias("PlayerPreviousClubName"),
            F.col(f"Player{n}SchoolName").alias("PlayerSchoolName"),
            F.col(f"Player{n}OtherSchool").alias("PlayerOtherSchool"),
            F.col(f"Player{n}SchoolGrade").cast("string").alias("PlayerSchoolGrade"),
        ]
        # FQFeeBypassCode only exists on the 2026 season
        if f"Player{n}FQFeeBypassCode" in df.columns:
            select_exprs.append(
                F.col(f"Player{n}FQFeeBypassCode").alias("PlayerFQFeeBypassCode")
            )
        else:
            select_exprs.append(F.lit(None).cast("string").alias("PlayerFQFeeBypassCode"))

        slot_df = df.select(*select_exprs).filter(
            F.col("PlayerSurname").isNotNull() & (F.col("PlayerSurname") != "")
        )
        parts.append(slot_df)

    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p, allowMissingColumns=True)
    return out