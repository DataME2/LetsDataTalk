"""
SILVER — Registration Invoices (cleansed, pivoted, all seasons unioned).

This is the most involved transformation in the whole pipeline because the
source CSV uses a parent/child row layout that needs forward-fill before any
analytics make sense. The M code does it in 13 numbered steps; we mirror
those steps in PySpark while also adding a "latest snapshot wins" pass that
the M version skipped (M was loading a single CSV, we load all snapshots).

Final shape: one row per invoice (Reference). Five fee-category columns
sum the line amounts that fell into each bucket, in $.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (
    CATALOG, SCHEMA_BRONZE, SCHEMA_SILVER,
    FEE_CATEGORY_RULES, FEE_CATEGORY_DEFAULT,
)
from transformations import (
    categorize_fee,
    extract_snapshot_ts,
    forward_fill_invoice_headers,
    keep_latest_snapshot,
    safe_cast,
)


def _build_invoice_silver(bronze_table: str, season: int) -> DataFrame:
    """Run all 13 M-code steps for one season's invoices."""
    df = spark.read.table(bronze_table)

    # Step 0: type cast (M's Changed Types). All bronze cols are strings,
    # so we cast the numeric/date ones with try_cast.
    numeric_cols = [
        "Line Nbr", "Line Amount", "Invoice Amount", "Paid Amount",
        "Outstanding Amount", "Commitment Amount", "Pending Amount",
        "Payable Amount",
    ]
    for c in numeric_cols:
        if c in df.columns:
            target = "long" if c == "Line Nbr" else "double"
            df = df.withColumn(c, safe_cast(c, target))

    # Steps 4 + 5: ghost-row removal + forward-fill (delegated to helper)
    df = forward_fill_invoice_headers(df)

    # Step 6: drop rows where Date is still null/empty after the fill
    df = df.filter(F.col("`Date`").isNotNull() & (F.col("`Date`") != ""))

    # Step 7: extract Player Name from Line Description (text after \r).
    # The CSV stores `"<fee description>\r<player name>"` in a single cell.
    df = df.withColumn(
        "PlayerName",
        F.when(F.col("`Line Description`").contains("\r"),
               F.element_at(F.split(F.col("`Line Description`"), "\r"), -1))
         .otherwise(F.col("`Line Description`"))
    )

    # Step 8: clean line description = text BEFORE \r (the fee descriptor only)
    df = df.withColumn(
        "LineDescriptionClean",
        F.element_at(F.split(F.col("`Line Description`"), "\r"), 1),
    )

    # Step 9: bucket every line item into one of the fee categories
    df = df.withColumn("FeeCategory", categorize_fee(F.col("LineDescriptionClean")))

    # Step 10/11: pivot fee categories → 5 columns of summed Line Amount,
    # grouped by the invoice header columns.
    group_cols = [
        "Surname", "First Name", "Reference", "Date",
        "Invoice Amount", "Paid Amount", "Outstanding Amount",
        "Commitment Amount", "Pending Amount", "Payable Amount",
        "Adjustment Indicator", "_source_file",
    ]
    group_cols = [c for c in group_cols if c in df.columns]

    pivoted = (
        df.groupBy(*[F.col(f"`{c}`") for c in group_cols])
          .pivot("FeeCategory",
                 [c for c, _ in FEE_CATEGORY_RULES] + [FEE_CATEGORY_DEFAULT])
          .agg(F.sum("`Line Amount`"))
    )

    # Step 12: stamp Season
    pivoted = pivoted.withColumn("Season", F.lit(season).cast("int"))

    # Step 13: cast Date now that we've grouped, and zero-fill the fee buckets
    pivoted = pivoted.withColumn("Date", safe_cast("Date", "date"))
    fee_cols = [c for c, _ in FEE_CATEGORY_RULES] + [FEE_CATEGORY_DEFAULT]
    for c in fee_cols:
        if c in pivoted.columns:
            pivoted = pivoted.withColumn(c, F.coalesce(F.col(f"`{c}`"), F.lit(0.0)))

    # Standardise pivoted column names to snake_case for downstream sanity
    rename_map = {
        "Surname":               "surname",
        "First Name":            "first_name",
        "Reference":             "invoice_ref",
        "Date":                  "invoice_date",
        "Invoice Amount":        "invoice_amount",
        "Paid Amount":           "paid_amount",
        "Outstanding Amount":    "outstanding_amount",
        "Commitment Amount":     "commitment_amount",
        "Pending Amount":        "pending_amount",
        "Payable Amount":        "payable_amount",
        "Adjustment Indicator":  "adjustment_indicator",
        "Registration Fee":      "registration_fee",
        "Governing Body Fee":    "governing_body_fee",
        "Volunteer Levy":        "volunteer_levy",
        "Sibling Discount":      "sibling_discount",
        "Other Adjustments":     "other_adjustments",
    }
    for old, new in rename_map.items():
        if old in pivoted.columns:
            pivoted = pivoted.withColumnRenamed(old, new)

    # Tag with snapshot_ts so the unified silver can pick latest per Reference
    pivoted = pivoted.withColumn("snapshot_ts", extract_snapshot_ts())

    return pivoted


# ---------------------------------------------------------------------------
# Per-season silver invoices
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_invoices_2024",
    comment="2024 invoices, pivoted by fee category, latest snapshot per Reference.",
    table_properties={"quality": "silver"},
)
def silver_invoices_2024() -> DataFrame:
    df = _build_invoice_silver(
        f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registration_invoices_2024", 2024,
    )
    return keep_latest_snapshot(df, business_keys=["invoice_ref"])


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_invoices_2025",
    comment="2025 invoices, pivoted by fee category, latest snapshot per Reference.",
    table_properties={"quality": "silver"},
)
def silver_invoices_2025() -> DataFrame:
    df = _build_invoice_silver(
        f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registration_invoices_2025", 2025,
    )
    return keep_latest_snapshot(df, business_keys=["invoice_ref"])


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_invoices_2026",
    comment="2026 invoices, pivoted by fee category, latest snapshot per Reference.",
    table_properties={"quality": "silver"},
)
def silver_invoices_2026() -> DataFrame:
    df = _build_invoice_silver(
        f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registration_invoices_2026", 2026,
    )
    return keep_latest_snapshot(df, business_keys=["invoice_ref"])


# ---------------------------------------------------------------------------
# Unified silver invoices — all seasons in one table
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_invoices",
    comment=(
        "Cleansed invoices across all seasons. Fee categories pivoted into "
        "five named columns (registration_fee, governing_body_fee, "
        "volunteer_levy, sibling_discount, other_adjustments). One row per "
        "invoice_ref — when the same invoice appears in multiple snapshots, "
        "the latest snapshot wins."
    ),
    table_properties={"quality": "silver"},
)
@dp.expect("invoice_ref_present",  "invoice_ref IS NOT NULL")
@dp.expect("invoice_amount_non_negative", "invoice_amount >= 0")
@dp.expect_or_drop("invoice_date_present", "invoice_date IS NOT NULL")
def silver_invoices() -> DataFrame:
    df_24 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_invoices_2024")
    df_25 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_invoices_2025")
    df_26 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_invoices_2026")

    return (
        df_24
        .unionByName(df_25, allowMissingColumns=True)
        .unionByName(df_26, allowMissingColumns=True)
    )