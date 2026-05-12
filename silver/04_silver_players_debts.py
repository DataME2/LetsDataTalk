"""
SILVER — Players Debts (cleansed, unioned, with CollectionRate everywhere).

Replaces the M queries' transformation logic for:
  - PlayersDebts_2024Season
  - PlayersDebts_2025Season
  - PlayersDebts_2026Season

The M code only computed `CollectionRate` for the 2026 season — odd
restriction, almost certainly an oversight. We compute it for every season
because the metric is just as useful historically.

Output: one row per InvoiceRef across all seasons, deduped, with collection
rate. Same "latest snapshot wins" guard as silver_invoices.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CATALOG, SCHEMA_BRONZE, SCHEMA_SILVER
from transformations import (
    extract_snapshot_ts,
    keep_latest_snapshot,
    safe_cast,
)


def _build_debts_silver(bronze_table: str, season: int) -> DataFrame:
    df = spark.read.table(bronze_table)

    # Step 1: type casts (mirrors M's Changed Type, but with try_cast).
    cast_specs = [
        ("InvoiceDate",         "date",   None),
        ("InvoiceAmount",       "double", 0.0),
        ("TotalPaymentAmount",  "double", 0.0),
        ("TotalCreditAmount",   "double", 0.0),
        ("OutstandingAmount",   "double", 0.0),
        ("CommitmentAmount",    "double", 0.0),
        ("PendingAmount",       "double", 0.0),
        ("PayableAmount",       "double", 0.0),
        ("NumberOfPayments",    "int",    0),
        ("NumberOfCredits",     "int",    0),
        ("LastPaymentDate",     "date",   None),
        ("LastCreditDate",      "date",   None),
    ]
    for col_name, target, default in cast_specs:
        if col_name in df.columns:
            df = df.withColumn(col_name, safe_cast(col_name, target, default))

    # Step 2: distinct by InvoiceRef (M's Removed Duplicates)
    df = df.dropDuplicates(["InvoiceRef"])

    # Step 3: replace empty TeamName with the same friendly placeholder
    df = df.withColumn(
        "TeamName",
        F.when((F.col("TeamName").isNull()) | (F.col("TeamName") == ""),
               F.lit("No team register yet!"))
         .otherwise(F.col("TeamName"))
    )

    # Step 4: CollectionRate (M does this only for 2026; we do it always).
    # Guard divide-by-zero / null InvoiceAmount → return 0.
    df = df.withColumn(
        "CollectionRate",
        F.when((F.col("InvoiceAmount").isNull()) | (F.col("InvoiceAmount") == 0),
               F.lit(0.0))
         .otherwise(F.col("TotalPaymentAmount") / F.col("InvoiceAmount"))
    )

    # Step 5: stamp Season + extract snapshot_ts for downstream dedup
    df = (
        df.withColumn("Season", F.lit(season).cast("int"))
          .withColumn("snapshot_ts", extract_snapshot_ts())
    )

    return df


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_players_debts_2024",
    comment="2024 invoice/debt summary per player, deduped by InvoiceRef.",
    table_properties={"quality": "silver"},
)
def silver_players_debts_2024() -> DataFrame:
    df = _build_debts_silver(
        f"{CATALOG}.{SCHEMA_BRONZE}.bronze_players_debts_2024", 2024,
    )
    return keep_latest_snapshot(df, business_keys=["InvoiceRef"])


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_players_debts_2025",
    comment="2025 invoice/debt summary per player, deduped by InvoiceRef.",
    table_properties={"quality": "silver"},
)
def silver_players_debts_2025() -> DataFrame:
    df = _build_debts_silver(
        f"{CATALOG}.{SCHEMA_BRONZE}.bronze_players_debts_2025", 2025,
    )
    return keep_latest_snapshot(df, business_keys=["InvoiceRef"])


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_players_debts_2026",
    comment="2026 invoice/debt summary per player, deduped by InvoiceRef.",
    table_properties={"quality": "silver"},
)
def silver_players_debts_2026() -> DataFrame:
    df = _build_debts_silver(
        f"{CATALOG}.{SCHEMA_BRONZE}.bronze_players_debts_2026", 2026,
    )
    return keep_latest_snapshot(df, business_keys=["InvoiceRef"])


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_players_debts",
    comment=(
        "Conformed players-debts table across all seasons. CollectionRate "
        "computed for every row (TotalPaymentAmount / InvoiceAmount, with "
        "divide-by-zero protection). Empty TeamName values replaced with "
        "'No team register yet!' to keep the gold dim_team join clean."
    ),
    table_properties={"quality": "silver"},
)
@dp.expect("invoice_ref_present", "InvoiceRef IS NOT NULL")
@dp.expect("amounts_non_negative",
           "InvoiceAmount >= 0 AND TotalPaymentAmount >= 0")
@dp.expect("collection_rate_in_range",
           "CollectionRate >= 0 AND CollectionRate <= 1.5")
def silver_players_debts() -> DataFrame:
    df_24 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_players_debts_2024")
    df_25 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_players_debts_2025")
    df_26 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_players_debts_2026")

    return (
        df_24
        .unionByName(df_25, allowMissingColumns=True)
        .unionByName(df_26, allowMissingColumns=True)
    )