"""
GOLD — summary_season (one row per season — executive KPIs).

Top-of-funnel dashboard table: registrations, distinct players, billed,
collected, outstanding, collection rate, and renewal rate per season.

This is the table the club president looks at first thing every morning.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CATALOG, SCHEMA_GOLD, SCHEMA_SILVER


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_GOLD}.summary_season",
    comment=(
        "Season-level executive KPIs: distinct players, total billed, paid, "
        "outstanding, collection rate, renewal rate (share of players who "
        "registered both this season and last)."
    ),
    table_properties={"quality": "gold"},
)
def summary_season() -> DataFrame:
    invoices_fact     = spark.read.table(f"{CATALOG}.{SCHEMA_GOLD}.fact_invoices")
    registrations_fact = spark.read.table(f"{CATALOG}.{SCHEMA_GOLD}.fact_registrations")

    # Financial roll-up from invoices fact
    fin = (
        invoices_fact
        .groupBy("season")
        .agg(
            F.count("*").alias("invoice_count"),
            F.sum("invoice_amount").alias("total_billed"),
            F.sum("paid_amount").alias("total_collected"),
            F.sum("outstanding_amount").alias("total_outstanding"),
            F.sum("registration_fee").alias("registration_fees"),
            F.sum("governing_body_fee").alias("governing_body_fees"),
            F.sum("volunteer_levy").alias("volunteer_levies"),
            F.sum("sibling_discount").alias("sibling_discounts"),
            F.sum(F.when(F.col("payment_status") == "Outstanding", 1)
                   .otherwise(0)).alias("outstanding_invoice_count"),
        )
        .withColumn(
            "season_collection_rate",
            F.when(F.col("total_billed") > 0,
                   F.col("total_collected") / F.col("total_billed"))
             .otherwise(F.lit(0.0))
        )
    )

    # Player metrics from registrations fact
    reg = (
        registrations_fact
        .groupBy("season")
        .agg(
            F.countDistinct("player_id").alias("distinct_players"),
            F.countDistinct("household_id").alias("distinct_households"),
            F.sum(F.when(F.col("registered_prior_season"), 1)
                   .otherwise(0)).alias("renewed_players"),
        )
        .withColumn(
            "renewal_rate",
            F.when(F.col("distinct_players") > 0,
                   F.col("renewed_players") / F.col("distinct_players"))
             .otherwise(F.lit(0.0))
        )
    )

    return fin.join(reg, "season", "outer").orderBy("season")