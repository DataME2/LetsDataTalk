"""
GOLD — fact_invoices (invoice grain).

One row per invoice. The five fee buckets from silver_invoices are kept
as separate measures, plus a derived `payment_status` (Paid / Partial /
Outstanding / Overpaid) and a `days_outstanding` metric measured against
the most recent pipeline run.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CATALOG, SCHEMA_SILVER, SCHEMA_GOLD


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_GOLD}.fact_invoices",
    comment=(
        "Invoice-grain fact. Fee breakdown across registration, governing "
        "body, volunteer levy, sibling discount and other adjustments. "
        "Status derived from outstanding amount; days_outstanding measured "
        "from invoice_date to current date."
    ),
    table_properties={"quality": "gold"},
)
def fact_invoices() -> DataFrame:
    src = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_invoices")

    fact = src.withColumn(
        "payment_status",
        F.when(F.col("outstanding_amount") <= 0,
               F.when(F.col("paid_amount") > F.col("invoice_amount"),
                      F.lit("Overpaid"))
                .otherwise(F.lit("Paid")))
         .when(F.col("paid_amount") > 0, F.lit("Partial"))
         .otherwise(F.lit("Outstanding"))
    ).withColumn(
        "days_since_invoice",
        F.datediff(F.current_date(), F.col("invoice_date"))
    ).withColumn(
        "fee_total_check",
        F.col("registration_fee")
        + F.col("governing_body_fee")
        + F.col("volunteer_levy")
        + F.col("sibling_discount")
        + F.col("other_adjustments")
    )

    return fact.select(
        F.col("invoice_ref"),
        F.col("season"),
        F.col("invoice_date"),
        F.col("surname"),
        F.col("first_name"),
        F.col("invoice_amount"),
        F.col("paid_amount"),
        F.col("outstanding_amount"),
        F.col("commitment_amount"),
        F.col("pending_amount"),
        F.col("payable_amount"),
        F.col("registration_fee"),
        F.col("governing_body_fee"),
        F.col("volunteer_levy"),
        F.col("sibling_discount"),
        F.col("other_adjustments"),
        F.col("fee_total_check"),
        F.col("adjustment_indicator"),
        F.col("payment_status"),
        F.col("days_since_invoice"),
    )