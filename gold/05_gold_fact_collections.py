"""
GOLD — fact_collections (player-season collections fact).

One row per (player, season). Aggregates every invoice attached to that
player across all snapshots into a single financial picture: total billed,
paid, outstanding, commitment, payment count, days since last payment, etc.

This is the dataset most useful to club admins chasing outstanding fees.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CATALOG, SCHEMA_SILVER, SCHEMA_GOLD


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_GOLD}.fact_collections",
    comment=(
        "Player-season collections fact. Aggregates all invoices for each "
        "player in each season into total_billed, total_paid, outstanding, "
        "and a player_collection_rate. Joins to dim_team via team_name."
    ),
    table_properties={"quality": "gold"},
)
def fact_collections() -> DataFrame:
    debts = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_players_debts")

    aggregated = (
        debts.groupBy("PlayerSurname", "PlayerFirstName", "TeamName", "Season")
             .agg(
                 F.sum("InvoiceAmount").alias("total_billed"),
                 F.sum("TotalPaymentAmount").alias("total_paid"),
                 F.sum("TotalCreditAmount").alias("total_credited"),
                 F.sum("OutstandingAmount").alias("total_outstanding"),
                 F.sum("CommitmentAmount").alias("total_commitment"),
                 F.sum("PendingAmount").alias("total_pending"),
                 F.sum("PayableAmount").alias("total_payable"),
                 F.sum("NumberOfPayments").alias("payment_events"),
                 F.sum("NumberOfCredits").alias("credit_events"),
                 F.max("LastPaymentDate").alias("last_payment_date"),
                 F.max("LastCreditDate").alias("last_credit_date"),
                 F.count("*").alias("invoice_count"),
             )
    )

    fact = aggregated.withColumn(
        "player_collection_rate",
        F.when(F.col("total_billed") > 0,
               F.col("total_paid") / F.col("total_billed"))
         .otherwise(F.lit(0.0))
    ).withColumn(
        "days_since_last_payment",
        F.datediff(F.current_date(), F.col("last_payment_date"))
    ).withColumn(
        "is_in_arrears",
        F.col("total_outstanding") > 0
    )

    return fact.select(
        F.col("PlayerSurname").alias("player_surname"),
        F.col("PlayerFirstName").alias("player_first_name"),
        F.col("TeamName").alias("team_name"),
        F.col("Season").alias("season"),
        F.col("invoice_count"),
        F.col("total_billed"),
        F.col("total_paid"),
        F.col("total_credited"),
        F.col("total_outstanding"),
        F.col("total_commitment"),
        F.col("total_pending"),
        F.col("total_payable"),
        F.col("payment_events"),
        F.col("credit_events"),
        F.col("last_payment_date"),
        F.col("last_credit_date"),
        F.col("days_since_last_payment"),
        F.col("player_collection_rate"),
        F.col("is_in_arrears"),
    )
