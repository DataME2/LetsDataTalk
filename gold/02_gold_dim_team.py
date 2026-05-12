"""
GOLD — dim_player (SCD-1 player dimension).

One row per unique player across all seasons. The natural key is the FFA
Number (Football Federation Australia ID), which is unique nationally per
person — so a player who registers in 2024, 2025 and 2026 produces a single
dimension row, with the most recent season's attributes winning.

Players whose FFA number is missing or zero (kids registering for the first
time before being assigned one) get a synthetic player_id derived from
name + DOB.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import CATALOG, SCHEMA_SILVER, SCHEMA_GOLD


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_GOLD}.dim_player",
    comment=(
        "SCD-1 player dimension built from silver_players. One row per "
        "unique player keyed by FFA number (or name+DOB hash when FFA is "
        "missing). Attributes reflect the most recent season the player "
        "appears in."
    ),
    table_properties={"quality": "gold", "delta.feature.allowColumnDefaults": "supported"},
)
def dim_player() -> DataFrame:
    src = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_players")

    # Pick the most recent season's row per player_id as the source of truth
    w = Window.partitionBy("player_id").orderBy(F.col("Season").desc())
    latest = (
        src
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Aggregate the seasons each player has registered in (handy for filtering)
    seasons_agg = (
        src.groupBy("player_id")
           .agg(
               F.collect_set("Season").alias("seasons_registered"),
               F.min("Season").alias("first_season"),
               F.max("Season").alias("last_season"),
               F.count("*").alias("registration_count"),
           )
    )

    return (
        latest.alias("l")
        .join(seasons_agg.alias("s"), "player_id", "left")
        .select(
            F.col("player_id"),
            F.col("PlayerFFANumber").alias("ffa_number"),
            F.col("PlayerSurname").alias("surname"),
            F.col("PlayerFirstName").alias("first_name"),
            F.col("PlayerMiddleName").alias("middle_name"),
            F.col("PlayerGender").alias("gender"),
            F.col("PlayerDateOfBirth").alias("date_of_birth"),
            F.col("PlayerSchoolName").alias("school_name"),
            F.col("PlayerSchoolGrade").alias("school_grade"),
            F.col("PlayerPreviousClubName").alias("previous_club_name"),
            F.col("seasons_registered"),
            F.col("first_season"),
            F.col("last_season"),
            F.col("registration_count"),
            F.col("household_id").alias("latest_household_id"),
            F.current_timestamp().alias("dim_updated_at"),
        )
    )