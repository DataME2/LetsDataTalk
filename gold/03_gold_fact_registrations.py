"""
GOLD — fact_registrations (player-season grain).

One row per (player, season). Contains the contextual attributes a typical
registration dashboard cares about: age at registration, school, last-season
club, age band, etc. Useful for cohort analyses ("how many U10 players
came back from 2024 to 2025?").
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CATALOG, SCHEMA_SILVER, SCHEMA_GOLD


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_GOLD}.fact_registrations",
    comment=(
        "Player-season grain registration fact. One row per player per "
        "season they registered. Includes age at registration, derived age "
        "band (MiniRoos U6-U11, Junior U12-U15, Senior U16+) and a "
        "renewal flag indicating whether the player was registered the "
        "prior season too."
    ),
    table_properties={"quality": "gold"},
)
def fact_registrations() -> DataFrame:
    src = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_players")

    # Age at registration (approx — use registration date - DOB)
    fact = src.withColumn(
        "age_at_registration",
        F.floor(
            F.datediff(F.col("RegistrationDate"), F.col("PlayerDateOfBirth")) / 365.25
        ).cast("int")
    )

    # Age band — sensible buckets for grassroots football reporting
    fact = fact.withColumn(
        "age_band",
        F.when(F.col("age_at_registration") < 12, F.lit("MiniRoos (U6-U11)"))
         .when(F.col("age_at_registration") < 16, F.lit("Junior (U12-U15)"))
         .when(F.col("age_at_registration") < 18, F.lit("Youth (U16-U17)"))
         .when(F.col("age_at_registration") < 35, F.lit("Senior"))
         .when(F.col("age_at_registration").isNotNull(), F.lit("Masters"))
         .otherwise(F.lit("Unknown"))
    )

    # Renewal flag: does this player_id also appear in (Season - 1)?
    prior = (
        src.select(
            F.col("player_id"),
            (F.col("Season") + 1).alias("Season"),  # shift forward
            F.lit(True).alias("registered_prior_season"),
        )
        .dropDuplicates(["player_id", "Season"])
    )

    fact = (
        fact.join(prior, ["player_id", "Season"], "left")
            .withColumn(
                "registered_prior_season",
                F.coalesce(F.col("registered_prior_season"), F.lit(False))
            )
    )

    return fact.select(
        F.col("player_id"),
        F.col("Season").alias("season"),
        F.col("household_id"),
        F.col("RegistrationDate").alias("registration_date"),
        F.col("RegistrationStatus").alias("registration_status"),
        F.col("age_at_registration"),
        F.col("age_band"),
        F.col("PlayerPreferredPlayingGroup").alias("preferred_playing_group"),
        F.col("PlayerPlayedLastSeason").alias("played_last_season"),
        F.col("PlayerPreviousClubName").alias("previous_club_name"),
        F.col("PlayerSchoolName").alias("school_name"),
        F.col("PlayerSchoolGrade").alias("school_grade"),
        F.col("PlayerSlot").alias("household_player_slot"),
        F.col("registered_prior_season"),
        F.col("Suburb").alias("suburb"),
        F.col("State").alias("state"),
        F.col("Postcode").alias("postcode"),
    )