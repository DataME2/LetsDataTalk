"""
SILVER — Teams (TeamDetails + TeamLists).

Replaces the M queries' transformation logic for:
  - TeamDetails_2026Season-PlayerRegistration(AllCompetitions)_*
  - TeamLists_2026Season-PlayerRegistration(AllCompetitions)_*

Both M queries were trivial: PromoteHeaders → ChangeTypes (+ TeamLists also
replaces null PrimaryContactPhoneNumber and FFA Number with 0). We do the
same in PySpark, plus the latest-snapshot dedup that the M didn't.

Note on `IsActive`: stored as 'Y'/'N' in the source. We keep the original
character but also expose a boolean alias for downstream filters.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CATALOG, SCHEMA_BRONZE, SCHEMA_SILVER
from transformations import safe_cast, keep_latest_snapshot, extract_snapshot_ts


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_team_details",
    comment=(
        "Cleansed team registry: one row per team per snapshot's-latest. "
        "IsActive kept as 'Y'/'N' string + exposed as boolean is_active."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dp.expect_or_drop("team_name_present", "TeamName IS NOT NULL AND TeamName <> ''")
def silver_team_details() -> DataFrame:
    df = spark.read.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_team_details")

    df = (
        df
        .withColumn("NumberOfOfficials", safe_cast("NumberOfOfficials", "int", 0))
        .withColumn("NumberOfPlayers",   safe_cast("NumberOfPlayers",   "int", 0))
        .withColumn("is_active",
                    F.when(F.upper(F.col("IsActive")) == "Y", F.lit(True))
                     .otherwise(F.lit(False)))
        .withColumn("snapshot_ts", extract_snapshot_ts())
    )

    return keep_latest_snapshot(df, business_keys=["TeamName"])


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_team_lists",
    comment=(
        "Cleansed team rosters: one row per (team, person, role). NULL "
        "PrimaryContactPhoneNumber and `FFA Number` defaulted to 0 to "
        "match the M code's ReplaceValue behaviour."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dp.expect("first_name_present", "FirstName IS NOT NULL")
@dp.expect("surname_present",    "Surname   IS NOT NULL")
def silver_team_lists() -> DataFrame:
    df = spark.read.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_team_lists")

    cast_specs = [
        ("DateOfBirth",                "date",   None),
        ("RegistrationDate",           "date",   None),
        ("PrimaryContactMobileNumber", "long",   0),
        ("PrimaryContactPhoneNumber",  "long",   0),
        ("FFA Number",                 "long",   0),
    ]
    for col_name, target, default in cast_specs:
        if col_name in df.columns:
            df = df.withColumn(col_name, safe_cast(col_name, target, default))

    df = df.withColumn("snapshot_ts", extract_snapshot_ts())

    # Latest snapshot per (TeamName, FFANumber, Role) — same person could be a
    # coach AND a player on the same team, so include Role in the key.
    return keep_latest_snapshot(
        df,
        business_keys=["TeamName", "`FFA Number`", "Role"],
    )