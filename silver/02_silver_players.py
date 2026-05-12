"""
SILVER — Players (player-grain, exploded from registrations).

The original M code keeps registrations in wide form: one row per household
with up to 5 PlayerN* column blocks. That's awful for analytics —
"how many U10 players are registered" requires UNION ALL of 5 wide selects.

This silver dataset doesn't have a 1:1 M-code counterpart, but it's a
necessary normalisation step for any sensible downstream gold model.
We explode each filled player slot into its own row.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CATALOG, SCHEMA_SILVER, PLAYER_SLOTS
from transformations import explode_player_slots


def _explode_one_season(season: int) -> DataFrame:
    df = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_registrations_{season}")
    return explode_player_slots(df, season=season, slot_indices=PLAYER_SLOTS)


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_players",
    comment=(
        "Player-grain registration data: one row per (household, season, "
        "player slot) where the slot has any data. The 5 PlayerN* column "
        "blocks from the wide registration form are unpivoted into "
        "standardised PlayerSurname / PlayerFFANumber / etc. columns."
    ),
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("player_name_present", "PlayerSurname IS NOT NULL AND PlayerSurname <> ''")
@dp.expect("ffa_number_present_when_known", "PlayerFFANumber IS NOT NULL OR PlayerFFANumber = 0")
def silver_players() -> DataFrame:
    parts = [_explode_one_season(s) for s in [2024, 2025, 2026]]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p, allowMissingColumns=True)

    # Compute a stable player_id from FFA number when present, else from
    # name+DOB. FFA numbers are unique per registered player nationally.
    return out.withColumn(
        "player_id",
        F.when(F.col("PlayerFFANumber") > 0,
               F.sha2(F.col("PlayerFFANumber").cast("string"), 256))
         .otherwise(F.sha2(F.concat_ws("|",
             F.coalesce(F.col("PlayerSurname"), F.lit("")),
             F.coalesce(F.col("PlayerFirstName"), F.lit("")),
             F.coalesce(F.col("PlayerDateOfBirth").cast("string"), F.lit("")),
         ), 256))
    )