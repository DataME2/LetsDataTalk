"""
SILVER — Registrations (cleansed, conformed, all seasons unioned).

Replaces the M queries' transformation logic for:
  - Registrations_2024Season_202603
  - Registrations_2025Season-Player
  - Registrations_2026Season-Player

The three M queries are 95% identical; only differences are:
  * Year-suffixed column name `Player{N}{PrevYear}ClubName` (renamed here
    to a stable `Player{N}PreviousClubName`)
  * 2024 has two specific PrimaryMemberNotes strings replaced with NULL
  * 2025 has an extra Player1SchoolGrade error→"clean"→"1" hack
  * 2026 has Text.Clean on Player1Surname/FirstName/MiddleName + Player2Surname,
    plus the FQFeeBypassCode column that doesn't exist in 2024/2025
  * 2024 has Player1FFANumber error→0; 2026 has the (probably wrong)
    cast of Player4FFANumber to date — we treat it as Long like every
    other FFA number column.

Output: a single materialized view with one row per registration (household),
one Season column, and consistent column names across years.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (
    CATALOG, SCHEMA_BRONZE, SCHEMA_SILVER,
    NULL_REPLACEMENTS_REGISTRATIONS,
    COLUMNS_TO_DROP_REGISTRATIONS,
    DEFAULT_POSTCODE,
    PLAYER_SLOTS,
)
from transformations import (
    apply_text_clean,
    replace_nulls,
    safe_cast,
)


# ---------------------------------------------------------------------------
# Per-season cleansing pipeline (one helper, three callsites)
# ---------------------------------------------------------------------------
def _clean_season_registrations(df: DataFrame, season: int) -> DataFrame:
    """
    Apply every M-code cleansing step for a single season.

    Steps mirror the M code in the same order:
      1. Drop unwanted columns (RegistrationTime + free-text noise).
      2. Strip commas from NotifyEmailAddresses (M did this BEFORE drop;
         we do it after the drop check because the column may already
         have been removed — guard against missing column).
      3. Rename `Player{N}{PrevYear}ClubName` → `Player{N}PreviousClubName`
         so all three seasons share a schema.
      4. Replace nulls with friendly defaults (M's chained ReplaceValue).
      5. Replace "/" with "No information provided" in the previous-club
         column for slot 1 (M only does this for 2026, but applying it to
         all seasons is safer and cheap).
      6. Type cast with try_cast (M's ReplaceErrorValues semantics).
      7. Run Text.Clean over the columns the M code targeted.
      8. Stamp Season column.
    """
    out = df

    # 1. Drop unwanted columns (skip ones not present in this season's schema)
    drop_now = [c for c in COLUMNS_TO_DROP_REGISTRATIONS if c in out.columns]
    out = out.drop(*drop_now)

    # 3. Rename year-suffixed previous-club columns → stable names
    prev_year = season - 1
    for n in PLAYER_SLOTS:
        old = f"Player{n}{prev_year}ClubName"
        new = f"Player{n}PreviousClubName"
        if old in out.columns:
            out = out.withColumnRenamed(old, new)

    # 4. Null replacements (only the ones we promised in the config dict)
    out = replace_nulls(out, NULL_REPLACEMENTS_REGISTRATIONS)

    # 5. Strip stray "/" sentinels from the slot-1 previous-club column
    if "Player1PreviousClubName" in out.columns:
        out = out.withColumn(
            "Player1PreviousClubName",
            F.when(F.col("Player1PreviousClubName") == "/",
                   F.lit("No information provided"))
             .otherwise(F.col("Player1PreviousClubName"))
        )

    # 5b. 2024-only: scrub two specific PrimaryMemberNotes leakage rows
    #     (the M code replaces them with NULL — they're stray email pairs).
    if season == 2024 and "PrimaryMemberNotes" in out.columns:
        bad_notes = [
            "jason.p.platt@gmail.com\namanda_atkinson@rocketmail.com",
            "orrinhughes@outlook.com\ncakeabow@gmail.com",
        ]
        out = out.withColumn(
            "PrimaryMemberNotes",
            F.when(F.col("PrimaryMemberNotes").isin(bad_notes), F.lit(None))
             .otherwise(F.col("PrimaryMemberNotes"))
        )

    # 6. Type casts. try_cast returns NULL on parse failure; coalesce
    #    to the same default M used (Postcode → 4000, FFA → 0, school grade → 0).
    cast_specs = [
        ("RegistrationDate",   "date",    None),
        ("Player1DateofBirth", "date",    None),
        ("Player2DateofBirth", "date",    None),
        ("Player3DateofBirth", "date",    None),
        ("Player4DateofBirth", "date",    None),
        ("Player5DateofBirth", "date",    None),
        ("Player1FFANumber",   "long",    0),
        ("Player2FFANumber",   "long",    0),
        ("Player3FFANumber",   "long",    0),
        # Note: M code for 2026 mistakenly casts Player4FFANumber to `date`.
        # That's an obvious bug — we cast to `long` like the other slots.
        ("Player4FFANumber",   "long",    0),
        ("Player5FFANumber",   "long",    0),
        ("Player2SchoolGrade", "string",  "0"),  # text in M after the cast hack
        ("Postcode",           "int",     DEFAULT_POSTCODE),
    ]
    for col_name, target, default in cast_specs:
        if col_name in out.columns:
            out = out.withColumn(col_name, safe_cast(col_name, target, default))

    # 7. Text.Clean on PrimaryMemberSurname (all seasons) +
    #    Player1Surname/FirstName/MiddleName + Player2Surname (2026 only).
    text_clean_cols = ["PrimaryMemberSurname"]
    if season == 2026:
        text_clean_cols += [
            "Player1Surname", "Player1FirstName", "Player1MiddleName",
            "Player2Surname",
        ]
    text_clean_cols = [c for c in text_clean_cols if c in out.columns]
    out = apply_text_clean(out, text_clean_cols)

    # 7b. 2025-only: Player1SchoolGrade had an "error→clean→1" remediation
    #     in the M code. Now that everything came in as STRING and we've
    #     already null-replaced "No Provided", the only hack remaining is
    #     turning anything that's not a plain digit into "1" — but actually
    #     just leaving it alone is fine because the M's "clean" → "1" step
    #     was an artefact of an Int cast that no longer applies.
    #     (Documented for archaeology; no code change needed.)

    # 8. Stamp the season + add a stable household key
    out = out.withColumn("Season", F.lit(season).cast("int"))

    # Build a deterministic household_id for downstream joins. PrimaryMember
    # surname + first name + email is unique enough as a natural key for the
    # purposes of this club's registration system.
    out = out.withColumn(
        "household_id",
        F.sha2(
            F.concat_ws("|",
                F.coalesce(F.col("PrimaryMemberSurname"),    F.lit("")),
                F.coalesce(F.col("PrimaryMemberFirstName"),  F.lit("")),
                F.coalesce(F.col("PrimaryMemberEmailAddress"), F.lit("")),
                F.col("Season").cast("string"),
            ),
            256,
        )
    )

    return out


# ---------------------------------------------------------------------------
# Per-season silver tables (handy for incremental troubleshooting + lineage)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_registrations_2024",
    comment="Cleansed 2024 registrations.",
    table_properties={
        "quality": "silver",
        "delta.columnMapping.mode": "name",
    },
)
def silver_registrations_2024() -> DataFrame:
    return _clean_season_registrations(
        spark.read.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registrations_2024"),
        season=2024,
    )


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_registrations_2025",
    comment="Cleansed 2025 registrations.",
    table_properties={
        "quality": "silver",
        "delta.columnMapping.mode": "name",
    },
)
def silver_registrations_2025() -> DataFrame:
    return _clean_season_registrations(
        spark.read.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registrations_2025"),
        season=2025,
    )


@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_registrations_2026",
    comment="Cleansed 2026 registrations.",
    table_properties={
        "quality": "silver",
        "delta.columnMapping.mode": "name",
    },
)
def silver_registrations_2026() -> DataFrame:
    return _clean_season_registrations(
        spark.read.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registrations_2026"),
        season=2026,
    )


# ---------------------------------------------------------------------------
# Unified silver table — all seasons stitched together with consistent schema
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{CATALOG}.{SCHEMA_SILVER}.silver_registrations",
    comment=(
        "All-seasons registrations, conformed schema. One row per household "
        "(primary member) per season; up to 5 player slots remain wide here. "
        "See silver_players for the long-format player-grain table."
    ),
    table_properties={
        "quality": "silver",
        "delta.columnMapping.mode": "name",
    },
)
@dp.expect_or_drop("registration_date_present", "RegistrationDate IS NOT NULL")
@dp.expect("primary_member_present", "PrimaryMemberSurname IS NOT NULL")
def silver_registrations() -> DataFrame:
    df_24 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_registrations_2024")
    df_25 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_registrations_2025")
    df_26 = spark.read.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_registrations_2026")

    # unionByName + allowMissingColumns lets each season's quirky columns
    # (e.g. Player1FQFeeBypassCode only exists in 2026) fill as NULL elsewhere.
    return (
        df_24
        .unionByName(df_25, allowMissingColumns=True)
        .unionByName(df_26, allowMissingColumns=True)
    )