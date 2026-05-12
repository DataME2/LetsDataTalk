"""
BRONZE — Registrations (one streaming table per season).

Replaces the M queries:
  - Registrations_2024Season_202603
  - Registrations_2025Season-Player
  - Registrations_2026Season-Player

Bronze responsibility: ingest the raw CSV exactly as dropped, with all columns
typed as STRING. Type coercion, null replacement and column drops are deferred
to silver — failure to parse one cell should not fail the whole batch.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (
    CATALOG, SCHEMA_BRONZE, CHECKPOINT_ROOT, SOURCE_VOLUME, CSV_OPTS_DEFAULT,
)
from transformations import add_ingestion_metadata


def _read_registrations_for_season(season: int) -> DataFrame:
    """Auto Loader stream over a season-specific filename glob."""
    path_glob = f"{SOURCE_VOLUME}/Registrations_{season}Season*.csv"
    schema_loc = f"{CHECKPOINT_ROOT}/bronze_registrations_{season}/schema"

    reader = (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "false")  # everything as STRING
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
    )
    for k, v in CSV_OPTS_DEFAULT.items():
        reader = reader.option(k, v)

    return (
        reader.load(path_glob)
            .transform(add_ingestion_metadata)
            .withColumn("_season", F.lit(season))
    )


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registrations_2024",
    comment=(
        "Raw 2024 season player registrations. Snapshot CSV ingested via "
        "Auto Loader; all source columns retained as STRING. Cleansing in silver."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dp.expect("source_file_present", "_source_file IS NOT NULL")
def bronze_registrations_2024() -> DataFrame:
    return _read_registrations_for_season(2024)


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registrations_2025",
    comment=(
        "Raw 2025 season player registrations. Snapshot CSV ingested via "
        "Auto Loader; all source columns retained as STRING. Cleansing in silver."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dp.expect("source_file_present", "_source_file IS NOT NULL")
def bronze_registrations_2025() -> DataFrame:
    return _read_registrations_for_season(2025)


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registrations_2026",
    comment=(
        "Raw 2026 season player registrations. Snapshot CSV ingested via "
        "Auto Loader; all source columns retained as STRING. Cleansing in silver."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dp.expect("source_file_present", "_source_file IS NOT NULL")
def bronze_registrations_2026() -> DataFrame:
    return _read_registrations_for_season(2026)