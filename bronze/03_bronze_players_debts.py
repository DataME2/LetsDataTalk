"""
BRONZE — Players Debts (one streaming table per season).

Replaces the M queries:
  - PlayersDebts_2024Season   (UTF-8 / 65001 in M code)
  - PlayersDebts_2025Season   (Windows-1252)
  - PlayersDebts_2026Season   (Windows-1252)

Encoding differs across years per the original M code, so each season is
read with its own options. All columns kept as STRING for safe ingestion;
typed cleansing happens at silver.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (
    CATALOG, SCHEMA_BRONZE, CHECKPOINT_ROOT, SOURCE_VOLUME,
    CSV_OPTS_DEFAULT, CSV_OPTS_UTF8,
)
from transformations import add_ingestion_metadata


def _read_debts_for_season(season: int, csv_opts: dict) -> DataFrame:
    path_glob = f"{SOURCE_VOLUME}/PlayersDebts_{season}Season*.csv"
    schema_loc = f"{CHECKPOINT_ROOT}/bronze_players_debts_{season}/schema"

    reader = (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
    )
    for k, v in csv_opts.items():
        reader = reader.option(k, v)

    return (
        reader.load(path_glob)
            .transform(add_ingestion_metadata)
            .withColumn("_season", F.lit(season))
    )


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_players_debts_2024",
    comment="Raw 2024 invoice/debt status per player. Source: UTF-8.",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
def bronze_players_debts_2024() -> DataFrame:
    # M code declared Encoding=65001 (UTF-8) for the 2024 file specifically.
    return _read_debts_for_season(2024, CSV_OPTS_UTF8)


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_players_debts_2025",
    comment="Raw 2025 invoice/debt status per player. Source: Windows-1252.",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
def bronze_players_debts_2025() -> DataFrame:
    return _read_debts_for_season(2025, CSV_OPTS_DEFAULT)


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_players_debts_2026",
    comment="Raw 2026 invoice/debt status per player. Source: Windows-1252.",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
def bronze_players_debts_2026() -> DataFrame:
    return _read_debts_for_season(2026, CSV_OPTS_DEFAULT)