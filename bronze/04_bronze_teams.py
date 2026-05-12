"""
BRONZE — Teams (TeamDetails + TeamLists).

Replaces the M queries:
  - TeamDetails_2026Season-PlayerRegistration(AllCompetitions)_*  (Windows-1252)
  - TeamLists_2026Season-PlayerRegistration(AllCompetitions)_*    (UTF-8 / 65001)

Both are 2026-only sources (the source system started exporting them in
2026). If/when 2024 and 2025 backfills arrive, the same glob will pick
them up and silver will partition by season.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from config import (
    CATALOG, SCHEMA_BRONZE, CHECKPOINT_ROOT, SOURCE_VOLUME,
    CSV_OPTS_DEFAULT, CSV_OPTS_UTF8,
    PATTERN_TEAM_DETAILS, PATTERN_TEAM_LISTS,
)
from transformations import add_ingestion_metadata


def _read_teams(path_glob: str, schema_loc: str, csv_opts: dict) -> DataFrame:
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

    return reader.load(path_glob).transform(add_ingestion_metadata)


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_team_details",
    comment=(
        "Raw team registry: team name, category, active flag, headcount of "
        "officials and players. Source encoding: Windows-1252."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
def bronze_team_details() -> DataFrame:
    return _read_teams(
        PATTERN_TEAM_DETAILS,
        f"{CHECKPOINT_ROOT}/bronze_team_details/schema",
        CSV_OPTS_DEFAULT,
    )


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_team_lists",
    comment=(
        "Raw team rosters: every player and official with team, role, "
        "DOB and contact details. Source encoding: UTF-8."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
def bronze_team_lists() -> DataFrame:
    return _read_teams(
        PATTERN_TEAM_LISTS,
        f"{CHECKPOINT_ROOT}/bronze_team_lists/schema",
        CSV_OPTS_UTF8,
    )