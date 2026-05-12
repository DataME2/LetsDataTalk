"""
BRONZE — Registration Invoices (one streaming table per season).

Replaces the M queries:
  - RegistrationInvoices_2024Season
  - RegistrationInvoices_2025Season
  - RegistrationInvoices_2026Season

The source CSV is hierarchical: a parent row carries Surname/Reference/Date
plus the first line item, then 2..N child rows have NULLs for the header
columns and contain only their own line item data. Bronze keeps that shape
intact and as STRING; silver does the ghost-row removal, forward-fill, fee
categorisation and pivot.

Note: the source uses Windows-1252 encoding and embeds carriage returns
inside double-quoted cells (the line description format
`"<fee description>\\r<player name>"`). `multiLine=true` handles those.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (
    CATALOG, SCHEMA_BRONZE, CHECKPOINT_ROOT, SOURCE_VOLUME, CSV_OPTS_DEFAULT,
)
from transformations import add_ingestion_metadata


def _read_invoices_for_season(season: int) -> DataFrame:
    path_glob = f"{SOURCE_VOLUME}/RegistrationInvoices_{season}Season*.csv"
    schema_loc = f"{CHECKPOINT_ROOT}/bronze_invoices_{season}/schema"

    reader = (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "false")
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
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registration_invoices_2024",
    comment="Raw 2024 invoice line-items, hierarchical layout preserved.",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
def bronze_registration_invoices_2024() -> DataFrame:
    return _read_invoices_for_season(2024)


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registration_invoices_2025",
    comment="Raw 2025 invoice line-items, hierarchical layout preserved.",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
def bronze_registration_invoices_2025() -> DataFrame:
    return _read_invoices_for_season(2025)


@dp.table(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.bronze_registration_invoices_2026",
    comment="Raw 2026 invoice line-items, hierarchical layout preserved.",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.columnMapping.mode": "name",
    },
)
def bronze_registration_invoices_2026() -> DataFrame:
    return _read_invoices_for_season(2026)