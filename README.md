# North Star FC — Lakeflow Spark Declarative Pipeline

A Databricks Lakeflow / Spark Declarative Pipelines (SDP) implementation of the
Power BI / Power Query (M-code) data preparation logic from `ns_data_transform.pbix`.
Implements the medallion architecture (bronze → silver → gold) over Unity Catalog,
sourcing CSV snapshots from `/Volumes/north_star_fc/nsfc/ext_files_ns`.

---

## 1. Architecture at a glance

```
┌────────────────────────┐
│  /Volumes/.../ext_files_ns  ← CSV snapshots dropped by upstream system
└──────────┬─────────────┘
           │  (Auto Loader, cloudFiles, streaming)
           ▼
┌────────────────────────┐
│   north_star_fc.bronze │  Raw CSV, all-string types, full provenance
│     11 streaming       │   (one bronze table per file pattern)
│     tables             │
└──────────┬─────────────┘
           │  (typed cleansing, M-code logic, latest-snapshot dedup)
           ▼
┌────────────────────────┐
│   north_star_fc.silver │  Cleansed, conformed, season-unioned MVs
│     ~13 materialized   │   (per-season + per-entity unified tables)
│     views              │
└──────────┬─────────────┘
           │  (dimensional modelling, KPIs, joins)
           ▼
┌────────────────────────┐
│   north_star_fc.gold   │  Star-schema dimensions + facts + summary
│     6 materialized     │
│     views              │
└────────────────────────┘
```

### Bronze (`north_star_fc.bronze`)
| Streaming table | Source files |
|---|---|
| `bronze_registrations_2024` | `Registrations_2024Season*.csv` |
| `bronze_registrations_2025` | `Registrations_2025Season*.csv` |
| `bronze_registrations_2026` | `Registrations_2026Season*.csv` |
| `bronze_registration_invoices_2024` | `RegistrationInvoices_2024Season*.csv` |
| `bronze_registration_invoices_2025` | `RegistrationInvoices_2025Season*.csv` |
| `bronze_registration_invoices_2026` | `RegistrationInvoices_2026Season*.csv` |
| `bronze_players_debts_2024` | `PlayersDebts_2024Season*.csv` (UTF-8) |
| `bronze_players_debts_2025` | `PlayersDebts_2025Season*.csv` |
| `bronze_players_debts_2026` | `PlayersDebts_2026Season*.csv` |
| `bronze_team_details` | `TeamDetails_*.csv` |
| `bronze_team_lists` | `TeamLists_*.csv` (UTF-8) |

All bronze tables ingest with `cloudFiles.inferColumnTypes=false` — every column
arrives as STRING. Typing happens at silver via `try_cast`, mirroring M's
`Table.ReplaceErrorValues` semantics. Two metadata columns are added:
`_source_file` and `_ingest_ts`.

### Silver (`north_star_fc.silver`)
| Materialized view | Purpose |
|---|---|
| `silver_registrations_{2024,2025,2026}` | Per-season cleansed registrations |
| `silver_registrations` | All seasons unioned, conformed schema |
| `silver_players` | **NEW** — player-grain (5 player blocks → long format) |
| `silver_invoices_{2024,2025,2026}` | Per-season pivoted invoice fees |
| `silver_invoices` | All seasons unioned |
| `silver_players_debts_{2024,2025,2026}` | Per-season debts with CollectionRate |
| `silver_players_debts` | All seasons unioned |
| `silver_team_details` | Cleansed team registry (typed counts, boolean flag) |
| `silver_team_lists` | Cleansed team rosters |

### Gold (`north_star_fc.gold`)
| Materialized view | Grain |
|---|---|
| `dim_player` | One row per unique player (FFA-number-keyed, SCD-1) |
| `dim_team` | One row per team (combines registry + roster counts) |
| `fact_registrations` | One row per (player, season), with age band + renewal flag |
| `fact_invoices` | One row per invoice, with payment_status + days_since_invoice |
| `fact_collections` | One row per (player, season), aggregated payment KPIs |
| `summary_season` | One row per season — executive dashboard KPIs |

---

## 2. M-code → PySpark mapping

| Power Query M idiom | PySpark equivalent (in `transformations.py`) |
|---|---|
| `Csv.Document(File.Contents(path), ...)` | `spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")...load(glob)` |
| `Table.PromoteHeaders(..., [PromoteAllScalars=true])` | `.option("header", "true")` on the CSV reader |
| `Table.TransformColumnTypes(...)` | `safe_cast(col, type, default)` — uses `try_cast` to avoid throwing |
| `Table.ReplaceErrorValues(..., {col, default})` | Wrapped into `safe_cast` (coalesce after try_cast) |
| `Table.ReplaceValue(..., null, "X", ...)` | `replace_nulls(df, mapping)` |
| `Table.ReplaceValue(..., "/", "X", ReplaceText, ...)` | `F.when(F.col(c) == "/", ...).otherwise(...)` |
| `Table.RemoveColumns(..., {c1, c2})` | `df.drop(c1, c2)` |
| `Table.SelectColumns(..., {...})` | `df.select(...)` |
| `Table.ReorderColumns(...)` | `df.select(...)` (Spark doesn't care about order, but we keep it stable in gold) |
| `Table.Distinct(..., {key})` | `df.dropDuplicates([key])` |
| `Table.SelectRows(..., each [Reference] <> null)` | `df.filter(F.col("Reference").isNotNull())` |
| `Table.AddColumn(..., "X", each ...)` | `df.withColumn("X", expr)` |
| `Text.Split(..., "#(cr)")` | `F.split(col, "\\r")` |
| `Text.Lower(...)` | `F.lower(...)` |
| `Text.Contains(...)` | `col.contains(kw)` |
| `Text.Clean(...)` (strips control chars) | `F.regexp_replace(col, r"[\x00-\x1F]", "")` (in `text_clean`) |
| `Table.FillDown(..., {cols})` | Window with `F.last(c, ignorenulls=True).over(...)` after building an invoice-group id (in `forward_fill_invoice_headers`) |
| `Table.Group(..., {keys}, {{name, each List.Sum(...)}})` | `df.groupBy(...).pivot("FeeCategory", [...]).agg(F.sum(...))` |
| Column rename via `Table.RenameColumns` | `df.withColumnRenamed(old, new)` |

### Things the M code did that we changed deliberately

1. **`Player4FFANumber` cast to `date` in 2026 M code** — almost certainly a bug.
   We cast to `long` like every other FFA number column.
2. **CollectionRate computed only for 2026 in M** — we extend to all seasons.
3. **No latest-snapshot dedup in M** — M loaded a single CSV. We accept many
   snapshots and pick the latest per business key.
4. **Players stayed wide (Player1..Player5 columns) in M** — we add a `silver_players`
   table that explodes the 5 player blocks into rows for sane downstream analytics.
5. **Year-suffixed `Player{N}{PrevYear}ClubName` columns in M** — we rename to
   the stable `Player{N}PreviousClubName` so all seasons share a schema.

---

## 3. Project structure

```
nsfc_pipeline/
├── README.md                        ← you are here
├── config.py                        ← paths, schemas, M-derived constants
├── transformations.py               ← reusable PySpark helpers (M-code equivalents)
├── bronze/
│   ├── 01_bronze_registrations.py   ← 3 streaming tables, one per season
│   ├── 02_bronze_invoices.py        ← 3 streaming tables, one per season
│   ├── 03_bronze_players_debts.py   ← 3 streaming tables (mixed encoding)
│   └── 04_bronze_teams.py           ← team_details (1252) + team_lists (UTF-8)
├── silver/
│   ├── 01_silver_registrations.py   ← per-season + unified
│   ├── 02_silver_players.py         ← exploded player-grain
│   ├── 03_silver_invoices.py        ← forward-fill + pivot + per-season + unified
│   ├── 04_silver_players_debts.py   ← per-season + unified, CollectionRate
│   └── 05_silver_teams.py           ← cleansed team_details + team_lists
└── gold/
    ├── 01_gold_dim_player.py        ← SCD-1 player dimension
    ├── 02_gold_dim_team.py          ← team dimension w/ roster counts
    ├── 03_gold_fact_registrations.py ← player-season grain registration fact
    ├── 04_gold_fact_invoices.py     ← invoice-grain fact + payment_status
    ├── 05_gold_fact_collections.py  ← player-season collections fact
    └── 06_gold_summary_season.py    ← executive season-level KPI summary
```

The numeric prefixes (`01_`, `02_`, ...) are visual hints for humans — the SDP
runtime resolves dataset dependencies from the `dp.read*()` calls inside each
function, not from filenames or import order.

---

## 4. Deployment

### 4.1 Create the catalog and schemas

Run once before the first pipeline execution:

```sql
CREATE CATALOG IF NOT EXISTS north_star_fc;
CREATE SCHEMA  IF NOT EXISTS north_star_fc.bronze;
CREATE SCHEMA  IF NOT EXISTS north_star_fc.silver;
CREATE SCHEMA  IF NOT EXISTS north_star_fc.gold;

CREATE VOLUME  IF NOT EXISTS north_star_fc.nsfc.ext_files_ns;
-- then upload CSVs to /Volumes/north_star_fc/nsfc/ext_files_ns
```

### 4.2 Create the pipeline

In Databricks:

1. **Pipelines → Create pipeline → Lakeflow Spark Declarative Pipelines**
2. **Source code paths**: point at the three folders separately:
   - `nsfc_pipeline/bronze`
   - `nsfc_pipeline/silver`
   - `nsfc_pipeline/gold`
   (Plus the root for `config.py` and `transformations.py` to be importable.)
3. **Default catalog**: `north_star_fc`
4. **Default schema**: `bronze` (gets overridden per-table by the `name=` kwarg)
5. **Channel**: `current` (or `preview` to use the very newest SDP features)
6. **Pipeline mode**: `Triggered` for batch refreshes, `Continuous` for streaming bronze

### 4.3 Validation queries

After the first successful run, sanity-check with:

```sql
-- Are all snapshots making it to bronze?
SELECT _source_file, COUNT(*) AS rows, MAX(_ingest_ts) AS ingested
FROM north_star_fc.bronze.bronze_registration_invoices_2026
GROUP BY _source_file ORDER BY ingested DESC;

-- Are silver invoices balancing? (sum of fees ≈ invoice amount)
SELECT season,
       ROUND(AVG(ABS(invoice_amount - fee_total_check)), 2) AS avg_drift
FROM north_star_fc.gold.fact_invoices
GROUP BY season ORDER BY season;

-- Season-over-season summary
SELECT * FROM north_star_fc.gold.summary_season ORDER BY season;
```

---

## 5. Data quality expectations

Each silver/gold table declares `@dp.expect` rules that are tracked in the
pipeline's event log. The current set:

| Table | Rule | Action |
|---|---|---|
| `bronze_registrations_*` | `_source_file IS NOT NULL` | warn |
| `silver_registrations` | `RegistrationDate IS NOT NULL` | drop |
| `silver_registrations` | `PrimaryMemberSurname IS NOT NULL` | warn |
| `silver_players` | `PlayerSurname IS NOT NULL AND PlayerSurname <> ''` | drop |
| `silver_invoices` | `invoice_ref IS NOT NULL` | warn |
| `silver_invoices` | `invoice_amount >= 0` | warn |
| `silver_invoices` | `invoice_date IS NOT NULL` | drop |
| `silver_players_debts` | `InvoiceRef IS NOT NULL` | warn |
| `silver_players_debts` | `CollectionRate BETWEEN 0 AND 1.5` | warn |
| `silver_team_details` | `TeamName IS NOT NULL AND TeamName <> ''` | drop |

Inspect violations after a run:

```sql
SELECT *
FROM event_log(TABLE(north_star_fc.bronze.bronze_registrations_2026))
WHERE event_type = 'flow_progress'
  AND details:flow_progress.metrics IS NOT NULL;
```

---

## 6. Operational notes

- **Encodings**: PlayersDebts 2024 + TeamLists are UTF-8; everything else is
  Windows-1252, exactly as declared in the original M code.
- **Snapshot semantics**: same season can have many CSV drops. Bronze accepts
  all of them; silver picks the latest snapshot per business key (`InvoiceRef`,
  `Reference`, `(TeamName, FFA Number, Role)`).
- **Reset behaviour**: bronze tables have `pipelines.reset.allowed=true` so you
  can fully reload from source if Auto Loader's state ever drifts. Silver/gold
  are recomputed from bronze automatically on each run.
- **Cost**: silver invoices uses a window function over `_source_file` (per-file
  ordering) for forward-fill — fine for the data volumes here (<10k invoices
  per season). At 100x scale, replace `monotonically_increasing_id()` with a
  pre-sorted ingestion via `cloudFiles.useStrictGlobber=true` and explicit
  `_metadata.file_modification_time` ordering.
