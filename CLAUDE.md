# Fabric Warehouse (dbt-fabric) — Lessons & Conventions

This repo is the **dbt-fabric / Fabric Data Warehouse** port of the duckrun (dbt-duckdb +
delta-rs) AEMO pipeline. Same models, same business logic — different execution engine.

## Architecture (what changed vs the duckrun repo)

- **dbt-fabric executes T-SQL in a Fabric Warehouse**; the analytics models materialize as
  warehouse tables (schemas `landing`, `mart`). dbt-fabric **cannot run Python (`dbt.python`)
  models** — only T-SQL.
- **The Python downloader is kept and runs as a standalone DuckDB step** (NOT a dbt model).
  `dbt/landing/stg_csv_archive_log.py` is the duckrun downloader, reused almost verbatim. It
  lives **outside** `model-paths: ["models"]` so dbt-fabric never tries to compile it. The
  notebook (`run_dwh`) and CI (`pipeline.yml` "Phase 1.5") invoke it by opening a plain
  `duckdb.connect()` as the `session` and passing a no-op `dbt` stub (its only dbt call is
  `dbt.config(...)`). DuckDB authenticates to OneLake via an `azure`-extension secret built
  from a storage access token. It downloads AEMO ZIPs, lands the CSVs **uncompressed** into
  `Files/csv_raw/{daily,scada_today,price_today,duid}/**`, and writes the watermark
  `Files/csv_raw_archive_log.parquet`. The T-SQL models then read these.
  - **Why uncompressed / why `csv_raw`** (verified against a live Warehouse): Fabric OPENROWSET
    **cannot read gzip CSV**. `DATA_COMPRESSION='GZIP'` is only valid under CSV `PARSER_VERSION='1.0'`,
    and 1.0 then can't tolerate the ragged/quoted AEMO rows (any `FIELDQUOTE` override to disable
    quoting breaks the gzip codec; without it the stray `"` rows fail). `PARSER_VERSION='2.0'`
    parses the ragged files fine but has no gzip support. So the only working combination is
    **plain CSV + PARSER 2.0**. This is why the dwh downloader writes plain `.CSV` (the duckrun
    repo gzips, since DuckDB reads gzip) — a deliberate divergence. The `csv_raw` folder + its own
    watermark keep this separate from any legacy gzip files (a `/*` glob would otherwise also match
    leftover `.CSV.gz` and choke).
- **`stg_csv_archive_log.sql`** (a T-SQL view, `schema=landing`) sits over the parquet the
  downloader writes, so every `ref('stg_csv_archive_log')` keeps working: the `.py` writes the
  log, the `.sql` view exposes it to the warehouse.
- **Models read those files in place via `OPENROWSET(BULK ...)`** (the user-chosen ingestion
  path). Fabric Warehouse reads the ragged multi-record AEMO CSVs fine: each fact model
  points `OPENROWSET` at the whole daily/intraday file and filters the record type in `WHERE`
  (`[I]='D' AND [UNIT]='DREGION'`, etc.), just like the DuckDB `read_csv(...) WHERE ...` did.
  See `macros/openrowset_csv.sql` + `macros/cast_floats.sql`.
- **Lakehouse + Warehouse**: the lakehouse is provisioned-if-missing/kept-if-present (it holds
  the Files); the Warehouse is the only new storage item. Both via `deploy.py`.
- **Semantic model** is Direct-Lake-on-OneLake repointed to the **Warehouse** item id (not the
  lakehouse) — `deploy.py` swaps the GUID in `model.bim`.

## T-SQL conversions (DuckDB → Fabric)

| DuckDB | Fabric T-SQL |
| --- | --- |
| `read_csv(path, columns={...}, all_varchar=1, ignore_errors=1)` | `OPENROWSET(BULK, FORMAT='CSV')` + `WITH ([col] VARCHAR(8000) <ordinal>)` + `TRY_CAST` |
| explicit new-file list via `getvariable()` pre-hook | folder wildcard + `<alias>.filepath(1)` and `WHERE parse_filename(filepath(1)) NOT IN (SELECT file FROM {{ this }})` |
| `incremental_strategy='safeappend'`, `partition_by` | `incremental_strategy='append'` (idempotent via the file-level NOT IN filter); **no partition_by** — Fabric has no table partitioning |
| duckrun `insert` | `append` + the cutoff watermark / file filter (no duckrun insert strategy exists) |
| `merge` (dim_duid) | `incremental_strategy='merge'` (Fabric supports T-SQL MERGE) |
| `TIMESTAMPTZ` | `DATETIME2` (AEMO times are local; Fabric has no `datetimeoffset`) |
| `strftime(ts,'%H%M')::INT` | `DATEPART(HOUR,ts)*100 + DATEPART(MINUTE,ts)` |
| `first(x)` + `GROUP BY ALL` | `MAX(x)` + explicit `GROUP BY` |
| `generate_series(d,d,INTERVAL 1 DAY)` | digit-tally CTE (`OPTION(MAXRECURSION)` can't live in dbt's subquery wrap) |
| `GREATEST(a,b)` | `SELECT MAX(v) FROM (SELECT a UNION ALL SELECT b) u` |
| `split_part(path,'/',-1)` etc. | `parse_filename` macro (`RIGHT/LEFT/CHARINDEX/REVERSE`) |
| `DOUBLE` | `FLOAT`; `DECIMAL(18,4)` unchanged |

- **Schemas must be created** before any model — Fabric does not auto-create them.
  `on-run-start: create_schemas_if_not_exists(['landing','mart'])`.
- **The OPENROWSET WITH lists columns by ordinal** for ONE record type; the file holds many
  record types of different widths + 'C'/'I' header rows. Fabric pads short rows to NULL and
  ignores extra fields; the `WHERE` keeps only the wanted rows.

## Auth

- **CI / local**: `authentication: CLI` (the workflow `az login`s via OIDC; `deploy.py` uses
  the Fabric CLI federated login). Set `FABRIC_DWH_SERVER` (warehouse `connectionString`) +
  `FABRIC_DWH_NAME` + `FABRIC_AUTH=CLI`.
- **Fabric notebook**: resolves the warehouse SQL endpoint via the Fabric REST API and passes
  a SQL-endpoint access token to dbt-fabric (`FABRIC_AUTH=ActiveDirectoryAccessToken`,
  `FABRIC_ACCESS_TOKEN`).

## ⚠️ Risks to validate in a real Fabric workspace (could not be tested offline)

1. ~~OPENROWSET reading gzip~~ **RESOLVED**: Fabric OPENROWSET can't read gzip CSV (see the
   uncompressed-`csv_raw` note in Architecture). Files are landed plain and read with PARSER 2.0;
   verified end-to-end against a live Warehouse (a 48 MB daily file → 147,456 DUNIT rows).
2. **dbt-fabric token auth inside a Fabric notebook** — confirm the exact `authentication`
   value / token audience the installed adapter version accepts (see `profiles.yml`).
3. **Direct-Lake-on-OneLake against a Warehouse** — confirm `model.bim`'s `AzureStorage.DataLake`
   URL repointed to the warehouse item id exposes `Tables/<schema>/<table>`.
4. **Reading all files each run** (folder wildcard + `filepath()` filter) is heavier than the
   duckrun explicit new-file list; correct, but watch runtime.

## Do NOT

- Turn `dbt/landing/stg_csv_archive_log.py` into a dbt-fabric model or move it under
  `models/` — dbt-fabric can't run Python; it must stay outside `model-paths` and be invoked
  via the standalone DuckDB step (notebook cell + CI "Phase 1.5"). It is kept close to the
  duckrun repo's version **except** it lands plain `.CSV` (not `.gz`) into `csv_raw/` — do not
  "resync" that back to gzip; Fabric OPENROWSET can't read gzip CSV.
- Add `partition_by` (Fabric has none) or `TIMESTAMPTZ` (use `DATETIME2`) **in the T-SQL
  models**. (The DuckDB downloader still uses `TIMESTAMPTZ` internally — that's fine, it's not
  warehouse SQL.)
- Use `NotebookEdit` on `notebook-content.ipynb` — keep cell `source` as an array of lines.
