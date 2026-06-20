# dbt + Fabric Data Warehouse (via dbt-fabric)

The Fabric **Data Warehouse** port of the AEMO electricity pipeline. Transformations are
written as dbt models and executed as **T-SQL in a Fabric Warehouse** by
[`dbt-fabric`](https://github.com/microsoft/dbt-fabric), the **official Microsoft-maintained
adapter**. A **Python notebook** downloads the AEMO data and orchestrates the run. The warehouse
stores its tables as Delta in OneLake, so Power BI **Direct Lake** reads them directly.

> **Python notebook downloads + orchestrates · Fabric Warehouse executes T-SQL · OPENROWSET reads the lakehouse files.**

See [CLAUDE.md](CLAUDE.md) for engine details and lessons.

## How data flows

```
Python notebook (run) — downloads the AEMO reports + writes the archive log
        ▼
Lakehouse OneLake Files
  Files/csv_raw/daily/*      Files/csv_raw/scada_today/*   Files/csv_raw/price_today/*
  Files/csv_raw/duid/*.csv   Files/csv_raw_archive_log.parquet
        │
        │  dbt-fabric models read in place via OPENROWSET(BULK ...)
        ▼
Fabric Warehouse   (schemas: landing, mart)
  landing.fct_price / fct_scada / fct_price_today / fct_scada_today / stg_csv_archive_log
  mart.dim_calendar / dim_duid / fct_summary
        │
        │  Direct Lake
        ▼
Power BI semantic model
```

**Landing the files:** the **Python notebook** handles downloading and orchestration. A landing
step (`dbt/landing/stg_csv_archive_log.py`, invoked by the `run` notebook and by CI before
the dbt run) downloads the AEMO ZIPs and lands the CSVs **uncompressed** into `Files/csv_raw/**`,
alongside the `csv_raw_archive_log.parquet` watermark. The T-SQL models then read those files via
`OPENROWSET`. The deploy provisions a Warehouse (and the lakehouse if missing) and materializes
the models there.


## dbt-fabric configuration

`dbt/profiles.yml` points the `fabric` adapter at the Warehouse SQL endpoint:

```yaml
aemo_electricity:
  target: dev
  outputs:
    dev:
      type: fabric
      driver: "ODBC Driver 18 for SQL Server"
      server:   "{{ env_var('FABRIC_DWH_SERVER') }}"   # warehouse connectionString
      database: "{{ env_var('FABRIC_DWH_NAME') }}"
      schema:   "{{ env_var('DBT_SCHEMA', 'mart') }}"
      authentication: "{{ env_var('FABRIC_AUTH', 'CLI') }}"
```

- **CI / laptop**: `authentication: CLI` (uses the logged-in `az` identity).
- **Fabric notebook**: an access token for the SQL endpoint is passed via
  `FABRIC_ACCESS_TOKEN` (`authentication: ActiveDirectoryAccessToken`).

### Reading files with OPENROWSET

Each fact model reads the raw AEMO report directly and filters the record type in SQL — e.g.
`fct_price`:

```sql
FROM {{ openrowset_csv(get_csv_archive_path() ~ '/daily/*', read_cols) }} AS src
WHERE [I] = 'D' AND [UNIT] = 'DREGION' AND [VERSION] = '3'
```

The shared `openrowset_csv` macro builds the `OPENROWSET(BULK ... FORMAT='CSV') WITH (...)`
ordinal column list; `cast_floats` does the `TRY_CAST(... AS FLOAT)` per column.

### Incremental strategies

| Strategy | Behavior | Used by |
|----------|----------|---------|
| `append` (+ file-level `NOT IN {{ this }}`) | Insert only files not yet loaded — idempotent at file grain | `fct_price`, `fct_scada`, `*_today` |
| `append` + cutoff watermark / `--full-refresh` | Append intraday; full overwrite when a new daily lands (runner-driven) | `fct_summary` |
| `merge` | Upsert on `DUID` | `dim_duid` |
| `append` (one-off) | Built once, then `WHERE 1=0` no-op | `dim_calendar` |

## Schema layout

- **`landing`** — staging + fact tables (`fct_*`, `stg_csv_archive_log`)
- **`mart`** — Power BI-facing models (`dim_duid`, `dim_calendar`, `fct_summary`)

## Run it

### Manual deploy

```bash
az login
python deploy.py --env main
```

Needs the [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) and the
[Microsoft Fabric CLI](https://microsoft.github.io/fabric-cli/) (`pip install ms-fabric-cli`).
`deploy.py` provisions the lakehouse (if missing) + Warehouse (if missing), copies the dbt
project to OneLake, deploys the notebook / pipeline / semantic model, and refreshes the model.

### Run dbt directly

```bash
pip install dbt-fabric          # plus the msodbcsql18 ODBC driver
export FABRIC_DWH_SERVER=<warehouse connectionString>
export FABRIC_DWH_NAME=<warehouse name>
export FILES_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh_id>/Files
export FABRIC_AUTH=CLI
dbt run  --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt
```

### CI/CD

`.github/workflows/pipeline.yml` runs the full flow on `dbt-fabric`: OIDC Azure
login (secrets `AZURE_CLIENT_ID` / `AZURE_TENANT_ID`), provision lakehouse + warehouse, land
the files via the Python downloader step, run the `check_new_daily → build → fct_summary →
test` sequence, then `deploy.py`. Triggers on push to any branch; the service principal has a
federated credential for `main`.

> **Granting the CI identity workspace access — pick the right object.** The deploy service
> principal must be added to the target workspace (Member/Admin). When you search for it in the
> workspace **Manage access** picker you may see **two entries with the same name**: one is the
> security **group** ("just a name"), the other is the **service principal / app** (shows an
> **App ID**). Add the one **with the App ID** matching `AZURE_CLIENT_ID` — that is the identity
> the GitHub OIDC login actually authenticates as. Granting the same-named *group* does nothing
> unless the SP is a member of it. Symptom of getting this wrong: Phase 1 fails with
> `The Workspace 'null.Workspace' could not be found` (the SP can't resolve the workspace name).

---

See **[CLAUDE.md](CLAUDE.md)** for the T-SQL conventions and notes from validating the pipeline
in a real Fabric workspace (OPENROWSET over the lakehouse files is the linchpin).
