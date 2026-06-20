{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='landing'
) }}

{#-- Intraday SCADA — reads the existing Files/csv/scada_today/* files. append + file-level
     NOT IN filter (idempotent at file grain). ~288 small files land per day. --#}

{%- set read_cols = [
  'I','DISPATCH','UNIT_SCADA','xx','SETTLEMENTDATE','DUID','SCADAVALUE','LASTCHANGED'
] -%}

SELECT
  [DUID],
  TRY_CAST([SCADAVALUE] AS FLOAT) AS [INITIALMW],
  {{ parse_filename('src.filepath(1)') }} AS [file],
  TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6)) AS [SETTLEMENTDATE],
  TRY_CAST([LASTCHANGED] AS DATETIME2(6)) AS [LASTCHANGED],
  TRY_CAST([SETTLEMENTDATE] AS DATE) AS [DATE],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [YEAR]
FROM {{ openrowset_csv(get_csv_archive_path() ~ '/scada_today/*', read_cols) }} AS src
WHERE [I] = 'D' AND TRY_CAST([SCADAVALUE] AS FLOAT) <> 0
{%- if is_incremental() %}
  AND {{ parse_filename('src.filepath(1)') }} NOT IN (SELECT DISTINCT [file] FROM {{ this }})
{%- endif %}
