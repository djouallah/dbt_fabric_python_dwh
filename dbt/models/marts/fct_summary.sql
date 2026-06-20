-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{#-- Rebuild-vs-append is decided by the RUNNER, not the model. The runner (notebook + CI) checks
     the check_new_daily run-operation and, when a new daily file landed, reruns this model with
     `--vars 'rebuild_summary: true'`. We deliberately do NOT use --full-refresh: on dbt-fabric
     that DROPs + recreates the table (a Sch-M DDL swap that deadlocks Fabric's background
     stats/clustering maintenance, loses grants, and rebinds Direct Lake every run).
       - rebuild (new daily): incremental_strategy='delete+insert' on unique_key
         [date,time,DUID]. The full-rebuild branch emits the complete history, so the keyed
         DELETE removes every existing row and the INSERT repopulates it (a native delete+insert,
         no DROP, no hook). The table object, CLUSTER BY definition and grants are preserved.
       - plain run: incremental_strategy='append' adds today's intraday. The cutoff watermark
         already excludes rows already in the table, so there is nothing to update/delete. --#}
{{ config(
    materialized='incremental',
    incremental_strategy=('delete+insert' if var('rebuild_summary', false) else 'append'),
    unique_key=['date', 'time', 'DUID'],
    schema='mart',
    cluster_by=['date', 'DUID']
) }}
{#-- cluster_by (samdebruyn): emits CREATE TABLE ... WITH (CLUSTER BY ([date],[DUID])) on the very
     first build, so the summary is physically clustered for the common filters/joins (date slicing
     + per-DUID). It's a table-definition property, so the daily delete+insert rebuild keeps it
     (the table is never dropped); Fabric maintains the clustering automatically thereafter. --#}

{%- set rebuild = var('rebuild_summary', false) -%}

{% if is_incremental() and not rebuild %}

-- Append intraday: today's rows after the last cutoff baked into the table.
WITH max_cutoff AS (
  SELECT MAX(cutoff) AS cutoff FROM {{ this }}
),

incremental_data AS (
  SELECT
    s.[DATE] AS [date],
    s.SETTLEMENTDATE,
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  CROSS JOIN max_cutoff mc
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > mc.cutoff
  GROUP BY s.[DATE], s.SETTLEMENTDATE, s.DUID
)

SELECT
  [date],
  DATEPART(HOUR, SETTLEMENTDATE) * 100 + DATEPART(MINUTE, SETTLEMENTDATE) AS [time],
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  CAST(MAX(SETTLEMENTDATE) OVER () AS DATETIME2(6)) AS cutoff
FROM incremental_data

{% else %}

-- Full rebuild (runs when rebuild_summary=true via delete+insert, or on first build): authoritative daily + today's
-- intraday after the daily cutoff. The cutoff column is the watermark the append path reads.
WITH scada_cutoff AS (
  SELECT MAX(SETTLEMENTDATE) AS c FROM {{ ref('fct_scada') }}
),
cutoff_calc AS (
  -- T-SQL has no GREATEST: max of (daily max, intraday max) via UNION ALL.
  SELECT MAX(v) AS cutoff FROM (
    SELECT MAX(SETTLEMENTDATE) AS v FROM {{ ref('fct_scada') }}
    UNION ALL
    SELECT COALESCE(MAX(SETTLEMENTDATE), CAST('1900-01-01' AS DATETIME2(6))) FROM {{ ref('fct_scada_today') }}
  ) u
),
daily_summary AS (
  SELECT
    s.[DATE] AS [date],
    DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE) AS [time],
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
  GROUP BY s.[DATE], DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE), s.DUID

  UNION ALL

  SELECT
    s.[DATE] AS [date],
    DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE) AS [time],
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > (SELECT c FROM scada_cutoff)
  GROUP BY s.[DATE], DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE), s.DUID
)

SELECT
  [date],
  [time],
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  (SELECT cutoff FROM cutoff_calc) AS cutoff
FROM daily_summary

{% endif %}
