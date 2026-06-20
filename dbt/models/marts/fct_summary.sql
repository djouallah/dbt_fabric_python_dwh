-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{#-- Overwrite-vs-append is decided by the RUNNER, not the model (full_refresh is fixed at
     parse time and can't be toggled from a run-time probe). The runner (notebook + CI) checks
     the check_new_daily run-operation and reruns this model with --full-refresh only when a
     new daily file landed (-> is_incremental() false -> full rebuild from daily; dbt-fabric
     drops+recreates the table). A plain run appends today's intraday (-> is_incremental()
     true). append (not merge): the incremental branch's cutoff watermark already excludes
     rows already in the table, so there is nothing to update. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='mart'
) }}

{% if is_incremental() %}

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

-- Full rebuild (runs under --full-refresh -> overwrite): authoritative daily + today's
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
