-- Test: fct_summary should not have NULL prices (indicates broken SCADA-to-price join via dim_duid region)
-- Returns rows where price is NULL, which means the DUID's region didn't match any REGIONID in price data

SELECT TOP 10
  [date],
  DUID,
  mw
FROM {{ ref('fct_summary') }}
WHERE price IS NULL
