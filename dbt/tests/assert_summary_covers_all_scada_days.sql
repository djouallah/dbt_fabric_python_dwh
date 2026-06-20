-- Summary should have at least as many distinct days as scada.
SELECT
  scada_days,
  summary_days
FROM (
  SELECT
    (SELECT COUNT(DISTINCT [DATE]) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) AS scada_days,
    (SELECT COUNT(DISTINCT [date]) FROM {{ ref('fct_summary') }}) AS summary_days
) t
WHERE scada_days > summary_days
