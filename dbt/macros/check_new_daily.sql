{% macro check_new_daily() %}
  {#-- Run-operation the pipeline runner (notebook + CI) calls to decide whether to OVERWRITE
       fct_summary this run. "New daily" = daily files already in the archive log but NOT yet
       ingested into fct_scada — i.e. landing this run, checked BEFORE fct_scada builds.

       Signals through the run-operation's exit status (the runner branches on success):
         - quiet success  -> no new daily -> fct_summary appends intraday
         - raises / fails  -> new daily pending -> runner reruns fct_summary --full-refresh

       Reads the archive log straight from Files/csv_archive_log.parquet (OPENROWSET) and
       compares against the warehouse landing.fct_scada table, with an OBJECT_ID guard so it
       is safe on the very first run before fct_scada exists. --#}
  {%- if execute -%}
    {%- set log_path = get_root_path() ~ '/csv_raw_archive_log.parquet' -%}
    {#-- Probe table existence in a separate query first: a single CASE referencing
         landing.fct_scada binds BOTH branches at compile time, so it errors with "Invalid
         object name" on the very first (cold) run before fct_scada exists. --#}
    {%- set fct_scada_exists = run_query("SELECT OBJECT_ID('landing.fct_scada', 'U') AS oid").rows[0][0] is not none -%}
    {%- if fct_scada_exists -%}
      {%- set q -%}
        SELECT COUNT(*) AS n FROM OPENROWSET(BULK '{{ log_path }}', FORMAT = 'PARQUET') AS l
        WHERE l.source_type = 'daily'
          AND l.csv_filename NOT IN (SELECT DISTINCT [file] FROM landing.fct_scada)
      {%- endset -%}
    {%- else -%}
      {%- set q -%}
        SELECT COUNT(*) AS n FROM OPENROWSET(BULK '{{ log_path }}', FORMAT = 'PARQUET') AS l
        WHERE l.source_type = 'daily'
      {%- endset -%}
    {%- endif -%}
    {%- set n = run_query(q).rows[0][0] -%}
    {{ log("pipeline: new daily files pending = " ~ n, info=true) }}
    {%- if n and n > 0 -%}
      {{ exceptions.raise_compiler_error("NEW_DAILY_PENDING") }}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}
