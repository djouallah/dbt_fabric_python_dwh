{#-- T-SQL port of DuckDB split_part(split_part(path,'/',-1),'.',1):
     take the last path segment, then everything before the first '.'.
     OPENROWSET's filepath(1) yields the full OneLake path of the source file. --#}
{% macro parse_filename(filepath) %}
  {%- set fn -%}RIGHT({{ filepath }}, CHARINDEX('/', REVERSE({{ filepath }}) + '/') - 1){%- endset -%}
  {#-- filepath(1) is NVARCHAR (unsupported as a Fabric Warehouse column type); the AEMO
       filename stem is short, so cast the result to VARCHAR for a storable, comparable key. --#}
  CAST(LEFT({{ fn }}, CHARINDEX('.', {{ fn }} + '.') - 1) AS VARCHAR(256))
{% endmacro %}
