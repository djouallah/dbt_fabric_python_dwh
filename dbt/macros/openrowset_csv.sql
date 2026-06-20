{#-- Builds an OPENROWSET(BULK ...) over a OneLake Files folder wildcard, typing every
     column as VARCHAR by ordinal position so the model can TRY_CAST individually (the
     T-SQL stand-in for DuckDB's read_csv(all_varchar=1, ignore_errors=1)).

     `columns` is the ordered list of source column names (position 1..N) for ONE AEMO
     record type. The landed files are the raw AEMO reports (one file holds many record
     types of differing widths, plus 'C'/'I' comment+header rows) exactly as the existing
     file-landing pipeline wrote them — nothing is split or padded. Fabric Warehouse reads
     these ragged rows fine: rows shorter than N pad to NULL, extra fields are ignored, and
     the model's WHERE on [I]/[UNIT]/record-type columns keeps only the rows of interest.

     FIRSTROW = 1 (the 'C'/'I' rows are filtered out by the WHERE, not skipped here). Alias
     the result and call <alias>.filepath(1) in the SELECT to recover the source file path. --#}
{% macro openrowset_csv(path_glob, columns) %}
OPENROWSET(
    BULK '{{ path_glob }}',
    FORMAT = 'CSV',
    {#-- PARSER 2.0 handles the ragged multi-record AEMO rows (pads short rows, ignores extra
         fields). Files are landed UNCOMPRESSED (Files/csv_raw/**): Fabric OPENROWSET can't
         read gzip CSV — DATA_COMPRESSION is only valid under PARSER 1.0, which then can't
         parse the ragged/quoted AEMO data (verified against the warehouse). --#}
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    {#-- Use the DEFAULT quote ('"'): AEMO wraps its 'D'-row values (dates, numbers) in double
         quotes, so the parser must STRIP them — verified against the warehouse, 7.7M DUNIT
         rows all cast cleanly. (An earlier FIELDQUOTE override to "disable" quotes left the
         literal `"` in every value, nulling all the TRY_CASTs.) --#}
    FIRSTROW = 1
)
WITH (
{%- for c in columns %}
    [{{ c }}] VARCHAR(8000) {{ loop.index }}{{ "," if not loop.last }}
{%- endfor %}
)
{% endmacro %}
