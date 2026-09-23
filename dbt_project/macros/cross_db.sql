{#
    Cross-warehouse shims so the same models build on Snowflake and on the
    local DuckDB target. Only the two Snowflake-specific constructs the models
    use are wrapped; everything else (datediff, count_if, any_value,
    date_trunc) is valid on both engines as written.

    The default__ implementations render the exact SQL the Snowflake models
    used before these macros existed.
#}

{# Extract a top-level key from the raw_payload JSON column as text. #}
{% macro json_text(column, key) -%}
    {{ return(adapter.dispatch('json_text')(column, key)) }}
{%- endmacro %}

{% macro default__json_text(column, key) -%}
    {{ column }}:{{ key }}::string
{%- endmacro %}

{% macro duckdb__json_text(column, key) -%}
    ({{ column }} ->> '{{ key }}')
{%- endmacro %}


{# Parse text to a zone-less timestamp; NULL (not an error) when unparseable. #}
{% macro try_to_timestamp(expr) -%}
    {{ return(adapter.dispatch('try_to_timestamp')(expr)) }}
{%- endmacro %}

{% macro default__try_to_timestamp(expr) -%}
    try_to_timestamp_ntz({{ expr }})
{%- endmacro %}

{% macro duckdb__try_to_timestamp(expr) -%}
    try_cast({{ expr }} as timestamp)
{%- endmacro %}
