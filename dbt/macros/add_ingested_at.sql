-- macros/add_ingested_at_column.sql
{% macro add_ingested_at_column() %}
    ALTER TABLE {{ this }} ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;
    UPDATE {{ this }} SET ingested_at = CURRENT_TIMESTAMP;
{% endmacro %}
