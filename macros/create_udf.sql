{% macro create_udf() %}
{% set sql %}

create schema if not exists {{target.schema}};

CREATE OR REPLACE FUNCTION {{target.schema}}.decode_url(ENCURL varchar)
 RETURNS varchar
 LANGUAGE JAVASCRIPT
AS
$$
 return decodeURI(ENCURL);
$$;
{% endset %}

{% do run_query(sql) %}
{% do log("udf created for decoding url", info=True) %}

{% endmacro %}