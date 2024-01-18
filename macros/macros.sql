{% macro test_phone_number(column_name) %}
  CASE
     WHEN {{ column_name }} REGEXP '^[0-9]{2}-[0-9]{3}-[0-9]{3}-[0-9]{4}$' THEN {{ column_name }}
    ELSE NULL
  END
{% endmacro %}


{% macro get_total_lineitems() %}
    {% set query %}
        SELECT COUNT(*) FROM {{ ref('fact_orders') }}
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set total_lineitems = results.columns[0].values()[0] %}
    {% else %}
        {% set total_lineitems = 0 %}
    {% endif %}
    
    {{ return(total_lineitems) }}
{% endmacro %}




{% macro get_supplier_num_orders() %}
    {% set query %}
    select 
        count(DISTINCT(order_key)) as num_orders,
        supp_key
    from fact_orders
    group by supp_key
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set num_orders = results.columns[0].values()[0] %}
    {% else %}
        {% set num_orders = 0 %}
    {% endif %}
    
    {{ return(total_lineitems) }}
{% endmacro %}