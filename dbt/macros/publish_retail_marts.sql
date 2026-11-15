{% macro publish_retail_marts() %}
    {% set marts = {
        'mart_daily_sales': ['order_date', 'net_sales', 'order_count', 'average_order_value'],
        'mart_product_performance': ['product_id', 'product_name', 'category', 'net_sales', 'units_sold', 'order_count'],
        'mart_customer_activity': ['customer_id', 'customer_segment', 'order_count', 'net_sales', 'first_order_date', 'last_order_date', 'is_repeat_customer']
    } %}
    {# All three marts move to the validated candidate data in a single transaction. #}
    {% for mart, columns in marts.items() %}
        {% set candidate = ref(mart ~ '_candidate') %}
        {% set published = candidate.incorporate(path={'identifier': mart}) %}
        {% set published_rows = candidate.incorporate(path={'identifier': mart ~ '_published_rows'}) %}
        {% set column_list = columns | join(', ') %}

        {% if mart == 'mart_daily_sales' %}
            {% set existing = adapter.get_relation(database=published.database,
                schema=published.schema, identifier=mart) %}
            {% if existing %}
                {% set existing_columns = adapter.get_columns_in_relation(existing) | map(attribute='name') | list %}
                {# Upgrade the old two-column daily view only after the candidates pass. #}
                {% if 'order_count' not in existing_columns %}
                    {% call statement() %} drop view {{ published }} {% endcall %}
                    {% call statement() %} drop table if exists {{ published_rows }} {% endcall %}
                {% endif %}
            {% endif %}
        {% endif %}
        {% call statement() %}
            create table if not exists {{ published_rows }} (like {{ candidate }})
        {% endcall %}
        {% call statement() %}
            create or replace view {{ published }} as
            select {{ column_list }} from {{ published_rows }}
        {% endcall %}
        {% call statement() %} lock table {{ published_rows }} {% endcall %}
        {% call statement() %} delete from {{ published_rows }} {% endcall %}
        {% call statement() %}
            insert into {{ published_rows }} ({{ column_list }})
            select {{ column_list }} from {{ candidate }}
        {% endcall %}
        {% call statement() %} drop table {{ candidate }} {% endcall %}
    {% endfor %}
    {% do adapter.commit() %}
{% endmacro %}
