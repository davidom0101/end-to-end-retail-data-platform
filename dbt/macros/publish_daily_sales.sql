{% macro publish_daily_sales() %}
    {% set candidate = ref('mart_daily_sales_candidate') %}
    {% set published = candidate.incorporate(path={'identifier': 'mart_daily_sales'}) %}
    {% set published_rows = candidate.incorporate(path={'identifier': 'mart_daily_sales_published_rows'}) %}

    {# Keep the existing reporting view and refresh its rows in a single transaction. #}
    {% call statement() %}
        create table if not exists {{ published_rows }} (like {{ candidate }})
    {% endcall %}
    {% call statement() %}
        create or replace view {{ published }} as
        select order_date, net_sales from {{ published_rows }}
    {% endcall %}
    {% call statement() %}
        lock table {{ published_rows }}
    {% endcall %}
    {% call statement() %}
        delete from {{ published_rows }}
    {% endcall %}
    {% call statement() %}
        insert into {{ published_rows }} (order_date, net_sales)
        select order_date, net_sales from {{ candidate }}
    {% endcall %}
    {% call statement() %}
        drop table {{ candidate }}
    {% endcall %}
    {% do adapter.commit() %}
{% endmacro %}
