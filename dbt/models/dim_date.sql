with recursive bounds as (
    select cast(min(order_date) as date) as first_date,
           cast(max(order_date) as date) as last_date
    from {{ source('core', 'orders') }}
), calendar (date_day, last_date) as (
    select first_date, last_date
    from bounds
    where first_date is not null
    union all
    select cast(dateadd(day, 1, date_day) as date), last_date
    from calendar
    where date_day < last_date
)
select date_day from calendar
