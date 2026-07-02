with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2035-01-01' as date)"
    ) }}
)

select
    cast(to_char(date_day, 'YYYYMMDD') as int) as date_key,
    cast(date_day as date) as date_day,
    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    to_char(date_day, 'Month') as month_name,
    extract(day from date_day)::int as day_of_month,
    extract(isodow from date_day)::int as day_of_week,
    to_char(date_day, 'Day') as day_name,
    (extract(isodow from date_day) in (6, 7)) as is_weekend
from date_spine
