with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
),

dates as (
    select
        date_day,
        extract(dow from date_day)     as day_of_week,
        extract(month from date_day)   as month_number,
        extract(quarter from date_day) as quarter_number,
        extract(year from date_day)    as year_number,
        to_char(date_day, 'Day')       as day_name,
        to_char(date_day, 'Month')     as month_name,
        extract(dow from date_day) in (0, 6) as is_weekend
    from date_spine
)

select * from dates
