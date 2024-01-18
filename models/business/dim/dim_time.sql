WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="TO_DATE('" ~ env_var('DBT_FIRST_DATE', '1992-01-01') ~ "', 'YYYY-MM-DD')",
        end_date="TO_DATE('" ~ env_var('DBT_CURRENT_DATE', '2023-09-12') ~ "', 'YYYY-MM-DD')"
    ) }}
)

SELECT
    date_day AS full_date,
    DAY(date_day) AS day,
    MONTH(date_day) AS month,
    YEAR(date_day) AS year
FROM date_spine
