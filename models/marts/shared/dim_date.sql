-- Date dimension share by both 311 illegal parking and mvc crashes
WITH all_date AS (

    -- Get date only and no time from 311 illegal parking
    SELECT DISTINCT CAST(created_date AS DATE) AS full_date
    FROM {{ ref('stg_311_illegal_parking') }}
    WHERE created_date IS NOT NULL

    UNION DISTINCT

    -- Get date only and no time from mvc crashes
    SELECT DISTINCT CAST(crash_date AS DATE) AS full_date
    FROM {{ ref('stg_mvc_crashes') }}
    WHERE crash_date IS NOT NULL

),

date_dimension AS (

SELECT
    {{ dbt_utils.generate_surrogate_key(['full_date']) }} AS date_key,

    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    FORMAT_DATE('%B', full_date) AS month_name,
    EXTRACT(DAY FROM full_date) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week,
    FORMAT_DATE('%A', full_date) AS day_name,

    EXTRACT(DAYOFWEEK FROM full_date) IN (1,7) AS is_weekend,

    CASE
        -- New Year's Day
        WHEN EXTRACT(MONTH FROM full_date)=1
         AND EXTRACT(DAY FROM full_date)=1 THEN TRUE

        -- MLK Day (3rd Monday Jan)
        WHEN EXTRACT(MONTH FROM full_date)=1
         AND EXTRACT(DAYOFWEEK FROM full_date)=2
         AND EXTRACT(DAY FROM full_date) BETWEEN 15 AND 21 THEN TRUE

        -- Presidents Day (3rd Monday Feb)
        WHEN EXTRACT(MONTH FROM full_date)=2
         AND EXTRACT(DAYOFWEEK FROM full_date)=2
         AND EXTRACT(DAY FROM full_date) BETWEEN 15 AND 21 THEN TRUE

        -- Memorial Day (last Monday May)
        WHEN EXTRACT(MONTH FROM full_date)=5
         AND EXTRACT(DAYOFWEEK FROM full_date)=2
         AND EXTRACT(DAY FROM full_date) >= 25 THEN TRUE

        -- Juneteenth
        WHEN EXTRACT(MONTH FROM full_date)=6
         AND EXTRACT(DAY FROM full_date)=19 THEN TRUE

        -- Independence Day
        WHEN EXTRACT(MONTH FROM full_date)=7
         AND EXTRACT(DAY FROM full_date)=4 THEN TRUE

        -- Labor Day (1st Monday Sep)
        WHEN EXTRACT(MONTH FROM full_date)=9
         AND EXTRACT(DAYOFWEEK FROM full_date)=2
         AND EXTRACT(DAY FROM full_date) <= 7 THEN TRUE

        -- Columbus Day (2nd Monday Oct)
        WHEN EXTRACT(MONTH FROM full_date)=10
         AND EXTRACT(DAYOFWEEK FROM full_date)=2
         AND EXTRACT(DAY FROM full_date) BETWEEN 8 AND 14 THEN TRUE

        -- Veterans Day
        WHEN EXTRACT(MONTH FROM full_date)=11
         AND EXTRACT(DAY FROM full_date)=11 THEN TRUE

        -- Thanksgiving (4th Thursday Nov)
        WHEN EXTRACT(MONTH FROM full_date)=11
         AND EXTRACT(DAYOFWEEK FROM full_date)=5
         AND EXTRACT(DAY FROM full_date) BETWEEN 22 AND 28 THEN TRUE

        -- Christmas
        WHEN EXTRACT(MONTH FROM full_date)=12
         AND EXTRACT(DAY FROM full_date)=25 THEN TRUE

        ELSE FALSE
    END AS is_holiday

FROM all_date

)

SELECT *
FROM date_dimension