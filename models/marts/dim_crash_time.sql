-- Create a crash time dimension from staged MVC crash data
-- Grain: one row per distinct crash time value
--
-- Purpose of this model:
-- 1. Standardize crash time values from the MVC staging model
-- 2. Create a reusable time dimension for crash analysis
-- 3. Derive hour and time_band for easier downstream reporting

WITH crash_times AS (

    -- Pull distinct crash_time values from the staging model
    -- This dimension should contain one row per unique time value
    SELECT DISTINCT
        crash_time
    FROM {{ ref('stg_mvc_crashes') }}
    WHERE crash_time IS NOT NULL

), 

with_hour AS (

    SELECT
        -- Generate surrogate key for the time dimension
        {{ dbt_utils.generate_surrogate_key(['crash_time']) }} AS crash_time_key,

        -- Natural key / business value for this dimension
        crash_time,

        -- Extract hour to support easier reporting and grouping
        EXTRACT(HOUR FROM crash_time) AS hour

    FROM crash_times

),

final AS (

    SELECT
        crash_time_key,
        crash_time,
        hour,

        -- Create broad time-of-day buckets for analysis
        CASE
            WHEN hour BETWEEN 0 AND 5 THEN 'Night'
            WHEN hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN hour BETWEEN 18 AND 23 THEN 'Evening'
            ELSE 'Unknown'
        END AS time_band

    FROM with_hour

)

SELECT *
FROM final