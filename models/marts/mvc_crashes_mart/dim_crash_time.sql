-- Create a crash time dimension from staged MVC crash data
-- Grain: one row per distinct crash time value
--
-- Purpose of this model:
-- 1. Standardize crash time values from the MVC staging model
-- 2. Create a reusable time dimension for crash analysis
-- 3. Derive hour and time_band for easier downstream reporting


WITH source AS (

    SELECT *
    FROM {{ ref('stg_mvc_crashes') }}

),

cleaned AS (

    SELECT DISTINCT
        SAFE_CAST(crash_time AS TIME) AS crash_time
    FROM source
    WHERE crash_time IS NOT NULL

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['crash_time']) }} AS crash_time_key,
        crash_time,
        EXTRACT(HOUR FROM crash_time) AS hour,
        CASE
            WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 0 AND 5 THEN 'Night'
            WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 18 AND 23 THEN 'Evening'
            ELSE 'Unknown'
        END AS time_band
    FROM cleaned
    WHERE crash_time IS NOT NULL

)

SELECT *
FROM final