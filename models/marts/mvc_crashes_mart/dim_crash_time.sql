-- Create a crash time dimension from staged MVC crash data
-- Grain: one row per distinct hour of day

-- Purpose of this model:
-- 1. Derive hour-of-day values from the MVC staging model
-- 2. Create a reusable crash time dimension for crash analysis
-- 3. Bucket hours into broader time bands for reporting


WITH source AS (

    SELECT *
    FROM {{ ref('stg_mvc_crashes') }}

),

cleaned AS (

SELECT DISTINCT
    -- Extract the hour directly from crash_date
    -- This creates one distinct row per hour of day
    EXTRACT(HOUR FROM crash_date) AS hour
FROM source
WHERE crash_date IS NOT NULL

),

final AS (

SELECT
    {{ dbt_utils.generate_surrogate_key(['hour']) }} AS crash_time_key,
    hour,
    CASE
        WHEN hour BETWEEN 0 AND 5 THEN 'Night'
        WHEN hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Unknown'
    END AS time_band
FROM cleaned

)

SELECT *
FROM final