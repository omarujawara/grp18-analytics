-- Create a contributing factor dimension from staged MVC collision data
-- Grain: one row per distinct contributing factor value

-- Purpose of this model:
-- 1. Standardize contributing factor values from the MVC staging model
-- 2. Create a reusable factor dimension for crash analysis
-- 3. Support the fact table foreign key for the representative contributing factor

WITH source AS (

    SELECT *
    FROM {{ ref('stg_mvc_crashes') }}

), 

cleaned AS (

    SELECT DISTINCT
        NULLIF(TRIM(CAST(contributing_factor_vehicle_1 AS STRING)), '') AS factor_desc
    FROM source
    WHERE contributing_factor_vehicle_1 IS NOT NULL

),



final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['factor_desc']) }} AS factor_key,
        factor_desc
    FROM cleaned
    WHERE factor_desc IS NOT NULL

)

SELECT *
FROM final