-- Create a vehicle type dimension from staged MVC collision data
-- Grain: one row per distinct vehicle type value

-- Purpose of this model:
-- 1. Standardize vehicle type values from the MVC staging model
-- 2. Create a reusable vehicle type dimension for crash analysis
-- 3. Support the fact table foreign key for the representative vehicle type

WITH source AS (

    SELECT *
    FROM {{ ref('stg_mvc_crashes') }}

), 

cleaned AS (

    SELECT DISTINCT
        NULLIF(TRIM(CAST(vehicle_type_code1 AS STRING)), '') AS vehicle_type_desc
    FROM source
    WHERE vehicle_type_code1 IS NOT NULL

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['vehicle_type_desc']) }} AS vehicle_type_key,
        vehicle_type_desc
    FROM cleaned
    WHERE vehicle_type_desc IS NOT NULL

)

SELECT *
FROM final