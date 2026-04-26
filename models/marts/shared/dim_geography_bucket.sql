WITH all_locations AS (
    SELECT DISTINCT 
        borough as borough,
        incident_zip as zip_code
    FROM {{ ref('stg_311_illegal_parking') }}
    WHERE incident_zip IS NOT NULL
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        borough as borough,
        zip_code as zip_code
    FROM {{ ref('stg_mvc_crashes') }}
    WHERE zip_code IS NOT NULL
),

geography_bucket AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code'])}} AS geo_key,
        borough,
        zip_code
    FROM all_locations
)

SELECT * FROM geography_bucket