WITH all_locations AS (

    -- From 311 staging
    SELECT DISTINCT 
        CASE 
            WHEN borough = 'UNKNOWN or CITYWIDE' THEN 'UNKNOWN'
            ELSE borough
        END AS borough,

        CASE 
            WHEN incident_zip IS NULL THEN 'UNKNOWN'
            ELSE incident_zip
        END AS zip_code

    FROM {{ ref('stg_311_illegal_parking') }}

    UNION DISTINCT
    
    -- From MVC crashes staging
    SELECT DISTINCT 
        CASE 
            WHEN borough IS NULL THEN 'UNKNOWN'
            WHEN borough = 'UNKNOWN or CITYWIDE' THEN 'UNKNOWN'
            ELSE borough
        END AS borough,

        CASE 
            WHEN zip_code IS NULL THEN 'UNKNOWN'
            ELSE zip_code
        END AS zip_code

    FROM {{ ref('stg_mvc_crashes') }}

),

geography_bucket AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code']) }} AS geo_key,
        borough,
        zip_code
    FROM all_locations
)

SELECT * FROM geography_bucket