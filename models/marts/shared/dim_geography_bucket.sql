WITH all_locations AS (
    SELECT DISTINCT
    FROM {{ ref('stg_311_illegal_parking')}}
    
    UNION DISTINCT
    
    SELECT DISTINCT
    FROM {{ ref('stg_mvc_crashes')}}
    ),

    geography_bucket AS (
        SELECT
        {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code'])}} AS
    geo_key,
        borough,
        zip_code
    FROM all_locations
    )