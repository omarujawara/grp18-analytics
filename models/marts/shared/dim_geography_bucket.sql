WITH all_locations AS (
    SELECT DISTINCT
    FROM {{ ref('stg_illegal_parking_mart')}}
    
    UNION DISTINCT
    
    SELECT DISTINCT
    FROM {{ ref('stg_mv_collisions_mart')}}
    ),

    geography_bucket AS (
        SELECT
        {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code'])}} AS
    geo_key,
        borough,
        zip_code
    FROM all_locations
    )