-- Grain: one row per Motor Vehicle Collision
    WITH requests AS (
        SELECT * FROM {{ ref('stg_mvc_crashes') }}
    ),

    dim_date AS (
        SELECT 
            date_key, 
            full_date, 
            month_name, 
            day_of_month, 
            day_of_week, 
            day_name, 
            is_weekend, 
            is_holiday 
        FROM {{ ref('dim_date') }}
  ),

    dim_geography_bucket AS (
        SELECT geo_key, zip_code, borough FROM {{ ref('dim_geography_bucket') }}
    ),

    dim_crash_time AS (
        SELECT
            crash_time_key,
            hour,
            time_band
        FROM {{ ref('dim_crash_time') }}
    ),

    dim_contributing_factor AS (
        SELECT
            factor_key,
            factor_desc
        FROM {{ ref('dim_contributing_factor') }}
    ),

    dim_vehicle_type AS (
        SELECT
            vehicle_type_key,
            vehicle_type_desc
        FROM {{ ref('dim_vehicle_type') }}
    ),

    final AS (
        SELECT
            -- Surrogate key, generated from unique id in data.
            {{ dbt_utils.generate_surrogate_key(['r.collision_id']) }} AS fact_mvc_key,

            r.collision_id,
            d.date_key,
            ge.geo_key,
            ct.crash_time_key,
            cf.factor_key,
            vt.vehicle_type_key,
            r.number_of_persons_injured,
            r.number_of_persons_killed

        FROM requests r

        LEFT JOIN dim_date d 
            ON CAST(r.crash_date AS DATE) = d.full_date

        LEFT JOIN dim_geography_bucket ge
            ON r.borough = ge.borough
            AND r.zip_code = ge.zip_code
        
        LEFT JOIN dim_crash_time ct
            ON CAST(EXTRACT(HOUR FROM r.crash_date) AS INT) = ct.hour

        LEFT JOIN dim_contributing_factor cf
            ON TRIM(CAST(r.contributing_factor_vehicle_1 AS STRING)) = TRIM(CAST(cf.factor_desc AS STRING)) 
        
        LEFT JOIN dim_vehicle_type vt
            ON TRIM(CAST(r.vehicle_type_code1 AS STRING)) = TRIM(CAST(vt.vehicle_type_desc AS STRING))
    )

    SELECT * FROM final