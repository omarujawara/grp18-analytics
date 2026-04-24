WITH source AS (
   SELECT * FROM {{ source('raw', 'raw_mvc_crashes') }}
),

cleaned AS (
   SELECT
       -- Get all columns from source, except ones we're transforming below
       -- To do cleaning on them or explicitly cast them as types just in case
       * EXCEPT (
           collision_id,
           crash_date,
           crash_time,
           borough,
           zip_code,
           latitude,
           longitude,
           on_street_name,
           off_street_name,
           cross_street_name
       ),

       -- Identifiers
        CAST(collision_id AS STRING) AS collision_id,

        -- Time
        CAST(crash_time as STRING) as crash_time,

       -- Date/Time
        CAST(
            DATETIME(
                DATE(crash_date),
                PARSE_TIME('%H:%M',
                    CASE
                        WHEN REGEXP_CONTAINS(crash_time, r'^\d{1}:\d{2}$')
                        THEN CONCAT('0', crash_time)
                        ELSE crash_time
                    END
                )
            ) AS TIMESTAMP
        ) AS crash_date,

       -- Location - clean zip code, handling several common zip code data problems
        CASE
           WHEN UPPER(TRIM(CAST(zip_code AS STRING))) IN ('N/A', 'NA') THEN NULL
           WHEN UPPER(TRIM(CAST(zip_code AS STRING))) = 'ANONYMOUS' THEN 'Anonymous'
           WHEN LENGTH(CAST(zip_code AS STRING)) = 5 THEN CAST(zip_code AS STRING)
           WHEN LENGTH(CAST(zip_code AS STRING)) = 9 THEN CAST(zip_code AS STRING)
           WHEN LENGTH(CAST(zip_code AS STRING)) = 10
               AND REGEXP_CONTAINS(CAST(zip_code AS STRING), r'^\d{5}-\d{4}')
           THEN CAST(zip_code AS STRING)
           ELSE NULL
        END AS zip_code,

    -- Location - standardized borough, just in case
    CASE
        WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
        WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
        WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
        WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
        WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
        ELSE NULL
    END AS borough,

     -- Treat 0 as NULL for latitude and longitude
    NULLIF(CAST(latitude AS DECIMAL), 0) AS latitude,
    NULLIF(CAST(longitude AS DECIMAL), 0) AS longitude,

    CAST(on_street_name AS STRING) AS on_street_name,
    CAST(off_street_name AS STRING) AS off_street_name,
    CAST(cross_street_name AS STRING) AS cross_street_name,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source

-- Deduplicate
QUALIFY ROW_NUMBER() OVER (PARTITION BY collision_id ORDER BY crash_date DESC) = 1
),

-- Get the exact polygon area for land-based coordinates
spatial_exact AS (
    SELECT
        c.collision_id,
        z.zip_code AS derived_zip,
        z.borough  AS derived_borough
    FROM cleaned c
    CROSS JOIN {{ ref('nyc_zip_polygons') }} z
    WHERE
        (c.zip_code IS NULL OR c.borough IS NULL)
        AND c.latitude  IS NOT NULL
        AND c.longitude IS NOT NULL
        AND ST_CONTAINS(
            ST_GEOGFROMTEXT(z.geom_wkt),
            ST_GEOGPOINT(c.longitude, c.latitude)
        )
),

-- Identify rows that got no match from the exact lookup (bridges, ocean, etc.)
unmatched AS (
    SELECT c.collision_id, c.latitude, c.longitude
    FROM cleaned c
    LEFT JOIN spatial_exact s USING (collision_id)
    WHERE
        (c.zip_code IS NULL OR c.borough IS NULL)
        AND c.latitude  IS NOT NULL
        AND c.longitude IS NOT NULL
        AND s.collision_id IS NULL  -- no exact match found
),

-- Assisn the nearest polygon area for unmatched coordinates
spatial_nearest AS (
    SELECT
        collision_id,
        derived_zip,
        derived_borough
    FROM (
        SELECT
            u.collision_id,
            z.zip_code AS derived_zip,
            z.borough  AS derived_borough,
            ROW_NUMBER() OVER (
                PARTITION BY u.collision_id
                ORDER BY ST_DISTANCE(
                    ST_GEOGFROMTEXT(z.geom_wkt),
                    ST_GEOGPOINT(u.longitude, u.latitude)
                ) ASC
            ) AS rn
        FROM unmatched u
        CROSS JOIN {{ ref('nyc_zip_polygons') }} z
    )
    WHERE rn = 1
),

-- Combine both layers
spatial_lookup AS (
    SELECT * FROM spatial_exact
    UNION ALL
    SELECT * FROM spatial_nearest
),

enriched AS (
    SELECT
        c.* EXCEPT (zip_code, borough),
        NULLIF(COALESCE(c.zip_code, s.derived_zip), '99999') AS zip_code,
        COALESCE(c.borough, s.derived_borough, 'UNKNOWN or CITYWIDE') AS borough
    FROM cleaned c
    LEFT JOIN spatial_lookup s USING (collision_id)
)

SELECT * FROM enriched