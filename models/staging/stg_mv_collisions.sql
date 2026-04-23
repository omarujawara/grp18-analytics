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
           ELSE 'UNKNOWN or CITYWIDE'
       END AS borough,

       CAST(on_street_name AS STRING) AS on_street_name,
       CAST(off_street_name AS STRING) AS off_street_name,
       CAST(cross_street_name AS STRING) AS cross_street_name,
       CAST(latitude AS DECIMAL) AS latitude,
       CAST(longitude AS DECIMAL) AS longitude,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   -- Deduplicate
   QUALIFY ROW_NUMBER() OVER (PARTITION BY collision_id ORDER BY crash_date DESC) = 1
)

SELECT * FROM cleaned
-- All should be part of this table: stg_mv_collisions