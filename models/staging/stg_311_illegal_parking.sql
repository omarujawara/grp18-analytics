-- Clean and standardize 311 Illegal Parking complaint data
-- Grain: one row per 311 complaint request
--
-- Purpose of this model:
-- 1. Take the raw 311 complaint source table and make it easier to use downstream
-- 2. Standardize important fields such as IDs, dates, borough, ZIP, and text values
-- 3. Preserve one row per complaint so this staging model can feed dimensions and the fact table

WITH source AS (

   -- Pull everything from the raw source table first.
   -- Using a short CTE name makes the later code easier to read.
   SELECT * 
   FROM {{ source('raw', 'raw_311_illegal_parking') }}

), -- Easier to refer to the dbt reference to a long name table this way

cleaned AS (

   SELECT

       -- Start with all columns from the raw source
       -- EXCEPT removes columns we want to explicitly clean / rename / cast below
       -- This helps avoid duplicate columns in the final output
       * EXCEPT (
           unique_key,
           created_date,
           closed_date,
           agency,
           agency_name,
           complaint_type,
           descriptor,
           location_type,
           status,
           incident_zip,
           borough,
           incident_address,
           street_name,
           cross_street_1,
           cross_street_2,
           latitude,
           longitude,
           open_data_channel_type
       ),

       ----------------------------------------------------------------------
       -- Identifiers
       ----------------------------------------------------------------------

       -- Rename the source system key to a clearer warehouse-style name
       -- Keep it as STRING to avoid any accidental numeric formatting issues
       CAST(unique_key AS STRING) AS complaint_id,

       ----------------------------------------------------------------------
       -- Date / Time
       ----------------------------------------------------------------------

       -- Cast date fields explicitly so downstream models have stable types
       -- Keeping created_date and closed_date as TIMESTAMP supports later
       -- derivation of date keys and any time-based analysis if needed
       CAST(created_date AS TIMESTAMP) AS created_date,
       CAST(closed_date AS TIMESTAMP) AS closed_date,

       ----------------------------------------------------------------------
       -- Request details
       ----------------------------------------------------------------------

       -- Standardize text fields as STRING.
       -- We are not creating surrogate keys here because this is staging only.
       -- The dimensions in the mart layer will handle distinct values and SK creation.
       CAST(agency AS STRING) AS agency,
       CAST(agency_name AS STRING) AS agency_name,
       CAST(complaint_type AS STRING) AS complaint_type,
       CAST(descriptor AS STRING) AS descriptor,

       -- Keep location_type because it may be useful downstream
       -- for the complaint dimension or descriptive analysis
       CAST(location_type AS STRING) AS location_type,

       -- Normalize status so values like "open", "Open", and " OPEN "
       -- all become the same standardized value
       UPPER(TRIM(CAST(status AS STRING))) AS status,

       ----------------------------------------------------------------------
       -- Location: ZIP code cleaning
       ----------------------------------------------------------------------

       -- ZIP codes are kept as STRING, not numeric, so leading zeros are preserved
       --
       -- This CASE statement handles several common data-quality issues:
       -- - "N/A" or "NA" are treated as missing
       -- - "ANONYMOUS" is preserved as a special value
       -- - 5-digit ZIP is accepted
       -- - 9-digit ZIP is accepted
       -- - ZIP+4 format such as 12345-6789 is accepted
       -- - everything else is set to NULL
       CASE
           WHEN UPPER(TRIM(CAST(incident_zip AS STRING))) IN ('N/A', 'NA') THEN NULL
           WHEN UPPER(TRIM(CAST(incident_zip AS STRING))) = 'ANONYMOUS' THEN 'Anonymous'
           WHEN LENGTH(CAST(incident_zip AS STRING)) = 5 THEN CAST(incident_zip AS STRING)
           WHEN LENGTH(CAST(incident_zip AS STRING)) = 9 THEN CAST(incident_zip AS STRING)
           WHEN LENGTH(CAST(incident_zip AS STRING)) = 10
               AND REGEXP_CONTAINS(CAST(incident_zip AS STRING), r'^\d{5}-\d{4}')
           THEN CAST(incident_zip AS STRING)
           ELSE NULL
       END AS incident_zip,

       ----------------------------------------------------------------------
       -- Location: Borough standardization
       ----------------------------------------------------------------------

       -- Standardize borough names to a clean small set of values
       -- This helps with grouping and joining later in dimensions / facts
       --
       -- We map several known variants to one standard borough label
       -- Anything else becomes "UNKNOWN or CITYWIDE"
       CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
           ELSE 'UNKNOWN or CITYWIDE'
       END AS borough,

       ----------------------------------------------------------------------
       -- Location: Address and coordinates
       ----------------------------------------------------------------------

       -- Preserve address fields as strings for possible descriptive analysis,
       -- troubleshooting, or future extensions of the model
       CAST(incident_address AS STRING) AS incident_address,
       CAST(street_name AS STRING) AS street_name,
       CAST(cross_street_1 AS STRING) AS cross_street_1,
       CAST(cross_street_2 AS STRING) AS cross_street_2,

       -- Cast coordinates to NUMERIC/DECIMAL style fields for geography analysis
       -- SAFE_CAST can be substituted here if your source sometimes has bad values
       CAST(latitude AS NUMERIC) AS latitude,
       CAST(longitude AS NUMERIC) AS longitude,

       ----------------------------------------------------------------------
       -- Submission channel
       ----------------------------------------------------------------------

       -- Rename this field to a clearer business-facing label
       -- This will likely feed the channel / submission method dimension
       CAST(open_data_channel_type AS STRING) AS method_of_submission,

       ----------------------------------------------------------------------
       -- Metadata
       ----------------------------------------------------------------------

       -- Track when this staging row was built by dbt
       -- Useful for debugging and auditability
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   ---------------------------------------------------------------------------
   -- Filters
   ---------------------------------------------------------------------------

   WHERE unique_key IS NOT NULL
   -- Complaint ID is required because this staging model keeps one row per complaint

   AND created_date IS NOT NULL
   -- Created date is required because complaint timing is central to the project
   -- and will later connect to the date dimension

   AND CAST(created_date AS DATE) >= DATE('2020-01-01')
   -- Project scope is focused on 2020 onward

   -- This filter is retained as a safeguard to ensure only Illegal Parking complaints
   -- are included in the staging model, even if upstream raw loading changes later
   AND complaint_type = 'Illegal Parking'

   ---------------------------------------------------------------------------
   -- Deduplicate
   ---------------------------------------------------------------------------

   QUALIFY ROW_NUMBER() OVER (
       PARTITION BY unique_key
       ORDER BY created_date DESC
   ) = 1
   -- If duplicate source rows exist for the same complaint,
   -- keep the most recent version by created_date

)

-- Final staged output
SELECT * 
FROM cleaned

-- Expected output:
-- one clean row per illegal parking complaint
-- this model should support downstream creation of:
-- 1. dim_date
-- 2. dim_geography_bucket
-- 3. dim_agency
-- 4. dim_complaint
-- 5. dim_status / dim_channel
-- 6. fact_311_illegal_parking_complaint