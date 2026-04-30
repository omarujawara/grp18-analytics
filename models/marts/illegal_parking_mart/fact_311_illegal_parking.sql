-- Grain: one row per illegal parking complaint


WITH complaints AS (
    -- All data from staging
    SELECT * FROM {{ ref('stg_311_illegal_parking') }}
),

dim_date AS (
    SELECT date_key, full_date
    FROM {{ ref('dim_date') }}
),

dim_geography AS (
    SELECT geo_key, zip_code, borough
    FROM {{ ref('dim_geography_bucket') }}
),

dim_complaint AS (
    SELECT complaint_key, complaint_type, descriptor, location_type
    FROM {{ ref('dim_complaint') }}
),

dim_agency AS (
    SELECT agency_key, agency, agency_name
    FROM {{ ref('dim_agency') }}
),

dim_channel AS (
    SELECT channel_key, method_of_submission
    FROM {{ ref('dim_channel') }}
),

dim_status AS (
    SELECT status_key, status
    FROM {{ ref('dim_status') }}
),


final AS (
    SELECT

        -- Surrogate PK
        {{ dbt_utils.generate_surrogate_key(['c.complaint_id']) }} AS fact_illegal_parking_key,

        -- Natural key
        c.complaint_id,

        -- Foreign keys
        d.date_key,
        g.geo_key,
        dc.complaint_key,
        a.agency_key,
        ch.channel_key,
        s.status_key,

        -- Measure
        1 AS complaint_count   -- always 1 per row (can aggregate later)

    FROM complaints c

    -- Date
    LEFT JOIN dim_date d
        ON CAST(c.created_date AS DATE) = d.full_date

    -- Geography
    LEFT JOIN dim_geography g
        ON c.incident_zip = g.zip_code
        AND c.borough = g.borough

    -- Complaint
    LEFT JOIN dim_complaint dc
        ON c.complaint_type = dc.complaint_type
        AND COALESCE(c.descriptor, '') = COALESCE(dc.descriptor, '')
        AND COALESCE(c.location_type, '') = COALESCE(dc.location_type, '')

    -- Agency
    LEFT JOIN dim_agency a
        ON c.agency = a.agency
        AND COALESCE(c.agency_name, '') = COALESCE(a.agency_name, '')

    -- Channel
    LEFT JOIN dim_channel ch
        ON c.method_of_submission = ch.method_of_submission
    -- Status
    LEFT JOIN dim_status s
        ON c.status = s.status
)

SELECT * FROM final