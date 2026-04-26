WITH complaint AS (
    SELECT DISTINCT
        complaint_type AS complaint_type,
        descriptor AS descriptor,
        location_type AS location_type  
    FROM {{ ref('stg_311_illegal_parking')}}
),

final_complaint_dim AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'complaint_type',
            'descriptor',
            'location_type'
        ]) }} AS complaint_key,
        complaint_type,
        descriptor,
        location_type
    FROM complaint 
)

SELECT * FROM final_complaint_dim 