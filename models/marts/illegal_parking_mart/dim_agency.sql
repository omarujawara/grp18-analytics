WITH agencies AS (
   SELECT DISTINCT
        agency AS agency,
        agency_name AS agency_name
   FROM {{ ref('stg_311_illegal_parking') }}
),

final_agency_dim AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([
           'agency',
           'agency_name'
       ]) }} AS agency_key,
       agency,
       agency_name
   FROM agencies
)

SELECT * FROM final_agency_dim