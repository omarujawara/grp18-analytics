WITH statuses AS (

    SELECT DISTINCT
        UPPER(TRIM(status)) AS status
    FROM {{ ref('stg_311_illegal_parking') }}
    WHERE status IS NOT NULL

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_key,
        status
    FROM statuses

)

SELECT *
FROM final