WITH channel AS (
    SELECT DISTINCT
        method_of_submission AS method_of_submission 
    FROM {{ ref('stg_311_illegal_parking')}}
),

final_channel_dim AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'method_of_submission'
        ]) }} AS channel_key,
        method_of_submission
    FROM channel 
)

SELECT * FROM final_channel_dim 