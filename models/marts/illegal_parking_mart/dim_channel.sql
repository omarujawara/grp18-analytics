WITH channel AS (
    SELECT DISTINCT
        open_data_channel_type AS open data channel type 
    FROM {{ ref('stg_311_illegal_parking')}}
),

final_channel_dim AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
                'open_data_channel_type'
        ]) }} AS channel_key,
    FROM channel 
)

SELECT * FROM final_channel_dim 