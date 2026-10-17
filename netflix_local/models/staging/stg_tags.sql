WITH raw_tags AS (
    SELECT * FROM {{ source('netflix_raw', 'raw_tags')}}
)

SELECT
    user_id,
    movie_id,
    tag,
    TO_TIMESTAMP(timestamp) as tag_timestamp
FROM raw_tags