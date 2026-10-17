WITH raw_ratings AS (
    SELECT * FROM {{ source('netflix_raw', 'raw_ratings')}}
)

SELECT
    user_id,
    movie_id,
    rating,
    TO_TIMESTAMP(timestamp) as rating_timestamp
FROM raw_ratings