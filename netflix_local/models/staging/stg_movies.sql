WITH raw_movies AS (
    SELECT * FROM {{ source('netflix_raw', 'raw_movies')}}
)
SELECT
    movie_id,
    title,
    genres
FROM raw_movies