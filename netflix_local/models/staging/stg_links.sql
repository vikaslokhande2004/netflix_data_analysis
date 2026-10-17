WITH raw_links AS (
    SELECT * FROM {{ source('netflix_raw', 'raw_links')}}
)

SELECT
    movie_id,
    imdb_id,
    tmdb_id
FROM raw_links