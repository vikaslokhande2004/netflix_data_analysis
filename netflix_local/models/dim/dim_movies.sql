WITH src_movies AS (
    SELECT * FROM {{ ref('stg_movies') }}
)

SELECT
    movie_id,
    INITCAP(TRIM(title)) AS movie_title,
    STRING_TO_ARRAY(genres, '|') AS genre_array,
    genres
FROM src_movies 