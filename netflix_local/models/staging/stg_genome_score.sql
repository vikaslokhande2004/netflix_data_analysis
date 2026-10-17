WITH raw_genome_scores AS (
    SELECT * FROM {{ source('netflix_raw', 'raw_genome_scores')}}
)

SELECT
    movie_id,
    tag_id,
    relevance
FROM raw_genome_scores