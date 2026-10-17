WITH raw_genome_tags AS (
    SELECT * FROM {{ source('netflix_raw', 'raw_genome_tags')}}
)

SELECT
    tag_id,
    tag
FROM raw_genome_tags