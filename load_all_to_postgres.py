import io
import boto3
import psycopg2

# 1. Connect to MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# 2. Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="movielens",
    user="postgres",
    password="postgrespassword"
)
cur = conn.cursor()

# 3. Create raw schema and all 6 tables matching official specifications
cur.execute("""
CREATE SCHEMA IF NOT EXISTS raw;

-- 1. Movies
CREATE TABLE IF NOT EXISTS raw.raw_movies (
    movie_id INT,
    title TEXT,
    genres TEXT
);

-- 2. Ratings
CREATE TABLE IF NOT EXISTS raw.raw_ratings (
    user_id INT,
    movie_id INT,
    rating NUMERIC(2,1),
    timestamp BIGINT
);

-- 3. Tags
CREATE TABLE IF NOT EXISTS raw.raw_tags (
    user_id INT,
    movie_id INT,
    tag TEXT,
    timestamp BIGINT
);

-- 4. Links
CREATE TABLE IF NOT EXISTS raw.raw_links (
    movie_id INT,
    imdb_id TEXT,
    tmdb_id TEXT
);

-- 5. Genome Tags
CREATE TABLE IF NOT EXISTS raw.raw_genome_tags (
    tag_id INT,
    tag TEXT
);

-- 6. Genome Scores
CREATE TABLE IF NOT EXISTS raw.raw_genome_scores (
    movie_id INT,
    tag_id INT,
    relevance NUMERIC(8,7)
);
""")
conn.commit()

# Mapping: S3 Object Filename -> PostgreSQL Table
files_to_load = {
    "movies.csv": "raw.raw_movies",
    "ratings.csv": "raw.raw_ratings",
    "tags.csv": "raw.raw_tags",
    "links.csv": "raw.raw_links",
    "genome-tags.csv": "raw.raw_genome_tags",
    "genome-scores.csv": "raw.raw_genome_scores"
}

bucket_name = "netflix-raw-data"

for s3_file, table_name in files_to_load.items():
    print(f"Streaming {s3_file} from MinIO into {table_name}...")
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=s3_file)
        # Decode using UTF-8 to handle special characters (e.g., "Misérables, Les (1995)")
        stream = io.StringIO(obj['Body'].read().decode('utf-8'))

        cur.execute(f"TRUNCATE TABLE {table_name};")
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", stream)
        conn.commit()
        print(f" Successfully loaded {s3_file} into {table_name}.")
    except Exception as e:
        print(f" Error loading {s3_file}: {e}")
        conn.rollback()

cur.close()
conn.close()
print("All 6 raw files loaded successfully into PostgreSQL!")