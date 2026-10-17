# 🎬 Netflix Local - dbt Transformation Project

This directory contains the **dbt Core** project for transforming, testing, and modeling the MovieLens/Netflix datasets in PostgreSQL.

For full project documentation including Docker infrastructure, MinIO object storage, and Python raw ingestion, see the [Root README.md](../README.md).

---

## 🏗️ dbt Project Architecture

```
netflix_local/
├── dbt_project.yml             # dbt configurations, model materializations
├── packages.yml                # External package dependencies (dbt_utils)
├── macros/                     # Custom SQL & Jinja macros
│   └── no_nulls_in_columns.sql
├── models/
│   ├── sources.yml             # raw.* source table definitions
│   ├── schema.yml              # Model documentation and generic test suites
│   ├── staging/                # Bronze views (stg_movies, stg_ratings, etc.)
│   ├── dim/                    # Silver dimensions (dim_movies, dim_users, dim_genome_tags)
│   ├── fct/                    # Silver facts (fct_ratings [incremental], fct_genome_scores)
│   └── mart/                   # Gold business marts (mart_movie_releases)
├── seeds/
│   └── seed_movie_release_dates.csv # Reference CSV loaded via `dbt seed`
└── snapshots/
    └── snap_tags.sql           # Type-2 Slowly Changing Dimension snapshot
```

---

## ⚡ Quick Start

### 1. Install Package Dependencies
```powershell
dbt deps
```

### 2. Load Seeds
```powershell
dbt seed
```

### 3. Run Models & Snapshots
```powershell
# Run all transformations
dbt run

# Run SCD Type-2 snapshots
dbt snapshot
```

### 4. Execute Data Tests
```powershell
dbt test
```

### 5. Generate & View Documentation Lineage
```powershell
dbt docs generate
dbt docs serve --port 8080
```
Visit [http://localhost:8080](http://localhost:8080) for the interactive DAG lineage graph and schema definitions.
