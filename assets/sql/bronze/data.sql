CREATE SCHEMA IF NOT EXISTS minio.bronze
WITH(
	location = 's3a://etl-data/trino-warehouse/bronze/'
);

SELECT * FROM minio.bronze.books

CREATE TABLE minio.bronze.books(			
        title varchar,
        price varchar,
        rating varchar,
        scraped_at varchar
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/book/',
    format = 'PARQUET'
);

