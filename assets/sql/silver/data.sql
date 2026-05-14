CREATE SCHEMA IF NOT EXISTS minio.silver
WITH(
	location = 's3a://etl-data/trino-warehouse/silver/'
);

CREATE TABLE minio.silver.books(	
		book_id varchar,
        title varchar,
        price double,
        rating integer,
        created_at timestamp,
        updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/book/',
    format = 'PARQUET'
);

