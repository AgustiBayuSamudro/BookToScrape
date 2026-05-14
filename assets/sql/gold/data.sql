CREATE SCHEMA IF NOT EXISTS minio.gold
WITH(
	location = 's3a://etl-data/trino-warehouse/gold/'
);

SELECT * FROM minio.gold.dim_time;
SELECT * FROM minio.gold.dim_books;
SELECT * FROM minio.gold.fact_books;
SELECT * FROM minio.gold.buku_terfaforit;
SELECT * FROM minio.gold.buku_kurang_digemari;
SELECT * FROM minio.gold.total_nilai_iventori;
SELECT * FROM minio.gold.total_harga_berdasarkan_rating;
SELECT * FROM minio.gold.top_buku_termahal;


CREATE OR REPLACE VIEW minio.gold.top_buku_termahal AS
SELECT
    b.title,
    f.price
FROM minio.gold.fact_books f
JOIN minio.gold.dim_books b
ON f.book_id = b.book_id
ORDER BY f.price DESC
LIMIT 10;

CREATE OR REPLACE VIEW minio.gold.total_harga_berdasarkan_rating AS
SELECT
    b.rating,
    AVG(f.price) AS avg_price
FROM minio.gold.fact_books f
JOIN minio.gold.dim_books b
ON f.book_id = b.book_id
GROUP BY b.rating
ORDER BY b.rating;

CREATE OR REPLACE VIEW minio.gold.total_nilai_iventori AS
SELECT 
    SUM(price) AS total_inventory_value,
    COUNT(book_id) AS total_books,
    AVG(price) AS average_book_price
FROM minio.gold.fact_books;

CREATE OR REPLACE VIEW minio.gold.buku_kurang_digemari AS
SELECT 
	title,
	rating 
FROM minio.gold.dim_books 
WHERE rating <= 2 
ORDER BY rating ASC;

CREATE OR REPLACE VIEW minio.gold.buku_terfaforit AS
SELECT 
	title,
	rating 
FROM minio.gold.dim_books 
WHERE rating = 5 
ORDER BY title ASC;


CREATE TABLE minio.gold.fact_books
WITH (format = 'PARQUET', external_location = 's3a://etl-data/data-lake/gold/fact_books/') AS
SELECT
    s.book_id,                         
    t.date_key, 
    s.price    
FROM minio.silver.books s
JOIN minio.gold.dim_books b ON s.book_id = b.book_id
JOIN minio.gold.dim_time t ON CAST(s.created_at AS DATE) = t.date_key;

CREATE TABLE minio.gold.dim_time
WITH(format = 'PARQUET', external_location = 's3a://etl-data/data-lake/gold/dim_time/') AS
SELECT DISTINCT
    CAST(created_at AS DATE) AS date_key,
    EXTRACT(YEAR FROM created_at) AS year,
    EXTRACT(QUARTER FROM created_at) AS quarter,
    EXTRACT(MONTH FROM created_at) AS month,
    FORMAT_DATETIME(created_at, 'MMMM') AS month_name,
    EXTRACT(DAY FROM created_at) AS day
FROM minio.silver.books;

CREATE TABLE minio.gold.dim_books 
WITH(format = 'PARQUET', external_location = 's3a://etl-data/data-lake/gold/dim_books/') AS
SELECT
	book_id,
	title ,	
	rating
FROM minio.silver.books;