from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Books_Silver") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Read raw JSON
df_raw = spark.read.parquet(
    "s3a://etl-data/data-lake/bronze/book/"
)

# Temp View
df_raw.createOrReplaceTempView("silver_books")

# SQL Transform
df_silver = spark.sql("""
    SELECT DISTINCT
        hex(md5((title))) AS book_id,
        LOWER(title) AS title ,
        CAST(REPLACE(price,'£','') AS Double) AS price ,
        CAST(
            CASE 
                WHEN rating = 'One' then 1
                WHEN rating = 'Two' then 2
                WHEN rating = 'Three' then 3
                WHEN rating = 'Four' then 4
                WHEN rating = 'Five' then 5
                ELSE 0
            END AS Integer) AS rating,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM silver_books;
""")

output_path = "s3a://etl-data/data-lake/silver/book"

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data silver book tersimpan di: {output_path}")

spark.stop()