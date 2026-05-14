from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Books_Bronze") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Read raw JSON
df_raw = spark.read.json(
    "s3a://etl-data/data-lake/raw/books/"
)

# Temp View
df_raw.createOrReplaceTempView("raw_books")

# SQL Transform
df_bronze = spark.sql("""
    SELECT 
        data.title,
        data.price,
        data.rating,
        data.scraped_at
    FROM (
        SELECT from_json(
            value,
            'title STRING, price STRING, rating STRING, scraped_at STRING'
        ) AS data
        FROM raw_books
    )
""")

output_path = "s3a://etl-data/data-lake/bronze/book"

df_bronze.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Berhasil! Data bronze book tersimpan di: {output_path}")

spark.stop()