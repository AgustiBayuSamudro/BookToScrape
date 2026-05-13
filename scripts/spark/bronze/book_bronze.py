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

df_raw = spark.read.json("s3a://etl-data/data-lake/raw/books/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_books")

