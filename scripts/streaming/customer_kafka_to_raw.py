from pyspark.sql import SparkSession

# Inisialisasi dengan paket eksplisit
spark = SparkSession.builder \
    .appName("KafkaToMinio_Streaming") \
    .getOrCreate()

# Konfigurasi Hadoop gaya Olist (Terbukti Berhasil)
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.fast.upload", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.attempts.maximum", "10")
hadoop_conf.set("fs.s3a.multipart.size", "104857600")

df_kafka = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "books-topic")
    .option("startingOffsets", "earliest")
    .load())

query = (df_kafka.selectExpr("CAST(value AS STRING)")
    .writeStream
    .format("json") 
    .option("path", "s3a://etl-data/data-lake/raw/books/")
    .option("checkpointLocation", "/opt/spark/scripts/streaming/checkpoints/books/")
    .trigger(processingTime='5 seconds')
    .start())

print("Streaming Aktif... Memantau Kafka topic 'books-topic'")
query.awaitTermination()