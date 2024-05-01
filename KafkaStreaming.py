from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, avg, min, max, count, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Define the schema for the streaming data
schema = StructType([
    StructField("stockName", StringType(), True),
    StructField("tradeType", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("eventTimeReadable", StringType(), True)
])

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .enableHiveSupport() \
    .master("local[*]") \
    .getOrCreate()

# Read from Kafka source
# Define Kafka broker addresses
bootstrap_servers = "ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"
spark.conf.set("hive.metastore.uris", "thrift://ec2-18-132-63-52.eu-west-2.compute.amazonaws.com:9083")
kafka_streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092") \
    .option("subscribe", "stockstreaming") \
    .option("startingOffsets", "latest") \
    .load()

# Split the value column based on comma and convert to appropriate types
split_df = kafka_streaming_df \
    .withColumn("value", col("value").cast("string")) \
    .withColumn("data", split(col("value"), ",")) \
    .select(
        col("data")[0].alias("stockName"),
        col("data")[1].alias("tradeType"),
        col("data")[2].cast("double").alias("price"),
        col("data")[3].cast("int").alias("quantity"),
        col("data")[4].cast("timestamp").alias("timestamp"),
        col("data")[5].alias("eventTimeReadable")
    )

# Define watermark based on the timestamp column
windowed_df = split_df \
    .withWatermark("timestamp", "5 seconds") \
    .groupBy(window("timestamp", "10 minutes"), "stockName", "tradeType") \
    .agg(max("price").alias("maxPrice"), avg("quantity").alias("avgQuantity"))

#console output
query = windowed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Write to Hive table
query = windowed_df \
      .writeStream \
      .format("parquet") \
      .option("database", "ukusmar") \
      .option("table", "stockstreaming") \
      .option("metastore.catalog.default", "hive") \
      .option("path", "/user/ec2-user/UKUSMarHDFS/Arun/Streaming") \
      .option("checkpointLocation", "/user/ec2-user/UKUSMarHDFS/Arun/StocksCheckingPoint") \
      .option("hive.metastore.uris", "thrift://ec2-18-132-63-52.eu-west-2.compute.amazonaws.com:9083") \
      .outputMode("append") \
      .start()

# query = windowed_df \
#      .writeStream \
#      .format("parquet") \
#      .option("path", "/user/ec2-user/UKUSMarHDFS/Arun/StreamingStocks") \
#      .option("checkpointLocation", "/user/ec2-user/UKUSMarHDFS/Arun/StocksCheckingPoint") \
#      .outputMode("append") \
#      .start()

# query = windowed_df \
#       .writeStream \
#       .format("csv") \
#       .option("path", "/user/ec2-user/UKUSMarHDFS/Arun/StreamingStocks") \
#       .option("checkpointLocation", "/user/ec2-user/UKUSMarHDFS/Arun/StocksCheckingPoint") \
#       .outputMode("append") \
#       .start()

# Await termination
query.awaitTermination()
