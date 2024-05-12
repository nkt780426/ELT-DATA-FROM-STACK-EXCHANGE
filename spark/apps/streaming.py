from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os

# Import UDFs from preprocess.py
import preprocess

# Kafka settings
kafka_topic_name = "stack_exchange"
# kafka_bootstrap_servers = "localhost:39092"
kafka_bootstrap_servers = "kafka-1:9092"

# AWS S3 settings
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
s3_bucket = "btl-bigdata"
s3_folder_name = "test"  # Thư mục trong bucket
s3_output_path = "s3a://{}/{}".format(s3_bucket, s3_folder_name)

# Initialize Spark Session with package configurations
spark = (
    SparkSession.builder.appName("KafkaConsumer")
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)

# Define schema for the data
schema = StructType(
    [
        StructField("title", StringType(), True),
        StructField("content", StringType(), True),
        StructField("time", StringType(), True),
        StructField("category", StringType(), True),
        StructField("views", StringType(), True),
        StructField("num_answer", StringType(), True),
        StructField("votes", StringType(), True),
        StructField("solved", StringType(), True),
    ]
)

# Read data from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic_name)
    .option("startingOffsets", "latest")
    # .option("startingOffsets", "earliest")
    .load()
)

# Convert JSON data from Kafka to DataFrame and apply preprocessing
df_json = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("content", preprocess.process_html("content"))
    .withColumn("views", preprocess.convert_to_numeric("views"))
    .withColumn("num_answer", preprocess.convert_to_numeric("num_answer"))
    .withColumn("votes", preprocess.convert_to_numeric("votes"))
)

# Write data to Amazon S3
query = (
    df_json.repartition(1)
    .writeStream.outputMode("append")
    .format("json")
    .option("checkpointLocation", "{}/checkpoint".format(s3_output_path))
    .option("path", "{}/data".format(s3_output_path))
    .start()
)

# Wait for the query to finish
query.awaitTermination()
