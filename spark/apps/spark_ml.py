import os
import pandas as pd
import preprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import Tokenizer, HashingTF, StringIndexer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import size
import boto3

# Tạo SparkSession
spark = SparkSession.builder.appName("SimpleSparkApp").getOrCreate()

# Đọc dữ liệu từ AWS S3
bucket_name = "btl-bigdata"
s3_input_file = "data.json"
schema = StructType(
    [
        StructField("title", StringType(), True),
        StructField("content", StringType(), True),
        StructField("time", StringType(), True),
        StructField("category", ArrayType(StringType()), True),
        StructField("views", StringType(), True),
        StructField("num_answer", StringType(), True),
        StructField("votes", StringType(), True),
        StructField("solved", StringType(), True),
    ]
)
df_all = spark.read.json(f"s3a://{bucket_name}/{s3_input_file}", schema=schema)

# Loại bỏ các bản ghi chứa null hoặc có độ dài của mảng "category" là 0
df_all_processed = df_all.na.drop().filter(size("category") > 0)

# Chuyển đổi cột category từ ArrayType(StringType) thành StringType
first_category_udf = udf(lambda x: x[0] if x else None, StringType())
df_train = df_all_processed.limit(20000).withColumn(
    "category", first_category_udf("category")
)

# Chuyển đổi category thành dạng số
indexer = StringIndexer(inputCol="category", outputCol="label", handleInvalid="keep")
tokenizer = Tokenizer(inputCol="content", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="features")
rf = RandomForestClassifier(labelCol="label", featuresCol="features")

# Tạo Pipeline
pipeline = Pipeline(stages=[indexer, tokenizer, hashingTF, rf])

# Chia dữ liệu thành tập huấn luyện và tập kiểm tra
(train_data, test_data) = df_train.randomSplit([0.8, 0.2], seed=42)

# Huấn luyện mô hình
model = pipeline.fit(train_data)

# Dự đoán trên tập kiểm tra
predictions = model.transform(test_data)

# Đánh giá mô hình
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy}")

# Lưu mô hình vào AWS S3
s3_output_path = "model"
model.save(f"s3a://{bucket_name}/{s3_output_path}")

# Đóng SparkSession
spark.stop()
