# Chạy docker container
docker-compose up -d


# Truy cập vào bash của container
docker exec -it 1e17e3cb7cfef279b5fbbd36dfbfadcb4a5101b02609514c5d91672eada60aba bash

# Thêm biến môi trường xác thực AWS bắt buộc phải set biến môi trường trước khi submit job
(window khi chạy local) 
set AWS_ACCESS_KEY_ID=your_access_key
set AWS_SECRET_ACCESS_KEY=your_secret_key
(linux khi chạy cluster)
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Chạy spark streaming trên cluster (tại bash của container master hoặc worker)
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--jars /opt/spark-apps/postgresql-42.2.22.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2,org.apache.hadoop:hadoop-aws:3.0.2 \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/streaming.py

# Chạy local
spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.2.2 streaming.py

