import os
import json
from elasticsearch import Elasticsearch, helpers

# Thông tin xác thực
username = os.environ.get("ELASTIC_USERNAME")
password = os.environ.get("ELASTIC_PASSWORD")

# Kết nối đến Elasticsearch với thông tin xác thực
es = Elasticsearch(["http://34.87.36.15:9200"], basic_auth=(username, password))

# Đường dẫn đến file JSON
file_path = "./output/small_file_10.json"


# Đọc dữ liệu từ file JSON
def read_json_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = [json.loads(line) for line in file]
    return data


# Đọc dữ liệu từ file JSON
data = read_json_file(file_path)


# Đẩy dữ liệu lên Elasticsearch
def push_to_elasticsearch(data):
    actions = [
        {"_op_type": "index", "_index": "bigdata_nhom_2", "_source": item}
        for item in data
    ]

    helpers.bulk(es, actions)


# Gọi hàm để đẩy dữ liệu
push_to_elasticsearch(data)
