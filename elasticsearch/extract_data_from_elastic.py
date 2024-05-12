from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

# Thông tin xác thực
username = os.environ.get("ELASTIC_USERNAME")
password = os.environ.get("ELASTIC_PASSWORD")

# Kết nối đến Elasticsearch với thông tin xác thực
es = Elasticsearch(['http://34.87.36.15:9200'], basic_auth=(username, password))

# Thực hiện truy vấn
index_name = 'group_19'
query = {"query": {"match_all": {}}}
results = scan(es, query=query, index=index_name)

# Lưu dữ liệu về máy với encoding utf-8
output_file = 'output.json'
with open(output_file, 'w', encoding='utf-8') as f:
    for result in results:
        f.write(str(result) + '\n')

print(f'Data saved to {output_file}')