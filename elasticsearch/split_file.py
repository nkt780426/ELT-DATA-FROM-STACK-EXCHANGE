import json
import os
import shutil

# Đường dẫn đến file JSON gốc và thư mục lưu trữ file nhỏ
input_file_path = "../spark/output/part-00000-491d2fd5-4c92-4c76-b15d-7f0aafddea01-c000.json"
# input_file_path = "../spark/output/test.json"
output_folder = "./output"

# Số lượng file nhỏ cần tạo
num_small_files = 10

# Xóa thư mục đầu ra nếu tồn tại
if os.path.exists(output_folder):
    shutil.rmtree(output_folder)

# Tạo thư mục mới
os.makedirs(output_folder)


# Đọc dữ liệu từ file JSON
def read_json_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = [json.loads(line) for line in file]
    return data


# Đọc dữ liệu từ file JSON gốc
data = read_json_file(input_file_path)

# Tính số lượng bản ghi trong mỗi file nhỏ
records_per_small_file = len(data) // num_small_files

# Chia dữ liệu thành các file nhỏ
for i in range(num_small_files):
    start_index = i * records_per_small_file
    end_index = (
        (i + 1) * records_per_small_file if i < num_small_files - 1 else len(data)
    )

    subset_data = data[start_index:end_index]
    output_file_path = os.path.join(output_folder, f"small_file_{i + 1}.json")

    # Ghi dữ liệu vào file nhỏ với mỗi đối tượng JSON trên một dòng
    with open(output_file_path, "w", encoding="utf-8") as output_file:
        for obj in subset_data:
            json.dump(obj, output_file, ensure_ascii=False)
            output_file.write("\n")

print("Chia dữ liệu thành các file nhỏ thành công.")
