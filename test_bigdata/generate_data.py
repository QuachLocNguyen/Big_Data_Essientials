import csv
import os
import random

os.makedirs(os.path.dirname(__file__) + "/data", exist_ok=True)
data_dir = os.path.join(os.path.dirname(__file__), "data")

# 1. Tạo dữ liệu bán hàng sales_big.csv (150,000 dòng, 6 cột đơn giản)
cities = ["Hanoi", "HCM", "Danang"]
payments = ["Ewallet", "Cash", "Credit card"]
genders = ["Female", "Male"]
product_lines = [
    "Health and beauty",
    "Electronic accessories",
    "Home and lifestyle",
    "Sports and travel",
    "Food and beverages",
    "Fashion accessories",
]

random.seed(42)
sales_path = os.path.join(data_dir, "sales_big.csv")
with open(sales_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["invoice_id", "city", "payment", "gender", "product_line", "total"])
    for i in range(1, 150001):
        inv_id = f"INV-{i:06d}"
        city = random.choice(cities)
        payment = random.choice(payments)
        gender = random.choice(genders)
        prod = random.choice(product_lines)
        total = round(random.uniform(15.0, 950.0), 2)
        writer.writerow([inv_id, city, payment, gender, prod, total])

print(f"Đã tạo {sales_path} (150,000 dòng dữ liệu)")

# 2. Tạo dữ liệu văn bản input_text_big.txt cho 4 bài toán PySpark MapReduce
sentences = [
    "apache hadoop is an open source framework for distributed storage and processing of big data",
    "hdfs splits large files into blocks and distributes them across datanodes in the cluster",
    "yarn manages cluster resources and schedules containers on worker nodes",
    "mapreduce processes data in parallel using map and reduce phases across multiple machines",
    "apache hive provides an sql like interface called hiveql to query data stored in hdfs",
    "apache spark is a fast and general engine for large scale data processing in memory",
    "pyspark allows developers to write spark applications using python rdd and dataframe apis",
    "word count word mean word median and word standard deviation are classic mapreduce benchmarks",
]

text_path = os.path.join(data_dir, "input_text_big.txt")
with open(text_path, "w", encoding="utf-8") as f:
    for _ in range(10000):
        for s in sentences:
            f.write(s + "\n")

print(f"Đã tạo {text_path} (80,000 dòng văn bản)")

