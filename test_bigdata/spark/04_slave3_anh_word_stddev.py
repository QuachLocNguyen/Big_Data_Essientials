# =====================================================================
# BÀI 4 (SLAVE 3): WORD STANDARD DEVIATION BẰNG PYSPARK MAPREDUCE
# Sinh viên thực hiện: Nguyễn Văn Hoàng Anh - 24133004 (anh24133004-slave3)
# Mục tiêu: Tính độ lệch chuẩn độ dài của các từ (Word Standard Deviation)
# =====================================================================
import math
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("04_WordStdDev_Slave3_Anh24133004") \
    .getOrCreate()
sc = spark.sparkContext

# 1. Đọc file văn bản từ HDFS
lines = sc.textFile("hdfs://anh24162006-master:9000/test_bigdata/input/input_text_big.txt")

# 2. Bước MAP: Ánh xạ mỗi từ thành bộ 3 giá trị: (1, độ_dài, bình_phương_độ_dài)
words = lines.flatMap(lambda line: line.split())
stats_mapped = words.map(lambda word: (1, len(word), len(word) ** 2))

# 3. Bước REDUCE: Cộng dồn (Tổng số từ N, Tổng độ dài SumX, Tổng bình phương SumX2)
total_words, sum_x, sum_x2 = stats_mapped.reduce(
    lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
)

# 4. Tính Trung bình (Mean), Phương sai (Variance) và Độ lệch chuẩn (StdDev)
mean = sum_x / total_words
variance = (sum_x2 / total_words) - (mean ** 2)
std_dev = math.sqrt(variance)

print("===== KẾT QUẢ TÍNH WORD STANDARD DEVIATION (ĐỘ LỆCH CHUẨN) =====")
print(f"Tổng số từ (N)                      : {total_words}")
print(f"Độ dài trung bình (Mean)            : {mean:.4f}")
print(f"Phương sai (Variance)               : {variance:.4f}")
print(f"Độ lệch chuẩn (Standard Deviation)  : {std_dev:.4f}")

spark.stop()

