# =====================================================================
# BÀI 2 (SLAVE 1): WORD MEAN BẰNG PYSPARK MAPREDUCE
# Sinh viên thực hiện: Nguyễn Đức Học - 24162039 (hoc-slave1)
# Mục tiêu: Tính độ dài trung bình của các từ trong văn bản (Word Mean)
# =====================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("02_WordMean_Slave1_Hoc") \
    .getOrCreate()
sc = spark.sparkContext

# 1. Đọc file văn bản từ HDFS
lines = sc.textFile("hdfs://anh24162006-master:9000/test_bigdata/input/input_text_big.txt")

# 2. Bước MAP: Tách từ và ánh xạ mỗi từ thành cặp (độ_dài_từ, 1)
words = lines.flatMap(lambda line: line.split())
length_pairs = words.map(lambda word: (len(word), 1))

# 3. Bước REDUCE: Cộng tổng độ dài tất cả các từ và tổng số lượng từ
total_length, total_words = length_pairs.reduce(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

word_mean = total_length / total_words

print("===== KẾT QUẢ TÍNH WORD MEAN (ĐỘ DÀI TRUNG BÌNH CỦA TỪ) =====")
print(f"Tổng số từ (Total Words)      : {total_words}")
print(f"Tổng độ dài (Total Characters): {total_length}")
print(f"Độ dài trung bình (Word Mean) : {word_mean:.4f}")

spark.stop()

