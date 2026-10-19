# =====================================================================
# BÀI 3 (SLAVE 2): WORD MEDIAN BẰNG PYSPARK MAPREDUCE
# Sinh viên thực hiện: Quách Lộc Nguyên - 21139037 (nguyen-slave2)
# Mục tiêu: Tính trung vị độ dài của các từ trong văn bản (Word Median)
# =====================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("03_WordMedian_Slave2_Nguyen") \
    .getOrCreate()
sc = spark.sparkContext

# 1. Đọc file văn bản từ HDFS
lines = sc.textFile("hdfs://anh24162006-master:9000/test_bigdata/input/input_text_big.txt")

# 2. Bước MAP: Tách từ và ánh xạ mỗi từ thành cặp (độ_dài_từ, 1)
words = lines.flatMap(lambda line: line.split())
length_pairs = words.map(lambda word: (len(word), 1))

# 3. Bước REDUCE: Đếm tần suất xuất hiện của từng độ dài từ và sắp xếp tăng dần
length_counts = length_pairs.reduceByKey(lambda a, b: a + b).sortByKey().collect()

# 4. Tìm vị trí chính giữa (Median) từ bảng phân phối tần suất độ dài
total_words = sum(count for _, count in length_counts)
mid_pos_1 = (total_words - 1) // 2
mid_pos_2 = total_words // 2

running_count = 0
val1, val2 = None, None
for length, count in length_counts:
    running_count += count
    if val1 is None and running_count > mid_pos_1:
        val1 = length
    if val2 is None and running_count > mid_pos_2:
        val2 = length
        break

word_median = (val1 + val2) / 2.0

print("===== PHÂN PHỐI ĐỘ DÀI TỪ (LENGTH -> COUNT) =====")
for length, count in length_counts:
    print(f"Độ dài {length:2d} ký tự: {count} từ")

print("===== KẾT QUẢ TÍNH WORD MEDIAN (TRUNG VỊ ĐỘ DÀI TỪ) =====")
print(f"Tổng số từ (Total Words)     : {total_words}")
print(f"Trung vị độ dài (Word Median): {word_median}")

spark.stop()

