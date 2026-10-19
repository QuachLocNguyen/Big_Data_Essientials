# =====================================================================
# BÀI 1 (MASTER): WORD COUNT BẰNG PYSPARK MAPREDUCE
# Sinh viên thực hiện: Lê Đặng Hoàng Anh - 24162006 (hoanganh24162006-master)
# =====================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("01_WordCount_Master_HoangAnh24162006") \
    .getOrCreate()
sc = spark.sparkContext

# 1. Đọc file văn bản từ HDFS
lines = sc.textFile("hdfs://anh24162006-master:9000/test_bigdata/input/input_text_big.txt")

# 2. Bước MAP: Tách từng dòng thành các từ và gán mỗi từ thành cặp (word, 1)
words = lines.flatMap(lambda line: line.split())
word_pairs = words.map(lambda word: (word, 1))

# 3. Bước REDUCE: Cộng dồn số lần xuất hiện của từng từ có cùng key
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

print("===== KẾT QUẢ WORD COUNT (20 TỪ ĐẦU TIÊN) =====")
for word, count in word_counts.take(20):
    print(f"{word:20s}: {count}")

print("===== TỔNG SỐ TỪ TRONG VĂN BẢN =====")
print("Total words:", words.count())

spark.stop()

