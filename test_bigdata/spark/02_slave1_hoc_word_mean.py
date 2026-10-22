# =====================================================================
# BÀI 2 (SLAVE 1): WORD MEAN BẰNG PYSPARK MAPREDUCE
# Sinh viên thực hiện: Nguyễn Đức Học - 24162039 (hoc-slave1)
# Mục tiêu: Tính độ dài trung bình của các từ trong customer_review (Word Mean)
# =====================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("02_WordMean_CustomerReview_Slave1_Hoc")
    .getOrCreate()
)

try:
    # Đọc CSV gốc trực tiếp từ HDFS; hàng đầu là header
    orders = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .csv("hdfs://anh24162006-master:9000/ecommerce_dw/staging/raw_orders")
    )

    reviews = orders.select("customer_review").where(
        col("customer_review").isNotNull()
        & (col("customer_review").cast("string") != "")
    )

    # MAP: tách mỗi review thành từ, ánh xạ thành (độ dài, 1)
    word_pairs = reviews.rdd.flatMap(
        lambda row: (row["customer_review"] or "").split()
    ).map(lambda word: (len(word), 1))

    # REDUCE: cộng tổng độ dài và tổng số từ
    total_chars, total_words = word_pairs.fold(
        (0, 0),
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    print("===== WORD MEAN: CUSTOMER REVIEW =====")
    print(f"Tổng số từ: {total_words}")

    if total_words:
        print(f"Tổng số ký tự: {total_chars}")
        print(f"Word Mean: {total_chars / total_words:.4f}")
    else:
        print("Không tìm thấy từ nào trong customer_review.")

finally:
    spark.stop()
