# =====================================================================
# BÀI 4 (SLAVE 3): WORD STANDARD DEVIATION BẰNG PYSPARK MAPREDUCE
# Sinh viên thực hiện: Nguyễn Văn Hoàng Anh - 24133004 (anh24133004-slave3)
# Mục tiêu: Tính độ lệch chuẩn độ dài của các từ (Word Standard Deviation)
# =====================================================================
import math
import sys
from pyspark.sql import SparkSession

DEFAULT_INPUT = "hdfs://anh24162006-master:9000/ecommerce_dw/staging/raw_orders"
input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT

spark = (
    SparkSession.builder
    .appName("04_WordStdDev_Slave3_Anh24133004")
    .getOrCreate()
)

try:
    # Đọc CSV raw_orders từ HDFS theo cùng cách với bài Word Count.
    orders = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .csv(input_path)
    )
    reviews = orders.select("customer_review").where(
        orders["customer_review"].isNotNull()
        & (orders["customer_review"] != "")
    )

    # MAP: tách customer_review thành từ; mỗi từ đóng góp
    # (1, độ dài, bình phương độ dài).
    words = reviews.rdd.flatMap(
        lambda row: (row["customer_review"] or "").split()
    )
    stats_mapped = words.map(lambda word: (1, len(word), len(word) ** 2))

    # REDUCE: gộp thống kê phân tán về một bộ tổng.
    totals = stats_mapped.reduce(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    )

    total_words, sum_x, sum_x2 = totals
    mean = sum_x / total_words
    # Đây là độ lệch chuẩn tổng thể (population), theo công thức E[X²] - E[X]².
    variance = max((sum_x2 / total_words) - (mean ** 2), 0.0)
    std_dev = math.sqrt(variance)

    print("===== KẾT QUẢ WORD STANDARD DEVIATION (POPULATION) =====")
    print(f"HDFS input                           : {input_path}")
    print("Cột văn bản                          : customer_review")
    print(f"Tổng số từ (N)                       : {total_words}")
    print(f"Độ dài trung bình (Mean)             : {mean:.4f}")
    print(f"Phương sai tổng thể (Variance)       : {variance:.4f}")
    print(f"Độ lệch chuẩn tổng thể (StdDev)      : {std_dev:.4f}")
except ValueError as error:
    raise SystemExit(
        f"Không có từ nào trong input '{input_path}'. "
        "Hãy kiểm tra đường dẫn và dữ liệu trên HDFS."
    ) from error
finally:
    spark.stop()
