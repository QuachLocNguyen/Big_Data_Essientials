# =====================================================================
# BÀI 5 (SLAVE 3): THỐNG KÊ ĐƠN HÀNG VÀ DOANH THU THEO SẢN PHẨM
# Sinh viên thực hiện: Nguyễn Văn Hoàng Anh - 24133004 (anh24133004-slave3)
# Công cụ: PySpark SQL; nguồn: các CSV đã nạp trên HDFS
# =====================================================================
import sys

from pyspark.sql import SparkSession

DEFAULT_ORDER_ITEMS = (
    "hdfs://anh24162006-master:9000/ecommerce_dw/staging/order_items"
)
DEFAULT_PRODUCTS = "hdfs://anh24162006-master:9000/ecommerce_dw/staging/products"

order_items_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ORDER_ITEMS
products_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_PRODUCTS

spark = (
    SparkSession.builder
    .appName("05_SalesByProduct_Slave3_Anh24133004")
    .getOrCreate()
)

try:
    # Đọc hai nguồn CSV đã có trên HDFS, giữ header để dùng tên cột trong SQL.
    order_items = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .csv(order_items_path)
    )
    products = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .csv(products_path)
    )

    order_items.createOrReplaceTempView("staging_order_items")
    products.createOrReplaceTempView("staging_products")

    # Mỗi order_id được đếm một lần cho từng sản phẩm; doanh thu lấy từ net_sales.
    sales_by_product = spark.sql("""
        SELECT
            p.product_id,
            p.product_name,
            COUNT(DISTINCT oi.order_id) AS total_orders,
            ROUND(SUM(CAST(oi.net_sales AS DECIMAL(18, 2))), 2) AS total_revenue
        FROM staging_order_items AS oi
        INNER JOIN staging_products AS p
            ON oi.product_id = p.product_id
        GROUP BY p.product_id, p.product_name
        ORDER BY total_revenue DESC, p.product_name ASC
        LIMIT 10
    """)

    print("===== TOP 10 SẢN PHẨM THEO TỔNG DOANH THU =====")
    print(f"HDFS order_items: {order_items_path}")
    print(f"HDFS products   : {products_path}")
    sales_by_product.cache()
    sales_by_product.show(10, truncate=False)
finally:
    spark.stop()
