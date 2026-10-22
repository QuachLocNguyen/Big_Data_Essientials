# Hướng Dẫn Triển Khai & Vận Hành Data Warehouse Trên Apache Hive & PySpark

Tài liệu này tổng hợp toàn bộ quá trình phân tích dataset, thiết kế kiến trúc Data Warehouse (DW), các bước nạp dữ liệu, kịch bản truy vấn phân tích cho các thành viên, hướng dẫn kết nối DBeaver từ máy Windows host, và chi tiết cách khắc phục các sự cố kỹ thuật thực tế gặp phải trong quá trình triển khai trên cụm Hadoop/Docker.

---

## Mục Lục
1. [Phân Tích Bộ Dữ Liệu (Dataset Analysis)](#1-phân-tích-bộ-dữ-liệu-dataset-analysis)
2. [Kiến Trúc Data Warehouse & Thiết Kế Schema](#2-kiến-trúc-data-warehouse--thiết-kế-schema)
3. [Quy Trình Triển Khai & Nạp Dữ Liệu Từng Bước](#3-quy-trình-triển-khai--nạp-dữ-liệu-từng-bước)
4. [Kịch Bản Truy Vấn Phân Tích Demo Theo Phân Công Nhóm](#4-kịch-bản-truy-vấn-phân-tích-demo-theo-phân-công-nhóm)
5. [Tích Hợp Dữ Liệu Với 4 Bài Toán PySpark MapReduce](#5-tích-hợp-dữ-liệu-với-4-bài-toán-pyspark-mapreduce)
6. [Hướng Dẫn Kết Nối DBeaver Từ Windows Host](#6-hướng-dẫn-kết-nối-dbeaver-từ-windows-host)
7. [Bản Chất Kiến Trúc: Hive Metastore vs HiveServer2](#7-bản-chất-kiến-trúc-hive-metastore-vs-hiveserver2)
8. [Nhật Ký Xử Lý Lỗi Kỹ Thuật (Troubleshooting Guide)](#8-nhật-ký-xử-lý-lỗi-kỹ-thuật-troubleshooting-guide)

---

## 1. Phân Tích Bộ Dữ Liệu (Dataset Analysis)

Thư mục `test_bigdata/data/` chứa bộ dữ liệu **E-commerce & Retail Sales Customer Analytics Dataset** với tổng dung lượng **~86.5 MB** và hơn **560.000 bản ghi**:

| Tên File | Dung lượng | Số dòng | Số cột | Vai trò trong DW | Mô tả nội dung |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `ecommerce_sales_customer_analytics_150k.csv` | ~46.5 MB | **138,116** | 46 | **Fact Table** (`fact_orders`) | Dữ liệu giao dịch đơn hàng: Ngày giờ, trạng thái đơn, kênh bán hàng, chi phí, doanh thu, lợi nhuận, chiết khấu, đánh giá (`customer_review`), phương thức thanh toán, vận chuyển,... |
| `order_items.csv` | ~35.8 MB | **397,569** | 12 | **Fact Table** (`fact_order_items`) | Chi tiết từng sản phẩm trong mỗi đơn hàng (Line Items): `order_id`, `product_id`, số lượng, đơn giá, chiết khấu, thuế, lợi nhuận. |
| `customer_master.csv` | ~2.1 MB | **25,000** | 11 | **Dimension Table** (`dim_customer`) | Hồ sơ khách hàng: ID, tên, tuổi, giới tính, phân khúc, địa phương (City, State, Country, Region), CAC. |
| `product_catalog.csv` | ~150 KB | **1,175** | 9 | **Dimension Table** (`dim_product`) | Danh mục sản phẩm: ID, tên, danh mục lớn/nhỏ, thương hiệu, nhà cung cấp, giá bán lẻ, giá vốn, điểm đánh giá. |
| `dataset_statistics.csv` | 284 B | 1 | 11 | **Metadata / KPIs** | Tóm tắt các chỉ số KPIs kinh doanh giai đoạn 2021–2025: Doanh thu \$177M, Lợi nhuận \$76M, AOV \$1,282.50,... |

### Đánh giá tính toàn vẹn và mức độ phù hợp với đề tài:
- **Tính toàn vẹn (Integrity):** 100% `order_id` trong `order_items` khớp với đơn hàng; 100% `customer_id` và `product_id` tồn tại trong bảng danh mục Master.
- **Phù hợp với Apache Hive:** Cấu trúc phân tách rõ ràng thành Fact & Dimension, thời gian đơn hàng trải đều 5 năm (2021–2025, ~27.5k đơn/năm) rất thích hợp cho kỹ thuật **Partitioning** và **Bucketing**.
- **Phù hợp với PySpark MapReduce:** Cột `customer_review` chứa **113,559 đánh giá thực tế** (~600.000 từ), lý tưởng để áp dụng bài toán phân tích độ dài và tần suất từ trong phản hồi của khách hàng (WordCount, WordMean, WordMedian, WordStdDev).
- **Kích thước lý tưởng:** ~86.5 MB đủ lớn để chia block trên HDFS và kích hoạt nhiều task YARN trên cụm đa máy qua Tailscale VPN mà không làm nghẽn mạng hay tràn bộ nhớ máy cá nhân.

---

## 2. Kiến Trúc Data Warehouse & Thiết Kế Schema

Hệ thống áp dụng **Mô hình 2 lớp (Layered Architecture)** theo chuẩn thực tế doanh nghiệp:

```
[Local CSV Data]
       │
       ▼ (Đẩy vào HDFS bằng `hdfs dfs -put` theo từng thư mục riêng)
[Lớp 1: Staging Layer (Bronze)]
  - Các bảng External Table (staging_customers, staging_products, staging_orders, staging_order_items)
  - Lưu trữ TextFile (CSV thô), dùng OpenCSVSerde để xử lý dấu phẩy văn bản
       │
       ▼ (ETL, ép kiểu dữ liệu chuẩn, nén cột ORC, Partitioning & Bucketing)
[Lớp 2: Core Data Warehouse (Silver/Gold) - Star Schema]
  - dim_customer (ORC)
  - dim_product (ORC)
  - fact_orders (ORC, PARTITIONED BY (order_year INT))
  - fact_order_items (ORC, CLUSTERED BY (order_id) INTO 8 BUCKETS)
```

---

## 3. Quy Trình Triển Khai & Nạp Dữ Liệu Từng Bước

### Bước 1: Tạo thư mục và nạp file lên HDFS

> [!IMPORTANT]
> Trong Hive, một bảng External trỏ đến một **thư mục** trên HDFS chứ không trỏ đến một file. Do đó, **mỗi bảng bắt buộc phải có một thư mục riêng biệt** và dùng **đường dẫn tuyệt đối (bắt đầu bằng `/`)**.

Chạy trên terminal của Master node:
```bash
# 1. Tạo cây thư mục riêng biệt trên HDFS (lưu ý có dấu / ở đầu)
hdfs dfs -mkdir -p /ecommerce_dw/staging/customers
hdfs dfs -mkdir -p /ecommerce_dw/staging/products
hdfs dfs -mkdir -p /ecommerce_dw/staging/order_items
hdfs dfs -mkdir -p /ecommerce_dw/staging/raw_orders

# 2. Đẩy file CSV từ máy local lên HDFS
hdfs dfs -put test_bigdata/data/customer_master.csv /ecommerce_dw/staging/customers/
hdfs dfs -put test_bigdata/data/product_catalog.csv /ecommerce_dw/staging/products/
hdfs dfs -put test_bigdata/data/order_items.csv /ecommerce_dw/staging/order_items/
hdfs dfs -put test_bigdata/data/ecommerce_sales_customer_analytics_150k.csv /ecommerce_dw/staging/raw_orders/

# 3. Kiểm tra file trên HDFS
hdfs dfs -ls -R /ecommerce_dw/staging
```

---

### Bước 2: Tạo Database và Lớp Staging trong Hive

Khởi tạo các bảng `EXTERNAL TABLE`. Riêng bảng `staging_orders` dùng `OpenCSVSerde` để không bị lệch cột do có dấu phẩy trong nội dung review:

```sql
CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE ecommerce_dw;

-- 1. STAGING CUSTOMERS
CREATE EXTERNAL TABLE IF NOT EXISTS staging_customers (
    customer_id STRING,
    customer_name STRING,
    customer_age STRING,
    gender STRING,
    customer_segment STRING,
    customer_city STRING,
    customer_state STRING,
    customer_country STRING,
    region STRING,
    customer_postal_code STRING,
    customer_acquisition_cost STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/ecommerce_dw/staging/customers'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. STAGING PRODUCTS
CREATE EXTERNAL TABLE IF NOT EXISTS staging_products (
    product_id STRING,
    product_name STRING,
    product_category STRING,
    product_subcategory STRING,
    brand STRING,
    supplier STRING,
    unit_price STRING,
    product_cost STRING,
    product_rating STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/ecommerce_dw/staging/products'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 3. STAGING ORDER ITEMS
CREATE EXTERNAL TABLE IF NOT EXISTS staging_order_items (
    order_id STRING,
    product_id STRING,
    quantity STRING,
    unit_price STRING,
    discount_percentage STRING,
    discount_amount STRING,
    gross_sales STRING,
    tax_amount STRING,
    shipping_cost STRING,
    net_sales STRING,
    product_cost STRING,
    profit STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/ecommerce_dw/staging/order_items'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 4. STAGING ORDERS (OpenCSVSerde xử lý dấu phẩy trong customer_review)
CREATE EXTERNAL TABLE IF NOT EXISTS staging_orders (
    order_id STRING,
    order_date STRING,
    order_time STRING,
    order_status STRING,
    sales_channel STRING,
    customer_id STRING,
    customer_name STRING,
    customer_age STRING,
    gender STRING,
    customer_segment STRING,
    customer_type STRING,
    customer_city STRING,
    customer_state STRING,
    customer_country STRING,
    region STRING,
    customer_postal_code STRING,
    payment_method STRING,
    payment_status STRING,
    currency STRING,
    shipping_method STRING,
    warehouse STRING,
    delivery_days STRING,
    estimated_delivery_days STRING,
    delivery_status STRING,
    return_status STRING,
    return_reason STRING,
    customer_rating STRING,
    review_sentiment STRING,
    customer_review STRING,
    marketing_channel STRING,
    campaign_name STRING,
    coupon_code STRING,
    loyalty_points_earned STRING,
    loyalty_points_redeemed STRING,
    quantity STRING,
    gross_sales STRING,
    discount_amount STRING,
    tax_amount STRING,
    shipping_cost STRING,
    net_sales STRING,
    product_cost STRING,
    profit STRING,
    profit_margin_percentage STRING,
    customer_lifetime_value STRING,
    is_repeat_customer STRING,
    customer_order_count STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/ecommerce_dw/staging/raw_orders'
TBLPROPERTIES ("skip.header.line.count"="1");
```

---

### Bước 3: Tạo Lớp Core Data Warehouse (ORC + Partitioning + Bucketing)

Chuyển đổi kiểu dữ liệu, tối ưu hóa lưu trữ với định dạng nén **ORC**, áp dụng **Dynamic Partitioning** theo `order_year` cho `fact_orders` và **Bucketing** cho `fact_order_items`:

```sql
USE ecommerce_dw;

-- Bật tính năng Partitioning động và Bucketing
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.enforce.bucketing = true;

-- 1. DIM_CUSTOMER
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id STRING,
    customer_name STRING,
    customer_age INT,
    gender STRING,
    customer_segment STRING,
    customer_city STRING,
    customer_state STRING,
    customer_country STRING,
    region STRING,
    customer_postal_code STRING,
    customer_acquisition_cost DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE dim_customer
SELECT 
    customer_id, customer_name, CAST(customer_age AS INT), gender,
    customer_segment, customer_city, customer_state, customer_country,
    region, customer_postal_code, CAST(customer_acquisition_cost AS DOUBLE)
FROM staging_customers;

-- 2. DIM_PRODUCT
CREATE TABLE IF NOT EXISTS dim_product (
    product_id STRING,
    product_name STRING,
    product_category STRING,
    product_subcategory STRING,
    brand STRING,
    supplier STRING,
    unit_price DOUBLE,
    product_cost DOUBLE,
    product_rating DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE dim_product
SELECT 
    product_id, product_name, product_category, product_subcategory,
    brand, supplier, CAST(unit_price AS DOUBLE), CAST(product_cost AS DOUBLE),
    CAST(product_rating AS DOUBLE)
FROM staging_products;

-- 3. FACT_ORDERS (Partitioned by order_year)
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id STRING,
    order_date DATE,
    order_time STRING,
    order_status STRING,
    sales_channel STRING,
    customer_id STRING,
    payment_method STRING,
    payment_status STRING,
    shipping_method STRING,
    warehouse STRING,
    delivery_days DOUBLE,
    delivery_status STRING,
    return_status STRING,
    customer_rating DOUBLE,
    review_sentiment STRING,
    customer_review STRING,
    marketing_channel STRING,
    campaign_name STRING,
    quantity INT,
    gross_sales DOUBLE,
    discount_amount DOUBLE,
    tax_amount DOUBLE,
    shipping_cost DOUBLE,
    net_sales DOUBLE,
    product_cost DOUBLE,
    profit DOUBLE,
    profit_margin_percentage DOUBLE,
    customer_lifetime_value DOUBLE,
    is_repeat_customer BOOLEAN
)
PARTITIONED BY (order_year INT)
STORED AS ORC;

INSERT OVERWRITE TABLE fact_orders PARTITION (order_year)
SELECT 
    order_id, CAST(order_date AS DATE), order_time, order_status,
    sales_channel, customer_id, payment_method, payment_status,
    shipping_method, warehouse, CAST(delivery_days AS DOUBLE),
    delivery_status, return_status, CAST(customer_rating AS DOUBLE),
    review_sentiment, customer_review, marketing_channel, campaign_name,
    CAST(quantity AS INT), CAST(gross_sales AS DOUBLE), CAST(discount_amount AS DOUBLE),
    CAST(tax_amount AS DOUBLE), CAST(shipping_cost AS DOUBLE), CAST(net_sales AS DOUBLE),
    CAST(product_cost AS DOUBLE), CAST(profit AS DOUBLE),
    CAST(profit_margin_percentage AS DOUBLE), CAST(customer_lifetime_value AS DOUBLE),
    CAST(is_repeat_customer AS BOOLEAN),
    CAST(SUBSTR(order_date, 1, 4) AS INT) AS order_year
FROM staging_orders;

-- 4. FACT_ORDER_ITEMS (Clustered by order_id into 8 buckets)
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DOUBLE,
    discount_percentage DOUBLE,
    discount_amount DOUBLE,
    gross_sales DOUBLE,
    tax_amount DOUBLE,
    shipping_cost DOUBLE,
    net_sales DOUBLE,
    product_cost DOUBLE,
    profit DOUBLE
)
CLUSTERED BY (order_id) INTO 8 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE fact_order_items
SELECT 
    order_id, product_id, CAST(quantity AS INT), CAST(unit_price AS DOUBLE),
    CAST(discount_percentage AS DOUBLE), CAST(discount_amount AS DOUBLE),
    CAST(gross_sales AS DOUBLE), CAST(tax_amount AS DOUBLE),
    CAST(shipping_cost AS DOUBLE), CAST(net_sales AS DOUBLE),
    CAST(product_cost AS DOUBLE), CAST(profit AS DOUBLE)
FROM staging_order_items;
```

---

## 4. Kịch Bản Truy Vấn Phân Tích Demo Theo Phân Công Nhóm

Nhóm 4 thành viên phân công nhiệm vụ truy vấn trên Data Warehouse như sau:

### 1. Master (`anh24162006-master`): Quản trị DW, Partition Pruning & Thống kê Kho
```sql
USE ecommerce_dw;

-- Kiểm tra danh sách phân vùng đã tạo tự động
SHOW PARTITIONS fact_orders;

-- Demo tính năng Partition Pruning: Chỉ quét dữ liệu năm 2024
EXPLAIN SELECT COUNT(*), ROUND(SUM(net_sales), 2) 
FROM fact_orders 
WHERE order_year = 2024;

-- Thống kê tổng số lượng bản ghi của toàn bộ kho dữ liệu
SELECT 'dim_customer' AS tbl_name, count(*) AS total_rows FROM dim_customer
UNION ALL
SELECT 'dim_product', count(*) FROM dim_product
UNION ALL
SELECT 'fact_orders', count(*) FROM fact_orders
UNION ALL
SELECT 'fact_order_items', count(*) FROM fact_order_items;
```

### 2. Slave 1 (`hoc-slave1`): Phân Tích Doanh Thu & Lợi Nhuận Theo Kênh Bán Hàng
```sql
USE ecommerce_dw;

SELECT 
    sales_channel,
    payment_method,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(net_sales), 2) AS total_revenue,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(AVG(profit_margin_percentage), 2) AS avg_profit_margin
FROM fact_orders
WHERE order_status = 'Completed'
GROUP BY sales_channel, payment_method
ORDER BY total_revenue DESC;
```

### 3. Slave 2 (`nguyen-slave2`): Phân Tích Khách Hàng, Phân Khúc & CLV
```sql
USE ecommerce_dw;

SELECT 
    c.customer_segment,
    c.customer_country,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(AVG(o.net_sales), 2) AS avg_order_value,
    ROUND(AVG(o.customer_lifetime_value), 2) AS avg_clv
FROM fact_orders o
JOIN dim_customer c ON o.customer_id = c.customer_id
WHERE o.order_year = 2025
GROUP BY c.customer_segment, c.customer_country
ORDER BY total_orders DESC
LIMIT 10;
```

### 4. Slave 3 (`anh24133004-slave3`): Phân Tích Sản Phẩm & Chuỗi Cung Ứng (Multi-Table JOIN)
```sql
USE ecommerce_dw;

-- JOIN 3 bảng: fact_order_items + dim_product + fact_orders
SELECT 
    p.product_category,
    p.brand,
    SUM(oi.quantity) AS total_units_sold,
    ROUND(SUM(oi.gross_sales), 2) AS gross_revenue,
    ROUND(SUM(oi.profit), 2) AS net_profit
FROM fact_order_items oi
JOIN dim_product p ON oi.product_id = p.product_id
JOIN fact_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'Completed'
GROUP BY p.product_category, p.brand
ORDER BY net_profit DESC
LIMIT 10;
```

---

## 5. Tích Hợp Dữ Liệu Với 4 Bài Toán PySpark MapReduce

Trích xuất cột `customer_review` trong bảng `staging_orders` thành file text trên HDFS để làm đầu vào thực tế cho 4 bài toán:

```bash
# Trích xuất review thực tế sang HDFS
hive -e "USE ecommerce_dw; SELECT customer_review FROM staging_orders WHERE customer_review IS NOT NULL AND customer_review != ''" \
    | hdfs dfs -put - /test_bigdata/input/customer_reviews.txt
```

Cập nhật đường dẫn trong các file PySpark (`01_master_word_count.py`, `02_slave1_hoc_word_mean.py`,...):
```python
lines = sc.textFile("hdfs://anh24162006-master:9000/test_bigdata/input/customer_reviews.txt")
```
Ý nghĩa đề tài: **Phân tích tần suất và phân bố độ dài từ vựng trong phản hồi của khách hàng (Customer Feedback Text Analysis)** bằng mô hình PySpark RDD MapReduce.

---

## 6. Hướng Dẫn Kết Nối DBeaver Từ Windows Host

1. Khởi động HiveServer2 trên Master:
   ```bash
   nohup hive --service hiveserver2 > ~/hiveserver2.log 2>&1 &
   ```
2. Đảm bảo port `10000` đang lắng nghe:
   ```bash
   netstat -tlpn | grep 10000
   ```
3. Cấu hình kết nối trên DBeaver (Windows):
   - **Connection Type:** Apache Hive
   - **Host:** `anh24162006-master` (hoặc `localhost` / `127.0.0.1` nếu dùng Docker port mapping)
   - **Port:** `10000`
   - **Database/Schema:** `ecommerce_dw`
   - **Username:** `hadoopanh24162006` *(bắt buộc nhập, không được để trống)*
   - **Password:** Để trống
   - Nhấn **Test Connection** -> Khi hiện **Connected** màu xanh, nhấn **Finish**.

---

## 7. Bản Chất Kiến Trúc: Hive Metastore vs HiveServer2

| Tiêu chí | Hive Metastore (`metastore`) | HiveServer2 (`hiveserver2`) |
| :--- | :--- | :--- |
| **Bản chất** | Máy chủ lưu trữ và quản lý siêu dữ liệu (Metadata Server). | Cổng tiếp nhận và điều phối truy vấn SQL (Query Gateway). |
| **Port mặc định** | `9083` | `10000` |
| **Giao thức** | Thrift RPC nội bộ (API dạng `get_table`, `get_partition`). | Thrift JDBC/ODBC (tiếp nhận cú pháp câu lệnh SQL chuẩn). |
| **Ai kết nối vào?** | HiveServer2, Apache Spark (PySpark), Presto/Trino. | DBeaver, Beeline, Tableau, ứng dụng BI, người dùng viết SQL. |
| **Có thể kết nối DBeaver vào port 9083 không?** | **KHÔNG THỂ**. Port 9083 không hỗ trợ JDBC SQL, không có Engine dịch SQL ra MapReduce. | **ĐƯỢC**. Đây chính là cổng duy nhất cho DBeaver. |

> [!NOTE]
> Khi sử dụng **Embedded Derby** (`metastore_db`), **KHÔNG ĐƯỢC CHẠY CẢ 2 DỊCH VỤ CÙNG LÚC**. HiveServer2 đã tích hợp sẵn Metastore bên trong. Chỉ cần khởi động duy nhất `hiveserver2`.

---

## 8. Nhật Ký Xử Lý Lỗi Kỹ Thuật (Troubleshooting Guide)

### Lỗi 1: Đường dẫn tương đối vs tuyệt đối trên HDFS
- **Hiện tượng:** Chạy `hdfs dfs -put ... /ecommerce_dw/staging/customers/` bị báo lỗi `No such file or directory`.
- **Nguyên nhân:** Trước đó tạo thư mục bằng lệnh tương đối `hdfs dfs -mkdir ecommerce_dw/...` (không có dấu `/` ở đầu) khiến thư mục bị tạo ở `/user/hadoopanh24162006/...`.
- **Khắc phục:** Luôn sử dụng tùy chọn `-p` và có dấu `/` ở đầu:
  ```bash
  hdfs dfs -mkdir -p /ecommerce_dw/staging/customers
  ```

---

### Lỗi 2: DBeaver báo lỗi `TTransportException` khi kết nối
- **Hiện tượng:** DBeaver không thể kết nối tới port 10000, hiện thông báo `Could not open client transport with JDBC Uri... null TTransportException`.
- **Nguyên nhân:** 
  1. Do chạy đồng thời cả `hive --service metastore` và `hive --service hiveserver2`. Vì dùng Embedded Derby nên `metastore` chiếm giữ lock của database, `hiveserver2` bị treo ở vòng lặp HikariPool và chưa mở port 10000.
  2. Trường `Username` trên DBeaver bị để trống.
- **Khắc phục:** 
  1. Kill tiến trình `metastore` bị chiếm quyền và xóa file `.lck` của Derby:
     ```bash
     kill -9 <PID_METASTORE>
     rm -f /home/hadoopanh24162006/hive/metastore_db/*.lck
     ```
  2. Chỉ chạy duy nhất `hiveserver2`:
     ```bash
     nohup hive --service hiveserver2 > ~/hiveserver2.log 2>&1 &
     ```
  3. Điền đúng Username `hadoopanh24162006` trên DBeaver.

---

### Lỗi 3: Xem được Schema nhưng mở tab Data bảng Staging bị `OutOfMemoryError: Java heap space`
- **Hiện tượng:** DBeaver mở xem cấu trúc bảng thì được, nhưng khi bấm sang tab **Data** của bảng thô `staging_orders` thì bị ngắt kết nối và báo lỗi `SQL Error [08S01]: org.apache.thrift.transport.TTransportException`.
- **Nguyên nhân cốt lõi:**
  - Bảng `staging_orders` là file CSV thô có tới 46 cột và 138,116 dòng (46.5 MB). 
  - HiveServer2 đang chạy ở mức cấu hình RAM mặc định **256 MB** (`-Xmx256m`).
  - Mặc định Hive bật thuộc tính **`hive.fetch.task.conversion = more`**: Khi gặp các truy vấn đơn giản như `SELECT * FROM table`, HiveServer2 tự cho rằng truy vấn này nhẹ nên **tự nạp toàn bộ file từ HDFS vào RAM của chính nó** để trả kết quả nhanh, không cần gọi YARN/MapReduce.
  - Tuy nhiên, việc parse 46.5 MB dữ liệu CSV thô với 46 cột chuỗi (String) tạo ra hàng triệu đối tượng Java trong Heap, khiến bộ nhớ 256 MB bị tràn (`OutOfMemoryError: Java heap space`) ngay lập tức, ngắt kết nối Thrift tới DBeaver.

---

### Giải Pháp & Chi Tiết Cấu Hình: `hive.fetch.task.conversion = none`

Để giữ nguyên mức **256 MB RAM mặc định** mà không bao giờ bị lỗi tràn bộ nhớ, giải pháp chuẩn nhất là cấu hình bắt buộc Hive phải giao tác vụ quét bảng cho **YARN MapReduce** xử lý phân tán:

#### 1. Phân biệt các giá trị của `hive.fetch.task.conversion`:
- **`more` (Mặc định):** HiveServer2 tự đọc file vào RAM của mình đối với hầu hết các truy vấn `SELECT`, `FILTER`, `LIMIT`. Dễ gây OOM nếu bảng thô lớn và RAM HiveServer2 nhỏ.
- **`minimal`:** Chỉ tự đọc trong RAM với các truy vấn `SELECT *` đơn giản nhất.
- **`none` (Khuyên dùng khi RAM thấp):** Tắt hoàn toàn việc HiveServer2 tự đọc file vào RAM. **Mọi câu lệnh `SELECT` đều được chuyển thành Job phân tán trên YARN**. YARN sẽ cấp phát container riêng biệt trên các worker node (Slave) để đọc và chỉ stream dữ liệu trả về, giúp HiveServer2 giữ mức RAM cực kỳ an toàn (< 100 MB).

#### 2. Vị trí cấu hình chuẩn xác:
> [!IMPORTANT]
> - **File đúng:** `config/hive-site.xml` (được mount vào `/home/hadoopanh24162006/hive/conf/hive-site.xml`).
> - **Lưu ý:** Tuyệt đối **KHÔNG** đặt thuộc tính này vào `hdfs-site.xml`. File `hdfs-site.xml` chỉ dành cho NameNode/DataNode quản lý block đĩa cứng, Hadoop sẽ phớt lờ hoàn toàn các thuộc tính `hive.*`.

Thêm đoạn XML sau vào bên trong `<configuration>...</configuration>` của file `config/hive-site.xml`:
```xml
    <property>
        <name>hive.fetch.task.conversion</name>
        <value>none</value>
        <description>Bat buoc moi truy van phai qua MapReduce/YARN de tranh tran bo nho 256MB</description>
    </property>
```

#### 3. Quy trình khởi động lại an toàn (Safe Restart - Giữ nguyên 100% dữ liệu):
Nhiều bạn lo lắng khởi động lại sẽ làm mất các bảng hoặc database đã tạo. Thực tế:
- **Dữ liệu thực tế** được lưu bền vững trên HDFS (trong Docker volume `hadoop_data`).
- **Cấu trúc bảng và metadata** được lưu trong cơ sở dữ liệu Derby trên đĩa cứng container.
- Bạn **KHÔNG CẦN và KHÔNG NÊN restart cụm HDFS**. Bạn chỉ cần khởi động lại dịch vụ **HiveServer2** để nạp cấu hình mới.

Thực hiện lệnh sau trên terminal của node Master:
```bash
# 1. Tắt tiến trình HiveServer2 cũ
pkill -9 -f HiveServer2

# 2. Xóa các file pid và khóa tạm thời (nếu có)
rm -f /home/hadoopanh24162006/hive/conf/hiveserver2.pid /home/hadoopanh24162006/hive/metastore_db/*.lck

# 3. Khởi động lại dịch vụ HiveServer2 chạy ngầm với cấu hình 256MB mặc định
su - hadoopanh24162006 -c "nohup hive --service hiveserver2 > ~/hiveserver2.log 2>&1 &"

# 4. Kiểm tra port 10000 đã mở lại chưa (đợi khoảng 10-20 giây)
netstat -tlpn | grep 10000
```
Sau khi port `10000` hiện `LISTEN`, bạn quay lại DBeaver bấm **F5 (Refresh)** là xem dữ liệu an toàn mà không sợ bị sập kết nối.

