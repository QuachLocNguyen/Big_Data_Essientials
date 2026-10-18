-- =====================================================================
-- PHẦN CHẠY CỦA MASTER: LÊ ĐẶNG HOÀNG ANH (hoanganh24162006-master)
-- Nhiệm vụ: Khởi tạo Database, tạo bảng ngoài sales_big và xem dữ liệu mẫu
-- =====================================================================

DROP DATABASE IF EXISTS test_bigdata_db CASCADE;
CREATE DATABASE test_bigdata_db;
SHOW DATABASES;

USE test_bigdata_db;

CREATE EXTERNAL TABLE sales_big (
    invoice_id STRING,
    city STRING,
    payment STRING,
    gender STRING,
    product_line STRING,
    total DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/test_bigdata/hive_sales_input'
TBLPROPERTIES ("skip.header.line.count"="1");

SHOW TABLES;

-- Kiểm tra 10 dòng dữ liệu đầu tiên
SELECT * FROM sales_big LIMIT 10;

