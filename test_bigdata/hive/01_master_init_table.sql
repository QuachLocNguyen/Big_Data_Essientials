-- 1. Tạo Database cho dự án
CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE ecommerce_dw;

-- =====================================================================
-- BẢNG 1: STAGING CUSTOMERS (25,000 dòng)
-- =====================================================================
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

-- =====================================================================
-- BẢNG 2: STAGING PRODUCTS (1,175 dòng)
-- =====================================================================
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

-- =====================================================================
-- BẢNG 3: STAGING ORDER ITEMS (397,569 dòng)
-- =====================================================================
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

-- =====================================================================
-- BẢNG 4: STAGING RAW ORDERS (138,116 dòng - 46 cột)
-- Dùng OpenCSVSerde để xử lý an toàn cột customer_review có dấu phẩy
-- =====================================================================
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