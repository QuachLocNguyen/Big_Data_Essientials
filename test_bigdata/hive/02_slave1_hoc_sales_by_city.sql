-- =====================================================================
-- PHẦN CHẠY CỦA SLAVE 1: NGUYỄN ĐỨC HỌC (hoc-slave1)
-- Nhiệm vụ: Thống kê số lượng đơn hàng và tổng doanh thu theo Thành phố
-- =====================================================================

USE test_bigdata_db;

SELECT
    city,
    COUNT(*) AS total_orders,
    ROUND(SUM(total), 2) AS total_sales
FROM sales_big
GROUP BY city;

