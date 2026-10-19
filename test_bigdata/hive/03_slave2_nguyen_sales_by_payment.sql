-- =====================================================================
-- PHẦN CHẠY CỦA SLAVE 2: QUÁCH LỘC NGUYÊN (nguyen-slave2)
-- Nhiệm vụ: Thống kê số lượng đơn hàng và tổng doanh thu theo Phương thức thanh toán
-- =====================================================================

USE test_bigdata_db;

SELECT
    payment,
    COUNT(*) AS total_orders,
    ROUND(SUM(total), 2) AS total_sales
FROM sales_big
GROUP BY payment;

