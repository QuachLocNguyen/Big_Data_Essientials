-- =====================================================================
-- PHẦN CHẠY CỦA SLAVE 1: NGUYỄN ĐỨC HỌC (hoc-slave1)
-- Nhiệm vụ: Thống kê số lượng đơn hàng và tổng doanh thu theo Thành phố
-- =====================================================================

SELECT
    customer_city,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(CAST(net_sales AS DOUBLE)) AS total_revenue
FROM ecommerce_dw.staging_orders
GROUP BY customer_city;
