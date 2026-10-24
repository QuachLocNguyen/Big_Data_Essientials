-- =====================================================================
-- PHẦN CHẠY CỦA SLAVE 2: QUÁCH LỘC NGUYÊN - 21139037 (nguyen-slave2)
-- Hệ thống: Apache Hive trên Hadoop Cluster (Tailscale VPN)
-- Database: ecommerce_dw
-- =====================================================================

USE ecommerce_dw;

-- =====================================================================
-- Thống kê đơn hàng và doanh thu theo Phương thức thanh toán
-- Đáp ứng yêu cầu phân công của Slave 2
-- =====================================================================
SELECT 
    payment_method,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(CAST(net_sales AS DOUBLE)), 2) AS total_sales,
    ROUND(AVG(CAST(net_sales AS DOUBLE)), 2) AS avg_order_value
FROM staging_orders
GROUP BY payment_method
ORDER BY total_sales DESC ;