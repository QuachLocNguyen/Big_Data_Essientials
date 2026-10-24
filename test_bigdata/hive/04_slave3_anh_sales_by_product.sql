-- =====================================================================
-- PHẦN CHẠY CỦA SLAVE 3: NGUYỄN VĂN HOÀNG ANH (anh24133004-slave3)
-- Nhiệm vụ: Thống kê số lượng đơn hàng và tổng doanh thu theo Dòng sản phẩm
-- =====================================================================

USE ecommerce_dw;

SELECT
    p.product_category AS product_line,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    ROUND(SUM(COALESCE(CAST(oi.net_sales AS DOUBLE), 0.0)), 2) AS total_revenue
FROM staging_order_items oi
JOIN staging_products p
    ON oi.product_id = p.product_id
GROUP BY p.product_category
ORDER BY total_revenue DESC;

