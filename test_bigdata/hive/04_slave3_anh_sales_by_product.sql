-- =====================================================================
-- PHẦN CHẠY CỦA SLAVE 3: NGUYỄN VĂN HOÀNG ANH (anh24133004-slave3)
-- Nhiệm vụ: Thống kê số lượng đơn hàng và tổng doanh thu theo Dòng sản phẩm
-- =====================================================================

USE test_bigdata_db;

SELECT
    product_line,
    COUNT(*) AS total_orders,
    ROUND(SUM(total), 2) AS total_sales
FROM sales_big
GROUP BY product_line;

