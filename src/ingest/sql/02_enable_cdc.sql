-- Chỉ chọn nhánh này sau spike AD-OPS-06; lỗi tier/quyền phải được báo, không tự hạ tier.
SET NOCOUNT ON;
SET XACT_ABORT ON;
IF DB_NAME() IN (N'master', N'model', N'msdb', N'tempdb')
    THROW 51000, N'Phải kết nối vào retail_db.', 1;
IF OBJECT_ID(N'dbo.product_prices', N'U') IS NULL OR OBJECT_ID(N'dbo.promotions', N'U') IS NULL
    THROW 51000, N'Chạy 01_schema.sql trước.', 1;
IF COL_LENGTH(N'dbo.product_prices', N'is_deleted') IS NOT NULL
   OR COL_LENGTH(N'dbo.promotions', N'is_deleted') IS NOT NULL
    THROW 51000, N'Đã chọn soft delete; không bật lẫn CDC. Cần kế hoạch chuyển đổi riêng.', 1;

IF (SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) = 0
    EXEC sys.sp_cdc_enable_db;
GO

-- Tên capture instance là hợp đồng với ADF, đặt tường minh và kiểm chủ sở hữu khi chạy lại.
IF EXISTS (SELECT 1 FROM cdc.change_tables
           WHERE capture_instance = N'dbo_product_prices' AND source_object_id <> OBJECT_ID(N'dbo.product_prices'))
   OR EXISTS (SELECT 1 FROM cdc.change_tables
              WHERE capture_instance = N'dbo_promotions' AND source_object_id <> OBJECT_ID(N'dbo.promotions'))
    THROW 51000, N'Capture instance đang trỏ sai bảng nguồn.', 1;

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_product_prices')
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo', @source_name = N'product_prices',
        @capture_instance = N'dbo_product_prices', @role_name = NULL, @supports_net_changes = 1;

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_promotions')
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo', @source_name = N'promotions',
        @capture_instance = N'dbo_promotions', @role_name = NULL, @supports_net_changes = 1;

-- 7 ngày = 10.080 phút. Azure SQL dùng scheduler nội bộ, không bật/tạo SQL Agent.
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;
SELECT name, is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID();
SELECT capture_instance, start_lsn FROM cdc.change_tables
WHERE capture_instance IN (N'dbo_product_prices', N'dbo_promotions');
EXEC sys.sp_cdc_help_jobs;
GO
