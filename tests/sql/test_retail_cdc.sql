-- Regression có thay đổi dữ liệu: chỉ chạy trên database test mới, không chạy trên retail_db.
:On Error exit
IF DB_NAME() NOT LIKE N'retail[_]db[_]test[_]%'
    THROW 51900, N'Chỉ chạy trên database retail_db_test_* mới, dùng riêng cho test.', 1;
IF OBJECT_ID(N'dbo.product_prices') IS NOT NULL OR OBJECT_ID(N'etl.cdc_watermark') IS NOT NULL
    THROW 51900, N'Test cần database trắng; không ghi đè nguồn đang dùng.', 1;
GO
:r src/ingest/sql/01_schema.sql
:r src/ingest/sql/02_enable_cdc.sql
:r src/ingest/sql/03_etl_control.sql
:r src/ingest/sql/01_schema.sql
:r src/ingest/sql/02_enable_cdc.sql
:r src/ingest/sql/03_etl_control.sql

SET NOCOUNT ON;
IF (SELECT COUNT(*) FROM cdc.change_tables WHERE capture_instance IN (N'dbo_product_prices', N'dbo_promotions')) <> 2
    THROW 51901, N'Thiếu capture instance.', 1;
-- SQL Server local lưu cấu hình job ở msdb, Azure SQL ở cdc; test này dùng SQL Server local.
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = 'cleanup' AND retention = 10080)
    THROW 51901, N'Retention không phải 7 ngày.', 1;
INSERT INTO dbo.product_prices(product_id, unit_price, updated_at)
VALUES (1, 1.25, '2015-01-01'), (2, 2.50, '2015-01-01');
INSERT INTO dbo.promotions(promo_id, product_id, promo_type, discount_pct, start_date, end_date, updated_at)
VALUES ('P0001', 1, 'percent_off', 10, '2026-03-01', '2026-03-07', '2015-01-01');
-- Local không bật Agent: scan thủ công để test tất định; Azure SQL dùng scheduler riêng.
EXEC sys.sp_cdc_scan @continuous = 0;
DECLARE @w TABLE (mode varchar(10), from_lsn varchar(22), to_lsn varchar(22));
INSERT INTO @w EXEC etl.usp_get_cdc_window N'dbo_product_prices';
INSERT INTO @w EXEC etl.usp_get_cdc_window N'dbo_promotions';
IF (SELECT COUNT(*) FROM @w) <> 2 OR EXISTS (SELECT 1 FROM @w WHERE mode <> 'snapshot' OR from_lsn IS NOT NULL OR LEN(to_lsn) <> 22)
    THROW 51901, N'Lần đầu phải trả snapshot cho cả hai bảng.', 1;
DECLARE @snapshot varchar(22) = (SELECT TOP (1) to_lsn FROM @w);
EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @snapshot, 'snapshot-prices', 'snapshot', @rows = 2;
EXEC etl.usp_set_cdc_watermark N'dbo_promotions', @snapshot, 'snapshot-promos', 'snapshot', @rows = 1;
EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @snapshot, 'snapshot-prices', 'snapshot', @rows = 2;
IF (SELECT COUNT(*) FROM etl.run_log) <> 2 THROW 51901, N'Retry snapshot nhân log.', 1;
PRINT N'PASS: schema/CDC/control chạy lại; hai snapshot; retry không nhân log';
GO

-- Chạy lại sau khi đã có dữ liệu và mốc: phải giữ nguyên cả hai.
:r src/ingest/sql/01_schema.sql
:r src/ingest/sql/02_enable_cdc.sql
:r src/ingest/sql/03_etl_control.sql
IF (SELECT COUNT(*) FROM dbo.product_prices) <> 2 OR (SELECT COUNT(*) FROM etl.run_log) <> 2
   OR EXISTS (SELECT 1 FROM etl.cdc_watermark WHERE last_lsn IS NULL)
    THROW 51901, N'Chạy lại đã làm mất dữ liệu hoặc watermark.', 1;
GO

DECLARE @w TABLE (mode varchar(10), from_lsn varchar(22), to_lsn varchar(22));
INSERT INTO @w EXEC etl.usp_get_cdc_window N'dbo_product_prices';
IF NOT EXISTS (SELECT 1 FROM @w WHERE mode = 'empty') THROW 51901, N'Không có LSN mới phải empty.', 1;
DECLARE @to varchar(22) = (SELECT to_lsn FROM @w);
EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @to, 'empty-prices', 'empty', @rows = 0;
IF (SELECT last_lsn FROM etl.cdc_watermark WHERE capture_instance = N'dbo_product_prices') <> CONVERT(binary(10), @to, 1)
    THROW 51901, N'Empty làm đổi mốc.', 1;

-- Thay đổi insert/update/delete thật rồi đọc hàm CDC theo cửa sổ Lookup.
UPDATE dbo.product_prices SET unit_price = 1.50, updated_at = '2026-03-01' WHERE product_id = 1;
DELETE FROM dbo.product_prices WHERE product_id = 2;
INSERT INTO dbo.product_prices(product_id, unit_price, updated_at) VALUES (3, 3.75, '2026-03-01');
EXEC sys.sp_cdc_scan @continuous = 0;
DELETE FROM @w;
INSERT INTO @w EXEC etl.usp_get_cdc_window N'dbo_product_prices';
DECLARE @from varchar(22) = (SELECT from_lsn FROM @w);
SET @to = (SELECT to_lsn FROM @w);
IF NOT EXISTS (SELECT 1 FROM @w WHERE mode = 'window')
   OR CONVERT(binary(10), @from, 1) <> sys.fn_cdc_increment_lsn((SELECT last_lsn FROM etl.cdc_watermark WHERE capture_instance = N'dbo_product_prices'))
    THROW 51901, N'Lookup không trả cửa sổ liên tiếp.', 1;
DECLARE @changes TABLE (op int);
INSERT INTO @changes SELECT __$operation FROM cdc.fn_cdc_get_all_changes_dbo_product_prices(CONVERT(binary(10), @from, 1), CONVERT(binary(10), @to, 1), N'all');
IF (SELECT COUNT(*) FROM @changes) <> 3 OR (SELECT COUNT(DISTINCT op) FROM @changes WHERE op IN (1, 2, 4)) <> 3
    THROW 51901, N'Không đọc đủ insert/update/delete.', 1;
DECLARE @gap varchar(22) = CONVERT(varchar(22), sys.fn_cdc_increment_lsn(CONVERT(binary(10), @from, 1)), 1);
BEGIN TRY
    EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @to, 'gap-window', 'window', @gap, 0;
    THROW 51901, N'Đã chấp nhận cửa sổ bỏ khoảng LSN.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51004 THROW;
END CATCH;
DECLARE @future varchar(22) = CONVERT(varchar(22), sys.fn_cdc_increment_lsn(CONVERT(binary(10), @to, 1)), 1);
BEGIN TRY
    EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @future, 'future-window', 'window', @from, 0;
    THROW 51901, N'Đã chấp nhận LSN chưa capture.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51002 THROW;
END CATCH;
EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @to, 'window-prices', 'window', @from, 3;
EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @to, 'window-prices', 'window', @from, 3;
-- Retry của run cũ hơn cũng không được hạ mốc.
DECLARE @old varchar(22) = (SELECT to_lsn FROM etl.run_log WHERE adf_run_id = 'snapshot-prices');
EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @old, 'snapshot-prices', 'snapshot', @rows = 2;
IF (SELECT last_lsn FROM etl.cdc_watermark WHERE capture_instance = N'dbo_product_prices') <> CONVERT(binary(10), @to, 1)
   OR (SELECT COUNT(*) FROM etl.run_log) <> 4
    THROW 51901, N'Retry nhân log hoặc hạ mốc.', 1;

BEGIN TRY
    EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @to, 'stale-window', 'window', @from, 3;
    THROW 51901, N'Đã chấp nhận cửa sổ cũ.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51004 THROW;
END CATCH;
BEGIN TRY
    EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @to, 'window-prices', 'window', @from, 99;
    THROW 51901, N'Đã chấp nhận run ID trùng nhưng nội dung khác.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51004 THROW;
END CATCH;
BEGIN TRY
    EXEC etl.usp_get_cdc_window N'unknown';
    THROW 51901, N'Đã chấp nhận capture instance lạ.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51001 THROW;
END CATCH;
DECLARE @invalid TABLE (lsn varchar(100));
INSERT INTO @invalid VALUES (NULL), ('0x01'), ('0x00000000000000000000'), ('0xGG000000000000000000'), ('0x0000000000000000000100');
WHILE EXISTS (SELECT 1 FROM @invalid)
BEGIN
    DECLARE @bad varchar(100) = (SELECT TOP (1) lsn FROM @invalid);
    BEGIN TRY
        EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @bad, 'bad', 'snapshot';
        THROW 51901, N'Đã chấp nhận LSN sai định dạng.', 1;
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() <> 51003 THROW;
    END CATCH;
    DELETE FROM @invalid WHERE lsn = @bad OR (lsn IS NULL AND @bad IS NULL);
END;
IF (SELECT COUNT(*) FROM etl.run_log) <> 4 THROW 51901, N'Run bị từ chối vẫn ghi log.', 1;
PRINT N'PASS: empty, cửa sổ inclusive liên tiếp, insert/update/delete, retry và từ chối đầu vào/run cũ';
GO

-- Cố tình làm bước ghi log lỗi SAU UPDATE để kiểm rollback nguyên tử.
CREATE TRIGGER etl.test_reject_log ON etl.run_log AFTER INSERT AS
    THROW 51999, N'Lỗi ghi log tự dựng trong test.', 1;
GO
UPDATE dbo.product_prices SET unit_price = 1.60, updated_at = '2026-03-08' WHERE product_id = 1;
EXEC sys.sp_cdc_scan @continuous = 0;
DECLARE @w TABLE (mode varchar(10), from_lsn varchar(22), to_lsn varchar(22));
INSERT INTO @w EXEC etl.usp_get_cdc_window N'dbo_product_prices';
DECLARE @last binary(10) = (SELECT last_lsn FROM etl.cdc_watermark WHERE capture_instance = N'dbo_product_prices');
DECLARE @from varchar(22) = (SELECT from_lsn FROM @w), @to varchar(22) = (SELECT to_lsn FROM @w);
BEGIN TRY
    EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @to, 'rollback-test', 'window', @from, 1;
    THROW 51901, N'Không nhận lỗi trigger.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51999 THROW;
END CATCH;
IF @@TRANCOUNT <> 0 OR (SELECT last_lsn FROM etl.cdc_watermark WHERE capture_instance = N'dbo_product_prices') <> @last
   OR EXISTS (SELECT 1 FROM etl.run_log WHERE adf_run_id = 'rollback-test')
    THROW 51901, N'Watermark/log chưa rollback hết.', 1;
DROP TRIGGER etl.test_reject_log;
PRINT N'PASS: lỗi ghi log rollback cả watermark';

-- Giả lập mốc đã quá retention, không thay đổi metadata CDC hệ thống.
UPDATE etl.cdc_watermark SET last_lsn = 0x00000000000000000001 WHERE capture_instance = N'dbo_product_prices';
DELETE FROM @w;
INSERT INTO @w EXEC etl.usp_get_cdc_window N'dbo_product_prices';
IF NOT EXISTS (SELECT 1 FROM @w WHERE mode = 'snapshot' AND from_lsn IS NULL)
    THROW 51901, N'Mốc quá retention không chuyển snapshot.', 1;
SET @to = (SELECT to_lsn FROM @w);
EXEC etl.usp_set_cdc_watermark N'dbo_product_prices', @to, 'retention-snapshot', 'snapshot', @rows = 2;
PRINT N'PASS: fallback snapshot khi mốc hết retention';
-- Capture instance bị gỡ/không truy cập được phải báo lỗi, không đoán snapshot với LSN 0.
EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'promotions', @capture_instance = N'dbo_promotions';
BEGIN TRY
    EXEC etl.usp_get_cdc_window N'dbo_promotions';
    THROW 51901, N'Đã chấp nhận capture instance không còn tồn tại.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51002 THROW;
END CATCH;
PRINT N'PASS: capture instance chưa sẵn sàng bị từ chối';
GO
PRINT N'PASS: toàn bộ regression CDC';
