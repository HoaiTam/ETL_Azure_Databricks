-- Nhánh riêng, không bật CDC; chỉ dùng database test mới.
:On Error exit
IF DB_NAME() NOT LIKE N'retail[_]db[_]test[_]%'
    THROW 51900, N'Chỉ chạy trên database retail_db_test_* mới, dùng riêng cho test.', 1;
IF OBJECT_ID(N'dbo.product_prices') IS NOT NULL OR OBJECT_ID(N'etl.ts_watermark') IS NOT NULL
    THROW 51900, N'Test cần database trắng.', 1;
GO
:r src/ingest/sql/01_schema.sql
:r src/ingest/sql/04_fallback_watermark.sql
:r src/ingest/sql/01_schema.sql
:r src/ingest/sql/04_fallback_watermark.sql
DECLARE @w TABLE (mode varchar(10), from_ts datetime2(7), to_ts datetime2(7));
INSERT INTO @w EXEC etl.usp_get_ts_window N'dbo_promotions';
IF NOT EXISTS (SELECT 1 FROM @w WHERE mode = 'snapshot' AND from_ts IS NULL AND to_ts = '0001-01-01')
    THROW 51901, N'Snapshot nguồn rỗng không đúng.', 1;
INSERT INTO dbo.product_prices(product_id, unit_price, updated_at) VALUES (1, 1.25, '2015-01-01'), (2, 2.50, '2015-01-01');
DELETE FROM @w;
INSERT INTO @w EXEC etl.usp_get_ts_window N'dbo_product_prices';
IF NOT EXISTS (SELECT 1 FROM @w WHERE mode = 'snapshot' AND to_ts = '2015-01-01')
    THROW 51901, N'Mốc phải theo lịch giả lập, không theo giờ máy.', 1;
EXEC etl.usp_set_ts_watermark N'dbo_product_prices', '2015-01-01', 'snapshot', 'snapshot', @rows = 2;
EXEC etl.usp_set_ts_watermark N'dbo_product_prices', '2015-01-01', 'snapshot', 'snapshot', @rows = 2;
-- Soft delete phải giữ dòng và cập nhật cùng mốc đợt, không DELETE vật lý.
UPDATE dbo.product_prices SET unit_price = 1.50, updated_at = '2026-03-01' WHERE product_id = 1;
UPDATE dbo.product_prices SET is_deleted = 1, updated_at = '2026-03-01' WHERE product_id = 2;
DELETE FROM @w;
INSERT INTO @w EXEC etl.usp_get_ts_window N'dbo_product_prices';
IF NOT EXISTS (SELECT 1 FROM @w WHERE mode = 'window' AND from_ts = '2015-01-01' AND to_ts = '2026-03-01')
    THROW 51901, N'Cửa sổ không bao phủ đợt seed tiếp theo.', 1;
IF (SELECT COUNT(*) FROM dbo.product_prices WHERE updated_at > '2015-01-01' AND updated_at <= '2026-03-01') <> 2
   OR NOT EXISTS (SELECT 1 FROM dbo.product_prices WHERE product_id = 2 AND is_deleted = 1)
    THROW 51901, N'Không lấy đủ update và soft delete.', 1;
EXEC etl.usp_set_ts_watermark N'dbo_product_prices', '2026-03-01', 'window', 'window', '2015-01-01', 2;
EXEC etl.usp_set_ts_watermark N'dbo_product_prices', '2026-03-01', 'window', 'window', '2015-01-01', 2;
EXEC etl.usp_set_ts_watermark N'dbo_product_prices', '2015-01-01', 'snapshot', 'snapshot', @rows = 2;
DELETE FROM @w;
INSERT INTO @w EXEC etl.usp_get_ts_window N'dbo_product_prices';
IF NOT EXISTS (SELECT 1 FROM @w WHERE mode = 'empty' AND to_ts = '2026-03-01')
    THROW 51901, N'Không có đợt mới phải empty.', 1;
EXEC etl.usp_set_ts_watermark N'dbo_product_prices', '2026-03-01', 'empty', 'empty', '2026-03-01', 0;
BEGIN TRY
    EXEC etl.usp_set_ts_watermark N'dbo_product_prices', '2026-03-01', 'stale', 'window', '2015-01-01', 2;
    THROW 51901, N'Đã chấp nhận cửa sổ cũ.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51004 THROW;
END CATCH;
BEGIN TRY
    EXEC etl.usp_set_ts_watermark N'dbo_product_prices', '2026-03-08', 'future', 'window', '2026-03-01', 0;
    THROW 51901, N'Đã chấp nhận mốc vượt nguồn.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51004 THROW;
END CATCH;
IF (SELECT COUNT(*) FROM etl.ts_run_log) <> 3
   OR (SELECT last_ts FROM etl.ts_watermark WHERE capture_instance = N'dbo_product_prices') <> '2026-03-01'
    THROW 51901, N'Retry hoặc run bị từ chối đã thay đổi log/mốc.', 1;
PRINT N'PASS: snapshot/empty/window theo lịch giả lập, soft delete, retry, từ chối mốc cũ/vượt nguồn';
GO
:r src/ingest/sql/01_schema.sql
:r src/ingest/sql/04_fallback_watermark.sql
IF (SELECT COUNT(*) FROM dbo.product_prices) <> 2 OR (SELECT COUNT(*) FROM etl.ts_run_log) <> 3
   OR (SELECT last_ts FROM etl.ts_watermark WHERE capture_instance = N'dbo_product_prices') <> '2026-03-01'
    THROW 51901, N'Chạy lại làm mất dữ liệu fallback.', 1;
PRINT N'PASS: toàn bộ regression fallback';
GO
