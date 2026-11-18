-- Chạy sau 01 + 02. Không reset watermark hoặc log khi chạy lại.
SET NOCOUNT ON;
SET XACT_ABORT ON;
IF DB_NAME() IN (N'master', N'model', N'msdb', N'tempdb')
    THROW 51000, N'Phải kết nối vào retail_db.', 1;
IF (SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) <> 1
    THROW 51000, N'Chạy 02_enable_cdc.sql thành công trước.', 1;
IF SCHEMA_ID(N'etl') IS NULL
    EXEC(N'CREATE SCHEMA etl AUTHORIZATION dbo;');

IF OBJECT_ID(N'etl.cdc_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE etl.cdc_watermark (
        capture_instance sysname NOT NULL CONSTRAINT PK_cdc_watermark PRIMARY KEY,
        last_lsn binary(10) NULL,
        updated_at datetime2(7) NOT NULL CONSTRAINT DF_cdc_watermark_updated DEFAULT SYSUTCDATETIME()
    );
END;
INSERT INTO etl.cdc_watermark(capture_instance)
SELECT ci FROM (VALUES (N'dbo_product_prices'), (N'dbo_promotions')) AS source(ci)
WHERE NOT EXISTS (SELECT 1 FROM etl.cdc_watermark AS w WHERE w.capture_instance = source.ci);

IF OBJECT_ID(N'etl.run_log', N'U') IS NULL
BEGIN
    CREATE TABLE etl.run_log (
        adf_run_id varchar(64) NOT NULL,
        capture_instance sysname NOT NULL,
        mode varchar(10) NOT NULL,
        from_lsn varchar(22) NULL,
        to_lsn varchar(22) NOT NULL,
        rows_copied bigint NULL,
        finished_at datetime2(7) NOT NULL CONSTRAINT DF_run_log_finished DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_run_log PRIMARY KEY (adf_run_id, capture_instance),
        CONSTRAINT CK_run_log_mode CHECK (mode IN ('snapshot', 'window', 'empty')),
        CONSTRAINT CK_run_log_rows CHECK (rows_copied >= 0)
    );
END;
GO

CREATE OR ALTER PROCEDURE etl.usp_get_cdc_window @capture_instance sysname
AS
BEGIN
    SET NOCOUNT ON;
    IF @capture_instance IS NULL OR @capture_instance NOT IN (N'dbo_product_prices', N'dbo_promotions')
       OR NOT EXISTS (SELECT 1 FROM etl.cdc_watermark WHERE capture_instance = @capture_instance)
        THROW 51001, N'Capture instance không được đăng ký.', 1;

    DECLARE @last binary(10) = (SELECT last_lsn FROM etl.cdc_watermark WHERE capture_instance = @capture_instance);
    DECLARE @min binary(10) = sys.fn_cdc_get_min_lsn(@capture_instance);
    DECLARE @max binary(10) = sys.fn_cdc_get_max_lsn();
    -- Min bằng 0 cũng có thể do thiếu quyền; không coi là nguồn rỗng rồi tăng watermark.
    IF @min IS NULL OR @max IS NULL OR @min = 0x00000000000000000000
       OR @max = 0x00000000000000000000 OR @min > @max
        THROW 51002, N'CDC chưa sẵn sàng hoặc thiếu quyền đọc; chờ capture rồi thử lại.', 1;
    IF @last > @max
        THROW 51002, N'Watermark vượt CDC hiện tại; kiểm tra restore/reset trước khi chạy tiếp.', 1;

    DECLARE @from binary(10) = CASE WHEN @last IS NOT NULL THEN sys.fn_cdc_increment_lsn(@last) END;
    DECLARE @mode varchar(10) = CASE
        WHEN @last IS NULL OR @from < @min THEN 'snapshot'
        WHEN @last = @max THEN 'empty'
        ELSE 'window' END;
    -- Hai đầu cửa sổ CDC đều inclusive; lần kế bắt đầu increment(last_lsn).
    SELECT mode = @mode,
           from_lsn = CASE WHEN @mode = 'window' THEN CONVERT(varchar(22), @from, 1) END,
           to_lsn = CONVERT(varchar(22), @max, 1);
END;
GO

-- Chỉ gọi sau khi copy, công bố landing và xóa staging đều thành công.
-- Nhận chuỗi dài để tự kiểm độ dài, tránh varchar(22) cắt cụt đầu vào trước khi kiểm.
CREATE OR ALTER PROCEDURE etl.usp_set_cdc_watermark
    @capture_instance sysname, @to_lsn varchar(8000),
    @adf_run_id varchar(8000), @mode varchar(8000),
    @from_lsn varchar(8000) = NULL, @rows bigint = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @to binary(10) = TRY_CONVERT(binary(10), @to_lsn, 1);
    DECLARE @from binary(10) = TRY_CONVERT(binary(10), @from_lsn, 1);
    IF @to_lsn IS NULL OR DATALENGTH(@to_lsn) <> 22 OR @to IS NULL OR @to = 0x00000000000000000000
       OR (@from_lsn IS NOT NULL AND (DATALENGTH(@from_lsn) <> 22 OR @from IS NULL))
        THROW 51003, N'LSN phải là chuỗi 0x + 20 chữ số hex; to_lsn phải khác 0.', 1;
    IF @adf_run_id IS NULL OR LEN(LTRIM(RTRIM(@adf_run_id))) = 0 OR DATALENGTH(@adf_run_id) > 64
       OR @mode IS NULL OR @mode COLLATE Latin1_General_100_BIN2 NOT IN ('snapshot', 'window', 'empty')
       OR DATALENGTH(@mode) <> LEN(@mode) OR @rows < 0
        THROW 51003, N'Run ID, mode hoặc số dòng không hợp lệ.', 1;
    IF (@mode = 'window' AND (@from IS NULL OR @from > @to))
       OR (@mode <> 'window' AND @from_lsn IS NOT NULL)
       OR (@mode = 'empty' AND @rows <> 0)
        THROW 51003, N'Tham số không khớp loại cửa sổ.', 1;
    IF @capture_instance IS NULL OR @capture_instance NOT IN (N'dbo_product_prices', N'dbo_promotions')
        THROW 51001, N'Capture instance không được đăng ký.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;
        DECLARE @last binary(10);
        -- Khóa theo bảng nguồn: hai run không thể ghi đè watermark của nhau.
        SELECT @last = last_lsn FROM etl.cdc_watermark WITH (UPDLOCK, HOLDLOCK)
        WHERE capture_instance = @capture_instance;
        IF @@ROWCOUNT <> 1
            THROW 51001, N'Capture instance không được đăng ký.', 1;

        -- Retry cùng activity là no-op; không nhân log hoặc hạ mốc sau một run mới hơn.
        IF EXISTS (SELECT 1 FROM etl.run_log WHERE adf_run_id = @adf_run_id AND capture_instance = @capture_instance)
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM etl.run_log
                WHERE adf_run_id = @adf_run_id AND capture_instance = @capture_instance AND mode = @mode
                  AND to_lsn = CONVERT(varchar(22), @to, 1)
                  AND (from_lsn = CONVERT(varchar(22), @from, 1) OR (from_lsn IS NULL AND @from IS NULL))
                  AND (rows_copied = @rows OR (rows_copied IS NULL AND @rows IS NULL)))
                THROW 51004, N'Run ID đã ghi nhận với nội dung khác.', 1;
            COMMIT TRANSACTION;
            RETURN;
        END;

        DECLARE @min binary(10) = sys.fn_cdc_get_min_lsn(@capture_instance);
        DECLARE @max binary(10) = sys.fn_cdc_get_max_lsn();
        IF @min IS NULL OR @max IS NULL OR @min = 0x00000000000000000000 OR @max = 0x00000000000000000000
           OR @min > @max OR @to > @max OR @last > @max
            THROW 51002, N'CDC chưa sẵn sàng hoặc mốc nằm ngoài timeline hiện tại.', 1;
        IF @mode = 'empty'
        BEGIN
            IF @last IS NULL OR @to <> @last
                THROW 51004, N'Nhánh empty chỉ ghi log và giữ nguyên watermark.', 1;
        END
        ELSE
        BEGIN
            IF @to < @min OR @to < @last
                THROW 51004, N'Không hạ watermark hoặc công bố cửa sổ đã hết retention.', 1;
            IF @mode = 'window' AND (@last IS NULL OR @from <> sys.fn_cdc_increment_lsn(@last) OR @from < @min)
                THROW 51004, N'Cửa sổ cũ hoặc có khoảng trống; chạy lại Lookup.', 1;
            IF @mode = 'snapshot' AND @last IS NOT NULL AND sys.fn_cdc_increment_lsn(@last) >= @min
                THROW 51004, N'Snapshot chỉ dùng lần đầu hoặc sau khi mất cửa sổ do retention.', 1;
            UPDATE etl.cdc_watermark SET last_lsn = @to, updated_at = SYSUTCDATETIME()
            WHERE capture_instance = @capture_instance;
        END;
        INSERT INTO etl.run_log(adf_run_id, capture_instance, mode, from_lsn, to_lsn, rows_copied)
        VALUES (@adf_run_id, @capture_instance, @mode, CONVERT(varchar(22), @from, 1), CONVERT(varchar(22), @to, 1), @rows);
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
