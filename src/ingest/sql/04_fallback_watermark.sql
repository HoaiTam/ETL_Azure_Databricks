-- Chỉ dùng thay cho 02 + 03 khi spike chọn watermark, không phải CDC log-based.
-- Chạy sau 01, trước khi seed. Seed phải dùng danh sách cột INSERT tường minh.
SET NOCOUNT ON;
SET XACT_ABORT ON;
IF DB_NAME() IN (N'master', N'model', N'msdb', N'tempdb')
    THROW 51000, N'Phải kết nối vào retail_db.', 1;
IF OBJECT_ID(N'dbo.product_prices', N'U') IS NULL OR OBJECT_ID(N'dbo.promotions', N'U') IS NULL
    THROW 51000, N'Chạy 01_schema.sql trước.', 1;
IF (SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) = 1
    THROW 51000, N'Không trộn CDC với nhánh dự phòng; cần kế hoạch chuyển đổi riêng.', 1;
IF COL_LENGTH(N'dbo.product_prices', N'is_deleted') IS NULL
    ALTER TABLE dbo.product_prices ADD is_deleted bit NOT NULL CONSTRAINT DF_prices_deleted DEFAULT 0 WITH VALUES;
IF COL_LENGTH(N'dbo.promotions', N'is_deleted') IS NULL
    ALTER TABLE dbo.promotions ADD is_deleted bit NOT NULL CONSTRAINT DF_promotions_deleted DEFAULT 0 WITH VALUES;
IF SCHEMA_ID(N'etl') IS NULL EXEC(N'CREATE SCHEMA etl AUTHORIZATION dbo;');
IF OBJECT_ID(N'etl.ts_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE etl.ts_watermark (
        capture_instance sysname NOT NULL CONSTRAINT PK_ts_watermark PRIMARY KEY,
        last_ts datetime2(7) NULL,
        updated_at datetime2(7) NOT NULL CONSTRAINT DF_ts_watermark_updated DEFAULT SYSUTCDATETIME()
    );
END;
INSERT INTO etl.ts_watermark(capture_instance)
SELECT ci FROM (VALUES (N'dbo_product_prices'), (N'dbo_promotions')) AS source(ci)
WHERE NOT EXISTS (SELECT 1 FROM etl.ts_watermark AS w WHERE w.capture_instance = source.ci);
IF OBJECT_ID(N'etl.ts_run_log', N'U') IS NULL
BEGIN
    CREATE TABLE etl.ts_run_log (
        adf_run_id varchar(64) NOT NULL,
        capture_instance sysname NOT NULL,
        mode varchar(10) NOT NULL,
        from_ts datetime2(7) NULL,
        to_ts datetime2(7) NOT NULL,
        rows_copied bigint NULL,
        finished_at datetime2(7) NOT NULL CONSTRAINT DF_ts_log_finished DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_ts_run_log PRIMARY KEY (adf_run_id, capture_instance),
        CONSTRAINT CK_ts_log_mode CHECK (mode IN ('snapshot', 'window', 'empty')),
        CONSTRAINT CK_ts_log_rows CHECK (rows_copied >= 0)
    );
END;
GO

CREATE OR ALTER PROCEDURE etl.usp_get_ts_window @capture_instance sysname
AS
BEGIN
    SET NOCOUNT ON;
    IF @capture_instance IS NULL OR @capture_instance NOT IN (N'dbo_product_prices', N'dbo_promotions')
       OR NOT EXISTS (SELECT 1 FROM etl.ts_watermark WHERE capture_instance = @capture_instance)
        THROW 51001, N'Bảng nguồn không được đăng ký.', 1;
    DECLARE @last datetime2(7) = (SELECT last_ts FROM etl.ts_watermark WHERE capture_instance = @capture_instance);
    DECLARE @to datetime2(7);
    -- updated_at dùng lịch giả lập: không lấy giờ máy làm mốc rồi bỏ sót các đợt seed sau.
    IF @capture_instance = N'dbo_product_prices' SELECT @to = MAX(updated_at) FROM dbo.product_prices;
    ELSE SELECT @to = MAX(updated_at) FROM dbo.promotions;
    SET @to = COALESCE(@to, CONVERT(datetime2(7), '0001-01-01'));
    IF @last > @to
        THROW 51004, N'Nguồn lùi thời gian hoặc đã bị hard delete; cần đối soát.', 1;
    SELECT mode = CASE WHEN @last IS NULL THEN 'snapshot' WHEN @last = @to THEN 'empty' ELSE 'window' END,
           from_ts = @last, to_ts = @to;
END;
GO

CREATE OR ALTER PROCEDURE etl.usp_set_ts_watermark
    @capture_instance sysname, @to_ts datetime2(7),
    @adf_run_id varchar(8000), @mode varchar(8000),
    @from_ts datetime2(7) = NULL, @rows bigint = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @capture_instance IS NULL OR @capture_instance NOT IN (N'dbo_product_prices', N'dbo_promotions')
        THROW 51001, N'Bảng nguồn không được đăng ký.', 1;
    IF @to_ts IS NULL OR @adf_run_id IS NULL OR LEN(LTRIM(RTRIM(@adf_run_id))) = 0 OR DATALENGTH(@adf_run_id) > 64
       OR @mode IS NULL OR @mode COLLATE Latin1_General_100_BIN2 NOT IN ('snapshot', 'window', 'empty')
       OR DATALENGTH(@mode) <> LEN(@mode) OR @rows < 0
       OR (@mode = 'snapshot' AND @from_ts IS NOT NULL)
       OR (@mode = 'window' AND (@from_ts IS NULL OR @from_ts >= @to_ts))
       OR (@mode = 'empty' AND (@from_ts IS NULL OR @from_ts <> @to_ts OR @rows <> 0))
        THROW 51003, N'Tham số watermark thời gian không hợp lệ.', 1;
    BEGIN TRY
        BEGIN TRANSACTION;
        DECLARE @last datetime2(7);
        SELECT @last = last_ts FROM etl.ts_watermark WITH (UPDLOCK, HOLDLOCK)
        WHERE capture_instance = @capture_instance;
        IF @@ROWCOUNT <> 1 THROW 51001, N'Bảng nguồn không được đăng ký.', 1;
        IF EXISTS (SELECT 1 FROM etl.ts_run_log WHERE adf_run_id = @adf_run_id AND capture_instance = @capture_instance)
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM etl.ts_run_log WHERE adf_run_id = @adf_run_id AND capture_instance = @capture_instance
                  AND mode = @mode AND to_ts = @to_ts
                  AND (from_ts = @from_ts OR (from_ts IS NULL AND @from_ts IS NULL))
                  AND (rows_copied = @rows OR (rows_copied IS NULL AND @rows IS NULL)))
                THROW 51004, N'Run ID đã ghi nhận với nội dung khác.', 1;
            COMMIT TRANSACTION;
            RETURN;
        END;
        DECLARE @upper datetime2(7);
        IF @capture_instance = N'dbo_product_prices' SELECT @upper = MAX(updated_at) FROM dbo.product_prices;
        ELSE SELECT @upper = MAX(updated_at) FROM dbo.promotions;
        SET @upper = COALESCE(@upper, CONVERT(datetime2(7), '0001-01-01'));
        IF @to_ts > @upper OR @to_ts < @last
           OR (@mode = 'snapshot' AND @last IS NOT NULL)
           OR (@mode IN ('window', 'empty') AND (@last IS NULL OR @from_ts <> @last))
            THROW 51004, N'Mốc cũ, lùi hoặc vượt nguồn; chạy lại Lookup.', 1;
        IF @mode <> 'empty'
            UPDATE etl.ts_watermark SET last_ts = @to_ts, updated_at = SYSUTCDATETIME()
            WHERE capture_instance = @capture_instance;
        INSERT INTO etl.ts_run_log(adf_run_id, capture_instance, mode, from_ts, to_ts, rows_copied)
        VALUES (@adf_run_id, @capture_instance, @mode, @from_ts, @to_ts, @rows);
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
