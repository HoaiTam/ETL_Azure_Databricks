-- AD-DE-04: chạy trong retail_db bằng admin Entra; không tạo/xóa database.
-- Dừng client ngay khi có lỗi (sqlcmd -b). Chạy lại không xóa dữ liệu đã seed.
SET NOCOUNT ON;
SET XACT_ABORT ON;

IF DB_NAME() IN (N'master', N'model', N'msdb', N'tempdb')
    THROW 51000, N'Phải kết nối vào database nguồn retail_db, không dùng database hệ thống.', 1;

IF OBJECT_ID(N'dbo.product_prices', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.product_prices (
        product_id int NOT NULL CONSTRAINT PK_product_prices PRIMARY KEY,
        unit_price decimal(10,2) NOT NULL,
        currency char(3) NOT NULL CONSTRAINT DF_product_prices_currency DEFAULT 'USD',
        updated_at datetime2(7) NOT NULL
    );
END;

IF OBJECT_ID(N'dbo.promotions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.promotions (
        promo_id varchar(10) NOT NULL CONSTRAINT PK_promotions PRIMARY KEY,
        product_id int NOT NULL,
        promo_type varchar(20) NOT NULL,
        discount_pct int NOT NULL,
        start_date date NOT NULL,
        end_date date NOT NULL,
        updated_at datetime2(7) NOT NULL
    );
END;
-- Không tạo FK sang products: danh mục Instacart nằm ở CSV/lakehouse, không ở database này.
GO
