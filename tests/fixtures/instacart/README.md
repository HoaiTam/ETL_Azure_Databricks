# Fixture Instacart cho kiểm tra local

Dữ liệu **tự dựng**, không phải mẫu 1% từ bộ Instacart thật. Giữ schema của sáu CSV
để kiểm tra script lấy mẫu mà không cần tải dữ liệu lớn hoặc đăng nhập Azure.

- 6 khách hàng, mỗi khách có 2 đơn prior và một đơn cuối.
- Khách 1, 3, 5 có đơn train; khách 2, 4, 6 có đơn test không có món hàng.
- Mỗi đơn prior/train có 2 món, đủ kiểm tra script giữ nguyên cả giỏ và lịch sử.
- Đơn đầu có days_since_prior_order rỗng; tên sản phẩm có dấu phẩy và dấu ngoặc kép.
- Danh mục giữ cả sản phẩm không xuất hiện trong mẫu đã chọn.

Chạy từ gốc repo để thử một mẫu có cả khách train và test:

```bash
python3 scripts/make_sample.py --input-dir tests/fixtures/instacart \
  --output-dir data/sample_fixture --fraction 0.5 --seed 42
```

Tỷ lệ 1% mặc định trên fixture 6 khách được làm tròn lên thành 1 khách. Test còn
dựng bộ 200 khách trong thư mục tạm để kiểm tra mặc định chọn đúng 2 khách (1%).
