# Nghiệm thu đối soát — AD-DE-17

## Phạm vi và điều kiện

SQL trong `src/tasks/` được tách để giai đoạn sớm không tham chiếu bảng chưa tồn tại:

| File | Nội dung |
|---|---|
| `init_reconciliation.sql` | Tạo bảng Delta thường expected_counts/reconciliation, seed số tham chiếu full nếu chưa có |
| `reconcile.sql` | 22 check batch: sáu bronze CSV, split, clock, header/món silver, khóa, payload và orphan |
| `reconcile_g3.sql` | 25 check G3: dim_product, SCD2 mở, stream, view hợp nhất, baskets |
| `reconcile_g4.sql` | 4 check gold fact: số dòng, không fanout, cặp khóa khớp silver và giá thiếu |

Mỗi file chạy nguyên trong cùng phiên SQL, dừng khi bất kỳ assert/statement lỗi.
G3 kiểm 22 check batch đã xanh cùng run/cấu hình trước khi đọc nguồn G3; G4 yêu cầu cả 47 check trước.
Không dùng điều kiện WHERE=false để giả tắt nhánh tham chiếu bảng thiếu.

Nghiệm thu G3 cần AD-DE-14 (#50), AD-DE-15 (#51), AD-ST-07 (#70) và hạ tầng/quyền/warehouse sẵn sàng.
Code trên nhánh dựa #163 có Delta local; dim/baskets/stream trong test là fixture theo hợp đồng các PR
nguồn, không phải kết quả pipeline thật. G4 chỉ chạy sau AD-DE-22 có `gold.fact_order_lines`.

## Cấu hình kỳ vọng độc lập nguồn

Profile chỉ đối soát sáu CSV Instacart. Kiểm tra Azure SQL retail và cửa sổ CDC theo AD-DE-07.
Khi cần đối chiếu replay, chạy `COUNT(*)` trên bảng bronze để xem số dòng thô bên cạnh số khóa
phân biệt. Chỉ số khóa phân biệt và payload đã chọn theo khóa dùng để chặn cổng.

1. Chạy `init_reconciliation.sql` trên warehouse **serverless** `wh-instacart-bi` với tham số
   `catalog` và `ops_schema` dev đã resolve. Schema phải tồn tại; file không cấp grant/tạo schema.
2. Profile `instacart_2017` seed **số tham chiếu chưa xác minh trên Zenodo trong phiên này**:
   orders 3.421.083; prior items 32.434.489; train items 1.384.617; products 49.688;
   aisles 134; departments 21; header prior/train/test 3.214.874 / 131.209 / 75.000.
   Init chỉ INSERT khóa thiếu, không ghi đè số đã cấu hình. Kiểm checksum/manifest nguồn trước dùng.
3. Với mẫu AD-DE-01 hoặc nguồn đầy đủ đã tạo manifest, từ gốc repo:

```bash
python3 scripts/expected_counts_sql.py --manifest data/sample_1pct/_manifest.json \
  --catalog instacart --ops-schema p2_ops --profile sample_1pct > /tmp/expected-counts.sql
```

Chạy SQL vừa in trên warehouse. Script đọc **manifest nguồn**, không đọc bảng bronze/silver, không kết nối
cloud. Nó kiểm chín số nguyên không âm, tổng ba split bằng orders và nguồn không rỗng; chỉ xuất số tổng hợp
và SHA-256 manifest, không đưa user_ids/đường dẫn vào SQL. Không suy số đếm bằng cách nhân tỉ lệ 1% vì danh mục
được giữ toàn bộ. Không lấy số actual rồi chép vào expected để làm check xanh.

`expected_counts` có profile/metric/expected/source/configured_at, một khóa mỗi profile/metric.
Đối soát yêu cầu đúng chín metric, không NULL/âm/trùng. Hai nguồn chuẩn hóa tên bảng món từ
`order_products__prior.csv` / `order_products__train.csv` sang metric một underscore theo tên bronze.

## Chạy theo cổng

Các file đối soát nhận tám tham số STRING bắt buộc, không bọc tên riêng bằng backtick:

| Tham số | Ví dụ | Ý nghĩa |
|---|---|---|
| catalog | instacart | Catalog đã resolve |
| ops_schema | p2_ops | Bảng cấu hình/kết quả |
| bronze_schema | p2_bronze | Nguồn batch |
| silver_schema | p2_silver | Target batch/stream/MV/view |
| gold_schema | p2_gold | Namespace dự kiến G4; batch/G3 không đọc bảng gold |
| profile | sample_1pct | Profile có manifest/số kỳ vọng tương ứng nguồn đang kiểm |
| run_id | Một UUID manual hoặc job run ID | Cùng giá trị cho toàn bộ sequence và repair của lần này |
| check_date | 2026-03-30 | Ngày ISO cố định cho sequence, không lấy lại ngày khi qua nửa đêm |

Sau pipeline nguồn hoàn tất và đứng yên trong lúc kiểm, chạy lần lượt:

1. `reconcile.sql`: có thể chạy trước khi bảng dim/stream/gold tồn tại.
2. `reconcile_g3.sql`: chỉ sau khi stream phát đủ và dim/view/baskets sẵn sàng; không dùng G3 partial làm đầu vào DS.
3. `reconcile_g4.sql`: chỉ khi gold đã triển khai. Chưa có gold thì không chạy file này, không ghi check G4 xanh giả.

MERGE theo `(check_date, check_name)` giữ kết quả **mới nhất trong ngày** của namespace ops.
Run_id/profile/stage/config_hash kèm mỗi hàng ngăn lấy kết quả xanh cũ làm prerequisite.
Hash gồm chín kỳ vọng, provenance và catalog/cả bốn schema; đổi nguồn namespace, profile hoặc manifest
cần chạy lại từ batch. Một ops namespace chỉ đại diện một dataset/profile hiện hành; mẫu và prod dùng
schema riêng, không chạy đồng thời nhiều sequence ghi cùng namespace.

MERGE ghi cả hàng sai rồi **assert_true cuối file làm task fail**; kết quả sai vẫn tồn tại cho điều tra/BI.
Assert yêu cầu đủ inventory (22/47/51), mọi check passed và cùng run/profile/hash, tránh trường hợp
không có dòng hoặc chỉ có kết quả cũ mà vẫn xanh. AD-DE-18 phải truyền tham số thống nhất và chỉ cho
refresh_bi chạy khi stage yêu cầu thành công; PR này chưa khai báo job điều phối.

## Kiểm lỗi, repair và bằng chứng

- Chạy cùng stage hai lần trong ngày: số khóa không đổi; checked_at mới hơn được phép, đó là thời điểm kiểm.
- Chỉ ở dev: đổi tạm expected products trong profile, chạy batch. Phải có hàng failed và task đỏ.
  Khôi phục expected đúng nguồn rồi repair sequence, hàng đó cập nhật về passed, không thêm khóa mới.
- Kiểm một lỗi có cùng count: payload món bị đổi hoặc một item giỏ sai. FULL OUTER JOIN theo khóa và so null-safe phải bắt được,
  không chỉ dựa vào count/distinct. Các check stream đối chiếu header/train payload với bronze train;
  basket đối chiếu header/split và explode(items) với các target món.
- Không sửa bảng do pipeline quản lý trên cloud để diễn tập. Dùng fixture local hoặc nguồn dev hợp lệ rồi
  refresh pipeline theo runbook; không xóa checkpoint/full refresh bronze hay sửa expected để che dữ liệu sai.
- Nếu cấu hình/hash/run sai hoặc bảng/field thiếu, dừng trước ghi check downstream; kiểm schema/input.
  Nếu bảng reconciliation cũ chỉ có sáu cột, cần P1 audit/migration trước dùng; script không DROP/overwrite lịch sử.

Đọc kết quả bằng query ở DE/06 §4, lọc đúng check_date/run_id/profile; không chỉ đếm tất cả hàng xanh trong ngày.
Ghi issue #53: commit, params/manifest hash, nguồn và pipeline/update ID, stage đã chạy, số check/failed,
count/distinct thật, lần chạy lại, thử sai expectation và repair. Không dùng dữ liệu fixture làm bằng chứng
3.421.083 orders / 49.688 products / 131.209 stream hoặc đạt G3 thật.

## Nghiệm thu Batch ngày 19/11/2026

Ngày 2026-11-19, chạy trên warehouse dev với profile `instacart_2017` và schema ops cách ly. Nguồn là snapshot Bronze/Silver sau [update batch](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/4df2ff96-be27-4af6-8e87-e2e876da866e/updates/7d05cb18-cfe9-46e8-9159-812a5ebffe6c).

| Lượt | Statement ID | Thời gian | Kết quả |
|---|---|---:|---|
| Lần một | `01f1e40a-5a63-1d58-abd6-ff69466eecac` | 53,36 giây | MERGE ghi 22 check xanh |
| Lần hai | `01f1e40a-859b-14e5-81f2-d2395fbfb7f4` | 43,85 giây | MERGE vẫn có 22 khóa, tất cả xanh |
| Thử sai | `01f1e40a-c9f6-1ba3-b693-18dde291364d` | 4,62 giây | Assert thất bại. Đã lưu `bronze.products` với `passed=false` (49.689 kỳ vọng, 49.688 thực tế) |
| Sau khi khôi phục | `01f1e40a-d7ff-1cec-b9fe-f4f074b4ed4e` | 42,82 giây | MERGE khôi phục 22 check xanh sau khi trả kỳ vọng về 49.688 |

Thời gian là từ khi gửi từng statement đến khi nhận trạng thái cuối. Hai lần chạy dùng cùng ngày và run ID.
Lượt đo ngày 19/11 chỉ chạy Batch. G4 dùng FULL OUTER JOIN
trên số dòng của từng cặp khóa để kiểm cả khóa bị thiếu lẫn số lần xuất hiện bị lệch.

## Diễn tập G3 trên toàn bộ release 2017

Ngày 26/11/2026, dùng các file SQL của `9a7e5b6` với profile `instacart_2017`,
run ID `ad17-ad22-dev-20261126` và ngày kiểm `2026-11-26`. Kết quả ghi vào
`instacart.23521394_ad17_gates_ops.reconciliation`.

- Bronze là `instacart.23521394_bronze`. Schema `instacart.23521394_ad17_gates_silver`
  giữ view đọc Silver batch dev và lịch sử giá phục hồi của #215.
- Hai view hợp nhất và `baskets` đến từ schema nghiệm thu AD-DE-15. Train đầy đủ
  được ánh xạ theo hợp đồng Silver stream từ Bronze và `order_time_sim`.
  Nguồn đó được mô tả trong [nghiệm thu AD-DE-15](03f_nghiem_thu_baskets.md#kết-quả-nghiệm-thu-dev).

| Stage | Số check đạt | Số khóa phân biệt | Statement |
|---|---:|---:|---|
| Batch | 22/22 | 22 | `01f1e978-c05c-1618-9218-907a7257618a` |
| G3 | 25/25 | 25 | `01f1e978-e7b9-1c78-8590-415f6cc11b59` |

Các check xác nhận 3.421.083 đơn, 49.688 sản phẩm, 131.209 đơn train,
1.384.617 món train và 0 phiên bản giá mở bị trùng trong lượt diễn tập.
Kết quả với Silver stream thật nằm ở lượt nghiệm thu dưới đây.

## Nghiệm thu sau replay AD-ST-07

Ngày `2026-11-26`, chạy Batch → G3 → G4 hai lượt trên warehouse dev với SQL ở
`fdeecae`. Cả ba stage dùng cùng run ID `ad17-replay-20261126-730929c6`, ngày kiểm và profile
`instacart_2017`.

| Tham số schema | Giá trị trong catalog `instacart` |
|---|---|
| `bronze_schema` | `23521394_bronze` |
| `silver_schema` | `23521394_ad17_replay_silver` |
| `gold_schema` | `23521394_ad215_gold` |
| `ops_schema` | `23521394_ad17_replay_ops` |

Các view trong schema Silver nghiệm thu đọc Silver batch dev, lịch sử giá đã phục hồi và
`stream_orders`, `stream_order_items`, `orders`, `order_items`, `baskets` từ
`23521394_ad15_silver`. Statement `01f1e988-47e9-1d0b-8651-fbb837630ce4` xác nhận 131.209 đơn mang mã
`ad-st-07-6b780de-02`, 1.384.617 món stream và 3.346.083 giỏ.
[Xác nhận nguồn AD-DE-15](03f_nghiem_thu_baskets.md#kết-quả-đối-soát-sau-replay).

| Stage | Check đạt mỗi lượt | Statement lượt 1 | Statement lượt 2 |
|---|---:|---|---|
| Batch | 22/22 | `01f1e988-87cc-16be-9abd-7abb5c255b00` | `01f1e988-bab5-119a-8203-0201b47f9dd5` |
| G3 | 25/25 | `01f1e988-a257-1e7f-bfee-cefa4430060c` | `01f1e988-d42c-1bde-b4cc-391117c1b4e7` |
| G4 | 4/4 | `01f1e988-b0c2-1878-a639-a73e3fc64cf7` | `01f1e988-e104-15a7-bf86-e6e15236ad32` |

Hai lượt trong cùng ngày đều có 51 dòng kết quả và 51 khóa `(check_date, check_name)`
phân biệt, cùng một `config_hash`. Lượt hai cập nhật kết quả theo khóa.
Statement chốt số khóa `01f1e988-eb44-1eef-ab62-608c2d13130d` xác nhận đủ 51 check đạt.
Các chỉ số G3 gồm 3.421.083 đơn, 49.688 sản phẩm, 131.209 đơn stream, 1.384.617 món
stream và 0 sản phẩm trùng phiên bản giá mở.

Để kiểm cổng lỗi, tăng riêng kỳ vọng `products` trong schema ops dev từ 49.688 lên
49.689. Batch báo `FAILED` ở assert cuối (`01f1e988-63a7-1428-abed-760e99e5af11`) và bảng lưu
`bronze.products` với `passed=false`, thực tế 49.688 (`01f1e988-82fb-13c2-aebe-48f08794cc2d`).
Đã trả kỳ vọng về 49.688 (`01f1e988-84e1-1b62-869f-4a66d63a24fc`) trước hai lượt Batch → G3 → G4 đạt ở trên.
