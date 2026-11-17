# ADF · ingest Instacart (AD-DE-06)

[DE/01 §1–2](../docs/de/01_ingest_landing.md) là thiết kế; các JSON ở đây chuẩn bị cho
AD-OPS-25 (#27) và AD-OPS-26 (#28) triển khai qua Terraform `40-orchestration`.
**Chưa debug/export từ ADF Studio hoặc chạy trên factory thật** vì linked service/dataset G1 chưa được cấp.
JSON được dựng theo API và đã kiểm model SDK; không publish trực tiếp từ agent hay Studio.

## File và ranh giới với P1

| File | Nội dung | Cách dùng |
|---|---|---|
| `instacart_sources.json` | Zenodo record 2654444, release 2017, 6 bảng, header/count/MD5 | Nguồn dựng mapping và tham số; checksum dùng đối chiếu nguồn local |
| `pipelines/pl_ingest_instacart.json` | **Mảng activities** pipeline cha | `activities_json = file(...)` |
| `pipelines/pl_ingest_instacart_table.json` | Mảng activities pipeline con | Tạo resource con trước cha |
| `pipeline_settings/<pipeline>.json` | `concurrency` và `parameters` mặc định | P1 phải nạp cùng activities, không bỏ qua |
| `datasets/*.json` | 3 định nghĩa dataset với linked service theo tên | Hợp đồng cho AD-OPS-25; chưa tạo resource Terraform trong PR này |
| `assets/_SUCCESS` | Template 0 byte | P1 đưa đúng file vào `landing/ops/_SUCCESS` trước lần chạy đầu |

`azurerm_data_factory_pipeline.parameters` là `map(string)`. Vì vậy `tables_json`, `expected_rows`,
`translators_json` là **String chứa JSON**, đọc bằng `json()` trong ADF; không đổi thành object/array
khi dùng provider này. Pipeline con nhận `table_name`, `expected_rows`, `translator_json` cũng là String.
Cha chọn object translator của từng bảng, gửi cả `type`, `mappings`, `typeConversion` sang con;
Copy đặt Expression **trên toàn bộ `translator`**, theo mẫu parameterize mapping của Microsoft.
Không đặt Expression riêng trong mảng `mappings`.

Với từng pipeline, P1 đọc `pipeline_settings/<name>.json` và gán:

- `parameters = jsondecode(file(...)).parameters`
- `concurrency = jsondecode(file(...)).concurrency` (**1** cho cả cha và con)
- `activities_json = file(.../pipelines/<name>.json)`
- `depends_on` cho linked service/dataset, và pipeline cha phụ thuộc resource pipeline con.

Các JSON trong `datasets/` có dạng resource API `{name, properties}`. Khi dùng custom dataset của
Terraform, lấy đúng `type`, `typeProperties`, `schema`, parameter và linked service trong `properties`;
không gán toàn resource dataset cho `activities_json`.

Hợp đồng linked service/dataset:

| Tên | Yêu cầu |
|---|---|
| `ls_zenodo` | **HttpServer**, Anonymous, base URL `https://zenodo.org/`, xác minh chứng chỉ TLS |
| `ls_adls` | **AzureBlobFS**, managed identity của ADF, quyền đọc/ghi/xóa đúng landing |
| `ds_instacart_http_csv` | DelimitedText HTTP; `file_name` → `api/records/2654444/files/<file>/content` |
| `ds_instacart_adls_csv` | DelimitedText ADLS; String params `file_system`, `folder_path`, `file_name` |
| `ds_instacart_adls_binary` | Binary ADLS; cùng 3 params, dùng metadata/công bố/delete/marker |

CSV dùng header, dấu phẩy, quote/escape `"`, UTF-8 không BOM. Dataset HTTP **không đặt rowDelimiter**
để ADF tự nhận LF/CRLF; dataset ghi ADLS chuẩn hóa về LF. Mọi cột được mapping tường minh dạng String
để giữ giá trị giờ có số 0 đầu và ô rỗng; kiểu nghiệp vụ được xử lý ở bronze/silver.

## Luồng chạy và retry

Pipeline cha ForEach **tuần tự** sáu tên bảng, không dùng biến chia sẻ giữa các lượt:

1. Get Metadata `landing/instacart/<bảng>/release=2017/_SUCCESS` với field `exists`.
2. Đã có marker: bỏ qua toàn bộ copy/delete của bảng.
3. Chưa có marker: If gọi pipeline con bằng Execute Pipeline, `waitOnCompletion = true`.

Pipeline con:

1. Kiểm template `landing/ops/_SUCCESS` tồn tại và **0 byte**, sai thì dừng trước HTTP Copy.
2. HTTP DelimitedText → `landing/_staging/instacart/<bảng>/release=2017/<bảng>.csv`.
   Có `TabularTranslator.mappings` tường minh và `quoteAllText = true` theo spike #141.
3. Kiểm **cả `rowsRead` và `rowsCopied` bằng expected_rows**, không tính header.
   Sai thì Fail; thiếu trường output làm expression lỗi, không có đường bỏ qua kiểm đếm để công bố.
4. Binary Copy đúng file staging sang `landing/instacart/<bảng>/release=2017/<bảng>.csv`.
5. Delete đúng thư mục staging của bảng/release.
6. Binary Copy template thành `_SUCCESS` **sau** khi copy và delete thành công.
7. Get Metadata xác nhận marker thực sự tồn tại; thiếu marker thì Fail.

Hai If kiểm template/số dòng nằm ở cấp cao nhất của pipeline con; không lồng If trong If.
Pipeline cha dùng ForEach → If → Execute Pipeline để tuân giới hạn nesting của ADF.

Theo chốt G0 #146, HTTP Copy từ Zenodo retry 3 lần, nghỉ 300 giây để chờ bảo trì/503.
Các Copy nội bộ ADLS/Metadata/Delete khác retry 5 lần, nghỉ 60 giây; timeout activity 2 giờ.
Riêng `get_marker_template` đặt retry 0: thiếu template là lỗi cấu hình cần sửa, không đợi 5 phút rồi thất bại.
Binary Copy dùng `PreserveHierarchy` + `validateDataConsistency: true`, không bỏ qua file lỗi.
HTTP không hỗ trợ data consistency verification nên dùng mapping + đếm dòng, không bật flag đó trên HTTP.

Tên file/thư mục cố định từ bảng và release, không dùng RunId/giờ chạy. Retry trước marker đọc/ghi lại
cùng đường dẫn; lỗi giữ staging hoặc file đích chưa công bố để chạy lại. Nếu đã ghi marker nhưng mất
phản hồi, lần sau bỏ qua an toàn vì dữ liệu đã kiểm và staging đã dọn trước marker.
Không xóa marker/file release để ép chạy lại một bản đã công bố.

**Điều kiện vận hành:** resource phải giữ concurrency 1, không có tiến trình khác ghi cùng release;
bronze/Databricks chỉ được gọi sau khi pipeline cha thành công toàn bộ. Copy không phải giao dịch
storage nguyên tử; không cho Auto Loader chạy trong lúc công bố. Marker là ranh giới commit theo thiết kế.

Landing giữ giá trị/cột/số dòng; quoteAllText chuẩn hóa CSV nên hash landing có thể khác file Zenodo.
Binary publish giữ nguyên byte giữa staging và landing. Catalog MD5 chỉ dùng đối chiếu CSV nguồn,
không phải kiểm MD5 HTTP trong pipeline.

## Nguồn không nén và kiểm dữ liệu thật

Record 2654444 hiện cung cấp đủ sáu **CSV không nén**, đúng spike #141. Không cấu hình ZipDeflate cho `.csv`;
spike ZIP chỉ là phép thử khả năng giải nén, không phải nguồn Instacart chính. Nếu đổi sang file nén/record khác,
cần PR riêng sửa dataset/mapping/catalog và kiểm lại số dòng trước khi công bố.

Đã kiểm ngày 2026-11-17 trên dữ liệu tải thật: MD5 khớp metadata Zenodo, header khớp catalog,
số bản ghi bằng `csv.reader` và `wc -l` (trừ 1 header) đều khớp:

| Bảng | Số dòng dữ liệu |
|---|---:|
| orders | 3.421.083 |
| order_products__prior | 32.434.489 |
| order_products__train | 1.384.617 |
| products | 49.688 |
| aisles | 134 |
| departments | 21 |

Nguồn ở `data/instacart/`, báo cáo local `data/ad_de_06_verification.json`, đều đã gitignore.
Không đưa CSV thật hoặc dữ liệu landing vào Git.

## Dựng, kiểm JSON và debug sau khi có hạ tầng

Từ gốc repo:

```bash
python3 scripts/build_instacart_adf.py
python3 scripts/build_instacart_adf.py --check
uv run --with azure-mgmt-datafactory==9.2.0 python scripts/build_instacart_adf.py --check --sdk-check
uv run pytest tests/test_adf_instacart.py -q
```

Sửa catalog hoặc script dựng, chạy lại để đồng bộ JSON; không sửa tay riêng một bản generated.
`--sdk-check` deserialize/serialize bằng SDK chính thức, không tạo Azure client hoặc gửi request.
Workflow `adf-checks` chạy kiểm này và bộ test local; không dùng secret/OIDC/quyền cloud.
Test local đọc **các JSON đã commit**, diễn giải tập biểu thức/hoạt động đang dùng, thay HTTP/ADLS bằng
fixture/file local để kiểm lỗi giữa chừng, missing count, marker, CSV dấu phẩy/ngoặc kép và retry.
Bộ mô phỏng đánh giá một tập biểu thức đang dùng; chỉ hỗ trợ dependency `Succeeded`, từ chối kiểu khác
thay vì bỏ qua. Test có nguồn CRLF và kiểm contract translator/template.
Đây là mô phỏng giới hạn, không xác nhận runtime ADF hay thông lượng file 578 MB.

Để lấy resource JSON đầy đủ nhập vào trình soạn JSON khi debug (khác mảng activities cho Terraform):

```bash
python3 scripts/build_instacart_adf.py --definition pl_ingest_instacart_table
python3 scripts/build_instacart_adf.py --definition pl_ingest_instacart
```

Sau #27, P1 dựng dataset/linked service và template, rồi #28 đưa cả hai pipeline/settings vào Terraform.
Có thể debug trước với `tables_json = ["departments"]` (giá trị String JSON, expected_rows vẫn là 21),
kiểm đủ rowsRead/rowsCopied, quoting và marker 0 byte. Không publish từ Studio.
Sau khi P1 publish qua workflow, chạy pipeline cha mặc định đủ sáu bảng, chụp output/count/path,
chạy lần hai và xác nhận sáu lượt bỏ qua không Copy/Delete, staging không còn file.
Theo dõi thời gian/throughput/retry của prior 578 MB. Chỉ sau đó đánh dấu nghiệm thu G1.

Tham chiếu chính thức:
[parameterize mapping](https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping#parameterize-mapping),
[nesting ADF](https://learn.microsoft.com/en-us/azure/data-factory/concepts-nested-activities),
[DelimitedText](https://learn.microsoft.com/en-us/azure/data-factory/format-delimited-text),
[consistency verification](https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-data-consistency),
[provider pipeline](https://github.com/hashicorp/terraform-provider-azurerm/blob/v4.0.0/internal/services/datafactory/data_factory_pipeline_resource.go),
[Zenodo record](https://zenodo.org/records/2654444).
