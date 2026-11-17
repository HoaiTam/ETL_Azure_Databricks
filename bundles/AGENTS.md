# bundles/ – quy tắc riêng cho Databricks Asset Bundles

Đọc `docs/devops/05` §4 và `docs/devops/06` §2.

- **Target:**
  - `dev`: mode development, tiền tố `[dev <user>]`, schedule bị tạm dừng.
  - `prod`: chạy bằng SP CI (`run_as` = `${var.ci_sp_id}`). CI đặt biến này qua `BUNDLE_VAR_ci_sp_id`.
  - Chỉ CI được deploy `prod`.
- **Compute:**
  - Job cluster luôn dùng `policy_id: ${var.policy_id}` (tra theo tên policy `instacart-single-node`), kèm `apply_policy_default_values: true` và `custom_tags.owner`.
  - Không tự đặt `node_type_id` hay `num_workers`.
- **Lịch chạy:** job batch không đặt `schedule`, vì ADF `pl_master` gọi job theo job ID (`40-orchestration`). Vì vậy không đổi `name` của job prod khi chưa sửa stack 40.
- **Đường dẫn:** code nằm ở `../src` (đã khai báo trong `sync.paths`), nên đường dẫn trong `resources/*.yml` tính tương đối từ file yml (`../../src/...`).
- **Trạng thái G0:** AD-DE-01 đã bỏ job Python tạm. `resources/jobs_batch.yml` có `jobs: {}`,
  chưa tạo job batch trên workspace. Không deploy khung này lên môi trường đã có job prod.
- **Việc còn treo:** `docs/de` dùng Lakeflow Declarative Pipelines (`instacart_batch`, `instacart_stream`).
  - AD-DE-08 thêm `resources/pipeline_batch.yml`; AD-ST-03 khai báo pipeline stream.
  - AD-DE-18 bổ sung job `instacart-batch-medallion` dùng `pipeline_task` và các task vận hành.
