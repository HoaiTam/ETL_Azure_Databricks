# ADF pipelines

JSON phần `activities` export từ ADF Studio, mỗi pipeline một file trong `pipelines/`.
Stack `infra/stacks/40-orchestration` đọc các file này bằng `file()` và publish lên factory khi merge
(docs/devops/06, mục 1). Studio chỉ dùng để debug, không publish trực tiếp.
