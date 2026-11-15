---
name: gate-check
description: Đối chiếu một cổng G0–G5 của dự án (ví dụ "/gate-check G1") với trạng thái repo hiện tại, chỉ ra mục nào đã có code, mục nào còn thiếu, mục nào phải kiểm tra tay trên Azure/Databricks/Power BI.
argument-hint: "G0 | G1 | G2 | G3 | G4 | G5"
---

# /gate-check – cổng đã sẵn sàng chưa

## Nguồn checklist

| Cổng | Lấy từ |
|---|---|
| G0 | `docs/devops/01` §4, `docs/devops/spike_results.md` |
| G1 | `docs/devops/05` §5, `docs/devops/06` §6.1, `docs/de/00` §5 |
| G2 | `docs/devops/03` mục 4 (checklist 20-stream), `docs/devops/06` §6.2 |
| G3 | `docs/devops/06` §6.3, `docs/de/03` |
| G4 | `docs/devops/06` §6.4, `docs/de/05`, `docs/de/07` |
| G5 | `docs/devops/06` §7 (tài liệu DS chưa có) |

## Cách làm

1. Đọc checklist của cổng được hỏi và của các cổng đứng trước nó.
2. Xếp từng mục vào một trong ba nhóm:
   - **Đã có trong code:** chỉ ra `file:dòng`. Ví dụ: resource đã viết, mục `[x]` trong `infra/stacks/*/main.tf`, job/pipeline trong `bundles/`, test trong `tests/`.
   - **Còn thiếu code:** nói cần viết gì, ở đâu.
   - **Phải kiểm tra tay** (số dòng đối soát, ảnh chụp RLS, quota…): ghi sẵn lệnh hoặc truy vấn để người dùng tự chạy. Ví dụ: `SELECT count(*) FROM instacart.bronze.orders` phải ra 3.421.083.
3. Kết luận cổng ở trạng thái nào: **chưa mở**, **gần mở**, hay **đủ điều kiện mở**. Kèm 3 việc nên làm tiếp và người phụ trách (P1–P4, theo `docs/devops/00` §4).

Chỉ đọc. Không chạy lệnh cần đăng nhập Azure hay Databricks.
