---
name: pr
description: Soạn và mở Pull Request đúng quy ước dự án - tiêu đề tiếng Anh dạng Conventional Commits, mô tả tiếng Việt theo template với đủ mục Đã làm / Chưa làm. Kiểm bằng scripts/commit_lint.py trước khi tạo. Dùng khi người dùng bảo tạo PR hoặc gõ /pr.
argument-hint: "[base, mặc định main]"
---

# /pr – mở Pull Request đúng quy ước

Chỉ push và tạo PR khi người dùng đã đồng ý.

## Quy trình

1. **Thu thập:**
   - `git status` phải sạch; còn thay đổi thì hỏi có cần `/commit` trước không.
   - `git log --oneline main..HEAD` và `git diff --stat main...HEAD`, đọc diff đầy đủ.
   - Kết quả kiểm tra đã chạy trong phiên (ví dụ `/check`). Chưa chạy thì chạy `/check` cho phần bị đổi.
2. **Kiểm commit trong nhánh:** `python3 scripts/commit_lint.py --range main..HEAD`.
   Có lỗi thì báo người dùng và đề nghị sửa (rebase/reword) trước; không tự viết lại lịch sử đã push.
3. **Tiêu đề (tiếng Anh):** `<type>(<scope>): <subject>`, tóm cả PR. Thường là commit chính của nhánh.
4. **Mô tả (tiếng Việt có dấu)**, viết vào file tạm trong scratchpad, theo đúng các mục của `.github/pull_request_template.md`:
   - `## Tóm tắt`: 1–3 câu, làm gì và vì sao.
   - `## Đã làm`: gạch đầu dòng, **chỉ** việc có trong diff và đã xong.
   - `## Chưa làm / để PR sau`: mọi việc dở, bước checklist chưa tick, kiểm tra bị BỎ QUA (thiếu tflint, chưa có workspace…), giả định chưa xác minh. Không có thì ghi `Không có`. Đây là mục người/AI sau dựa vào để làm tiếp, phải trung thực.
   - `## Cách kiểm tra`: lệnh đã chạy và kết quả thật; ghi "chưa kiểm" cho phần chưa chạy.
   - `## Ảnh hưởng`: tài nguyên và chi phí ước tính, quyền mới, breaking change, việc người khác phải làm.
   - `## Liên quan` và `## Checklist`: giữ như template, chỉ tick mục đã thật sự làm.
   - Kết thúc mô tả bằng dòng attribution của Claude Code nếu hệ thống yêu cầu.
5. **Kiểm:** `python3 scripts/commit_lint.py --title "<tiêu đề>" --pr-body-file <file>`. Sửa tới khi sạch.
6. **Cho người dùng xem** tiêu đề và mô tả, chờ đồng ý.
7. **Tạo:**
   ```bash
   git push -u origin HEAD
   gh pr create --base main --title "<tiêu đề>" --body-file <file>
   ```
8. Báo lại link PR. Không bật auto-merge, không tự merge.
