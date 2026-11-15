---
name: commit
description: Soạn commit message tiếng Anh theo Conventional Commits của dự án (CONTRIBUTING.md), kiểm bằng scripts/commit_lint.py rồi mới commit. Dùng mỗi khi người dùng bảo commit hoặc gõ /commit.
argument-hint: "[gợi ý nội dung]  (bỏ trống = tự đọc diff)"
---

# /commit – commit đúng quy ước

Chỉ chạy khi người dùng đã yêu cầu commit.

## Quy trình

1. **Xem thay đổi:** `git status`, `git diff`, `git diff --staged`, `git log --oneline -5`.
   - Không stage file bí mật (`*.auto.tfvars`, `*.tfstate`, `.env`). Thấy thì dừng và báo.
2. **Nhánh:** nếu đang ở `main`, tạo nhánh `<type>/<vai>-<việc>` (tiếng Anh, kebab-case) trước khi commit.
3. **Tách commit:** nếu diff chứa nhiều thay đổi logic khác nhau (ví dụ Terraform và pipeline dữ liệu), đề xuất tách thành nhiều commit, stage theo từng nhóm file.
4. **Soạn message tiếng Anh** theo `CONTRIBUTING.md` §1:
   - `<type>(<scope>): <subject>`; type/scope trong danh sách cho phép; subject mệnh lệnh, chữ thường đầu, ≤ 72 ký tự cả dòng, không dấu chấm cuối, nói rõ cái gì ở đâu.
   - Thân (nếu cần): vì sao thay đổi, tiếng Anh, dòng ≤ 100 ký tự.
   - Footer: `Closes #…` nếu có, `BREAKING CHANGE:` nếu phá tương thích, và dòng `Co-Authored-By:` của model.
5. **Kiểm trước khi commit:**
   `python3 scripts/commit_lint.py --message "<toàn bộ message>"`
   Sai thì sửa message và kiểm lại, không commit khi còn lỗi.
6. **Commit** bằng heredoc để giữ xuống dòng:
   ```bash
   git commit -F - <<'EOF'
   feat(infra): add landing container to core storage

   Co-Authored-By: ...
   EOF
   ```
   Không dùng `--no-verify`. Nếu hook pre-commit báo lỗi: sửa, stage lại và tạo commit **mới** (không `--amend` commit đã push).
7. **Báo lại:** hash, nhánh, message. Không push trừ khi người dùng yêu cầu.
