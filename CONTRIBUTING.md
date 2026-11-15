# Quy ước commit, nhánh và Pull Request

Áp dụng **bắt buộc** cho mọi người và mọi AI agent (Claude Code, Codex, Copilot…). Được kiểm tự động ở 3 lớp:

| Lớp | Kiểm gì | Khi nào |
|---|---|---|
| Hook `commit-msg` (pre-commit) | Commit message | Ngay lúc `git commit` trên máy (sau khi chạy `pre-commit install`) |
| Workflow `pr-lint` | Tiêu đề PR, mọi commit trong PR, mô tả PR | Mỗi lần mở / sửa / push PR. Là status check bắt buộc |
| Thủ công | `python3 scripts/commit_lint.py --message "…"` | Khi muốn thử trước |

Luật nằm trong một chỗ duy nhất: `scripts/commit_lint.py`. Sửa luật thì sửa script, test (`tests/test_commit_lint.py`) và file này.

## Tóm tắt ngôn ngữ

| Phần | Ngôn ngữ |
|---|---|
| Tên nhánh | Tiếng Anh |
| Commit message (dòng đầu và thân) | **Tiếng Anh** |
| Tiêu đề PR | **Tiếng Anh**, cùng định dạng commit |
| Mô tả PR | **Tiếng Việt** có dấu, theo template |
| Review comment, tài liệu `docs/`, comment trong code | Tiếng Việt |

## 1. Commit message

Theo [Conventional Commits 1.0](https://www.conventionalcommits.org/):

```text
<type>(<scope>): <subject>

<body: vì sao thay đổi, tiếng Anh, tùy chọn>

<footer: Closes #12, BREAKING CHANGE: …, Co-Authored-By: …>
```

**type**

| type | Dùng khi |
|---|---|
| `feat` | Thêm tính năng, tài nguyên, bảng, pipeline mới |
| `fix` | Sửa lỗi |
| `docs` | Chỉ sửa tài liệu |
| `refactor` | Đổi cấu trúc code, không đổi hành vi |
| `test` | Thêm hoặc sửa test |
| `perf` | Cải thiện hiệu năng |
| `ci` | Workflow GitHub Actions |
| `build` | Đóng gói, dependency, `pyproject.toml` |
| `chore` | Việc lặt vặt khác: cấu hình agent, `.gitignore`… |
| `style` | Chỉ định dạng |
| `revert` | Hoàn tác commit trước |

**scope** (không bắt buộc, nhưng nên có): `infra`, `de`, `stream`, `bundle`, `adf`, `bi`, `ds`, `ci`, `docs`, `deps`, `agents`.

**subject**
- Động từ mệnh lệnh, chữ thường đầu câu: `add`, `fix`, `remove`, `rename`. Không viết `Added`, `Adds`, `Add`.
- Nói rõ **cái gì, ở đâu**. Không có dấu câu ở cuối. Dòng đầu tối đa 72 ký tự.
- Cấm subject chung chung: `update`, `fix bug`, `wip`, `misc`, `changes`…
- Thay đổi phá tương thích: thêm `!` sau type/scope (`feat(infra)!: …`) và ghi `BREAKING CHANGE:` ở footer.

**Thân** (tùy chọn): giải thích *vì sao*, tiếng Anh, cách dòng đầu một dòng trống, mỗi dòng ≤ 100 ký tự.

**Một commit = một thay đổi logic.** Không gộp Terraform với pipeline dữ liệu trong cùng commit. Commit `fixup!` / `squash!` được phép lúc làm, nhưng phải gộp trước khi merge.

**AI agent** thêm dòng `Co-Authored-By: <tên model> <email>` ở footer.

Ví dụ đúng:

```text
feat(infra): add landing container and lifecycle rule to core storage
fix(de): deduplicate order_items by event_id on rerun
docs(de): document SCD2 rules for dim_product
ci: lint PR titles and commit messages
chore(deps): bump ruff to 0.8.4
```

Ví dụ sai: `update code`, `Fix bug`, `feat: Thêm storage`, `feat(infra): add storage.`, `WIP`.

## 2. Nhánh

`<type>/<vai>-<việc>` bằng tiếng Anh, kebab-case. Ví dụ: `feat/p1-core-storage`, `fix/p2-bronze-dedup`, `docs/p4-powerbi-guide`.
- Không commit thẳng vào `main`; mọi thay đổi đi qua PR.
- Một PR nên nhỏ: một bước trong checklist stack, hoặc một bảng/pipeline.

## 3. Pull Request

**Tiêu đề PR:** tiếng Anh, **cùng định dạng commit**. PR được merge bằng *squash*, nên tiêu đề PR trở thành commit trên `main`.

**Mô tả PR:** tiếng Việt, dùng đúng `.github/pull_request_template.md`. Năm mục sau là **bắt buộc** và không được để trống; nếu không có gì thì ghi `Không có`:

| Mục | Ghi gì |
|---|---|
| `## Tóm tắt` | 1–3 câu: PR làm gì, vì sao |
| `## Đã làm` | Gạch đầu dòng từng việc **đã thật sự làm xong** trong PR |
| `## Chưa làm / để PR sau` | Việc trong phạm vi nhưng **chưa làm**, việc làm dở, giả định chưa kiểm, TODO còn lại. Người sau (hoặc AI sau) đọc mục này để làm tiếp |
| `## Cách kiểm tra` | Lệnh đã chạy và kết quả; bước kiểm tay cho reviewer |
| `## Ảnh hưởng` | Tài nguyên mới, chi phí ước tính, quyền mới, thay đổi phá tương thích, việc người khác phải làm |

Quy tắc trung thực:
- Chỉ ghi vào "Đã làm" những gì có trong diff và đã kiểm. Thứ chưa chạy thử thì ghi rõ "chưa kiểm" ở "Cách kiểm tra".
- Không giấu việc dở: test bỏ qua, bước BỎ QUA vì thiếu công cụ, checklist chưa tick đều phải nằm ở "Chưa làm".

**Tạo PR bằng CLI** (`gh pr create --body` không tự dùng template):

```bash
gh pr create --title "feat(infra): add core storage account" --body-file /path/to/body.md
```

**Merge:** squash merge, cần review của CODEOWNERS và các status check xanh (`pr-checks`, `pr-lint`).

## 4. Cài hook trên máy

```bash
brew install pre-commit
pre-commit install            # cài cả hook pre-commit và commit-msg (xem default_install_hook_types)
```

Không dùng `git commit --no-verify` để lách hook.

## 5. Cấu hình GitHub (P1 làm một lần)

Settings → General → Pull Requests:
- Chỉ bật **Allow squash merging**, default message = **Pull request title and description**.
- Bật **Automatically delete head branches**.

Branch protection của `main`: thêm `pr-lint / title`, `pr-lint / commits`, `pr-lint / body` vào danh sách status check bắt buộc.
