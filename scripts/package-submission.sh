#!/bin/bash
# Đóng gói bài nộp Moodle: 23127371.zip đúng cây đề (trang 6-7).
# Chỉ lấy src/ (trừ target) + docs/. KHÔNG kèm data/, out/, .git, report .tex.
# Chạy: bash scripts/package-submission.sh
set -e

SID=23127371
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAGE="$ROOT/build_submission/$SID"
ZIP="$ROOT/$SID.zip"

echo "[1] dọn stage cũ"
rm -rf "$ROOT/build_submission" "$ZIP"
mkdir -p "$STAGE/src" "$STAGE/docs"

echo "[2] copy source (trừ target/, out/, .class)"
for d in Task_1-1 Task_1-2 Task_2-1 Task_2-2 common docker; do
  cp -r "$ROOT/src/$d" "$STAGE/src/$d"
done
cp "$ROOT/src/pom.xml" "$STAGE/src/pom.xml"
# clean.ipynb = bước tiền xử lý dữ liệu, kèm để tái lập
cp "$ROOT/src/clean.ipynb" "$STAGE/src/clean.ipynb"
find "$STAGE/src" -type d -name target -exec rm -rf {} + 2>/dev/null || true
find "$STAGE/src" -type f -name "*.class" -delete 2>/dev/null || true

echo "[3] docs: Report.pdf, drive_link.txt, README.md"
[ -f "$ROOT/report/Report.pdf" ] && cp "$ROOT/report/Report.pdf" "$STAGE/docs/Report.pdf" \
  || echo "  ! CẢNH BÁO: chưa có report/Report.pdf — build LaTeX trước khi nộp"
cp "$ROOT/docs/drive_link.txt" "$STAGE/docs/drive_link.txt"
cp "$ROOT/docs/README.md" "$STAGE/docs/README.md"   # bản nộp cho thầy (không phải README gốc của nhóm)

echo "[4] nén $SID.zip"
if command -v zip >/dev/null 2>&1; then
  ( cd "$ROOT/build_submission" && zip -qr "$ZIP" "$SID" )
else
  # Git Bash trên Windows thường không có zip — dùng PowerShell
  powershell.exe -NoProfile -Command \
    "Compress-Archive -Path '$(cygpath -w "$ROOT/build_submission/$SID")' -DestinationPath '$(cygpath -w "$ZIP")' -Force"
fi

echo "[5] xong. Cây trong ZIP:"
if command -v unzip >/dev/null 2>&1; then
  unzip -l "$ZIP" | awk '{print $4}' | grep -v '^$'
else
  ( cd "$ROOT/build_submission" && find "$SID" -type f | sort )
fi
echo
echo "KIỂM TRƯỚC NỘP:"
echo "  - docs/Report.pdf đã build từ report/main.tex?"
echo "  - docs/drive_link.txt đã điền link thật + 4 file trên Drive?"
echo "  - data/ KHÔNG nằm trong ZIP (đúng — data lên Drive, không nộp)"
