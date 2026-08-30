# ASSUMPTIONS — chốt chung cả nhóm (Lab 3)

> **Quy tắc vàng:** mỗi chỗ đề mập mờ → chốt MỘT cách, xuất file theo cách đó,
> report trình bày CẢ HAI cách kèm số đo lệch. Ăn 1,5đ lập luận + giữ 0,175đ correctness.
>
> File này ĐÓNG BĂNG sau họp bước 0. Muốn sửa → báo cả nhóm. Copy thẳng vào Report.

Dataset gốc: `Amazon Sale Report.csv` — **128.975 dòng** (raw, before cleaning), 24 cột, 31/03→29/06/2022 (91 ngày).

> **Tiền xử lý (clean.ipynb) → `data/asr.csv` = CANONICAL INPUT của cả 4 task.**
> - `upper(trim)` + map alias `ship-state` (`NEW DELHI→DELHI`, `ORISSA→ODISHA`, `RJ→RAJASTHAN`, `PB→PUNJAB`…).
> - Loại **33 dòng thiếu ship-state** + **1 dòng state không hợp lệ `APO`** (index=45187; APO không phải bang, KHÔNG đoán map).
> - Kết quả: **128.941 dòng, 36 bang**. Mọi số dưới đây đo trên bản CLEAN này (trừ chỗ ghi rõ "raw").
> - State normalization làm ở tiền xử lý, KHÔNG lặp lại trong Spark/MapReduce.

Checkpoint trên bản clean (128.941 dòng):

| Đại lượng | Giá trị |
|---|---|
| Status == "Shipped" | 77.788 |
| Status contains "shipped" | 109.670 |
| Qty == 0 | 12.801 |
| Amount null | 7.792 |
| bought (shipped & Qty>0) | 109.566 · 36 bang |
| Merchant + Courier=Shipped | 31.871 dòng · 36 ngưỡng bang |
| Cancelled + Standard | 6.906 |
| SKU-month groups | 16.486 · max 426 |

---

## A. Năm cái bẫy dữ liệu — áp cho MỌI task

| # | Bẫy | Chốt | Số đo |
|---|-----|------|-------|
| ① | `Status` có 13 giá trị, 10 cái chứa "shipped" | "shipped" = `lower(Status)` **contains** "shipped" (KHÔNG `== "Shipped"`) | `==`: 77.788 · contains: **109.670** (clean) |
| ② | Tên bang nhiều cách viết | chuẩn hoá ở clean.ipynb (raw 69 cách viết → **36 bang** sau clean) | raw 69→47 upper(trim); +map alias & loại APO → **36** |
| ③ | Ngày format **MM-DD-YY** | parse `MM-dd-yy` (`04-30-22` = 30/04/2022) | — |
| ④ | Size không sort theo alphabet | thang số: XS=1 S=2 M=3 L=4 XL=5 XXL=6 3XL=7 4XL=8 5XL=9 6XL=10; `Free`=0 (loại khỏi so sánh ≥) | alphabet vs thang số lệch gần 2× |
| ⑤ | NULL khắp nơi | `Qty=0`: 12.801 · `Amount` null: 7.792 → dùng `coalesce` tường minh | (clean) |

**Bẫy parser (bài 2-1, 2-2):** cột `promotion-ids` chứa dấu phẩy TRONG dấu ngoặc kép.
Phải dùng CSV parser hiểu quote (Spark: `.option("multiLine",true).option("quote","\"").option("escape","\"")`;
MapReduce: opencsv hoặc parser tự viết). `line.split(",")` là SAI.

## B. Hai chỗ trông giống mà PHẢI khác nhau — Report Editor đừng "sửa cho đồng bộ"

| Chỗ | 1-1 | 1-2 | 2-1 | Lý do |
|-----|-----|-----|-----|-------|
| Thứ tự Size | **alphabet** | **thang số** | — | 1-1 đề ghi rõ tie-break "lexicographically"; 1-2 "at least XXL" cần thang số |
| Chữ "Shipped" | `Status` **contains** | — | `Courier Status` **==** | cột khác nhau: Status 13 giá trị / Courier Status 4 giá trị |

---

## C. Ba quyết định mập mờ — chốt ở họp bước 0

### C1 · Bài 1-1: variance tính trên cột nào?
- **CHỐT: `Amount`.** Đề phân biệt "the *quantity* is non-zero" (cột Qty) vs "variance of the purchased *amount*" (cột Amount). Có cột tên đúng `Amount`.
- Amount có 7.792 dòng null → tính variance chỉ trên đơn Amount không null, NHƯNG vẫn đếm đơn đó vào tần suất (bought định nghĩa bằng Status+Qty).
- Bought: 109.566 dòng, 36 bang. Chỉ MAHARASHTRA (19.103) + KARNATAKA (14.950) dùng cửa sổ 5 ngày.
- Expected output: **3.473 dòng**. Report ghi: nếu hiểu là Qty thì **182/3.473** dòng ra winner khác.

### C2 · Bài 1-2: "style từng bán ≥ XXL" xét ở đâu? ⚠️ NẶNG NHẤT
- **CHỐT: TOÀN CỤC trong file xuất** (style bán XXL ở bất kỳ đâu → đủ điều kiện mọi nơi).
- Lý do: slide xác nhận file đáp án giảng viên làm toàn cục → dễ khớp 0,175đ correctness.
- Câu chữ đề ("within a specific region") nghiêng TRONG NHÓM → report trình bày cả 2.
- Số đo (clean): ≥XXL rows = **34.621**; qualifying styles = **1.103**; output toàn cục = **129** nhóm, trong-nhóm = **122** nhóm; **41/122** nhóm chung lệch median. MAHARASHTRA 2022-04: toàn cục=3.0, trong-nhóm=4.0.
- Median số phần tử chẵn → trung bình 2 phần tử giữa (khớp numpy/Spark).

### C3 · Bài 2-2: "exact percentile" định nghĩa thế nào?
- **CHỐT: nội suy tuyến tính** (chuẩn numpy/`percentile`), đối trọng thật của `percentile_approx`.
- Nếu chọn nearest-rank → trùng approx → report phần (a)(c) rỗng, mất điểm.
- `stddev_pop` (chia N, ddof=0), KHÔNG `stddev` (chia N−1) — lệch 41%.
- Nhóm sau lọc < 2 đơn → sd = 0.0 (`coalesce` null → 0).
- Xuất 4 cột: `sd_p90_exact, sd_p90_approx, sd_p80_exact, sd_p80_approx`.

### C4 · Bài 2-1: đơn vị "đơn" + ngưỡng ngày
- Đếm "đơn" theo **dòng CSV** (không gộp Order ID). Slide xác nhận gộp Order ID cũng ra 0.
- "active period ≥ 2 days" = `last − first ≥ 2` (between = phép trừ) → 185 mã hợp lệ. Report ghi: `≥ 1` → 233 mã.
- **LEFT join** (đơn không KM vẫn ở mẫu số). Mã Amazon vẫn đếm (270/284 mã).
- Số đo (clean): 284 promotions, 185 valid, 270 Amazon-issued; Merchant+Shipped = 31.871 dòng → 36 ngưỡng bang; Cancelled+Standard = 6.906 đơn; candidate city-null = **0** (các đơn thiếu state đã bị loại ở clean); output 1.434 city, total_orders=6.906, qualified=0.
- Đáp số **= 0% mọi thành phố** — đã chứng minh, KHÔNG nới điều kiện để ra số đẹp.

---

## D. Môi trường chung
- Docker image Lab1 (`hcmus-hadoop:3.4.1`) + **thêm Spark 3.5.x** (bin-hadoop3, Scala 2.12, Java 8).
- Ngôn ngữ: **Scala cả 4 bài** (ăn 0,125đ/bài "runnable Scala").
- Benchmark ≥ 5 lần, báo mean + sd (bài 2-2 bắt buộc; 1-1 nếu đo shuffle timing).
- KHÔNG Google Colab. Xuất CSV/Parquet **1 file** đọc được bằng filesystem thường (coalesce(1) + rename part-file).

## E. RepresentativeID
`23127371` — tên ZIP, folder trong ZIP, folder trên Drive phải trùng.
