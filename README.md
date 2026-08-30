# Lab 03 — Advanced MapReduce & Spark Structured APIs

**Môn:** Nhập môn Phân tích Dữ liệu lớn — HCMUS
**Nhóm:** 4 người · **RepresentativeID:** `23127371`
**Ngôn ngữ:** Scala · **Môi trường:** Docker (Hadoop 3.4.1 + Spark 3.5.3, Java 8)
**Repo:** https://github.com/hungle123-dev/bigdata-lab3

---

## 0. Đề bài tóm tắt

4 bài, mỗi bài 2,5đ (tổng 10). Dữ liệu chung: `Amazon Sale Report.csv` (đơn hàng Amazon
India, 31/03→29/06/2022).

| Task | Framework | Nội dung | Xuất |
|------|-----------|----------|------|
| 1-1 | MapReduce | Sliding window độ dài động — size bán chạy nhất mỗi (bang, ngày) | `Task_1-1.csv` |
| 1-2 | MapReduce | Median variety (số SKU distinct) theo (bang, tháng), style ≥ XXL | `Task_1-2.csv` |
| 2-1 | Spark DataFrame | % đơn Cancelled+Standard thoả điều kiện promotion + ngưỡng bang | `Task_2-1.parquet` |
| 2-2 | Spark DataFrame | Std-dev theo percentile động P90/P80 mỗi (SKU, tháng) | `Task_2-2.parquet` |

**Đề gốc + slide:** xem `Lab03/Lab 3 - MR-Spark.pdf` và `Lab03/Lab3_Slide_ref.pdf`.

> **60% điểm mỗi bài nằm ở REPORT** (phân tích + decomposition + reasoning = 1,5/2,5đ).
> Đọc kỹ `ASSUMPTIONS.md` trước khi code — mọi giả định chốt ở đó, không tự quyết khác.

---

## 1. Cấu trúc repo

```
bigdata-lab3/
├── ASSUMPTIONS.md          ← CHỐT CHUNG: 5 bẫy dữ liệu + 3 chỗ mập mờ. ĐỌC TRƯỚC KHI CODE.
├── README.md               ← file này
├── data/
│   └── asr.csv             ← dữ liệu ĐÃ CLEAN (input chung 4 task, đã commit)
├── out/                    ← kết quả chạy (gitignored, build lại được)
├── src/
│   ├── pom.xml             ← build chung 4 task (Hadoop + Spark + opencsv)
│   ├── clean.ipynb         ← tiền xử lý: chuẩn hoá ship-state → data/asr.csv
│   ├── docker/             ← Dockerfile, docker-compose, config Hadoop
│   ├── common/
│   │   ├── MRCommon.scala      ← tầng chung MapReduce (parse CSV, ngày, size)
│   │   └── SparkCommon.scala   ← tầng chung Spark (đọc CSV, chuẩn hoá, đếm promo)
│   ├── Task_1-1/Task11.scala
│   ├── Task_1-2/Task12.scala
│   ├── Task_2-1/Task21.scala   ← HOÀN CHỈNH + verified
│   └── Task_2-2/Task22.scala
├── report/                 ← LaTeX: main.tex + 5 file con + plans/
├── scripts/
│   └── package-submission.sh   ← đóng gói 23127371.zip đúng cây nộp
├── docs/
│   ├── README.md               ← bản nộp cho giảng viên
│   └── drive_link.txt          ← link Drive chứa 4 file kết quả
└── Lab03/                  ← ĐỀ BÀI GỐC (PDF + slide + CSV gốc). Chỉ để tham khảo,
                              KHÔNG phải code nhóm, KHÔNG nộp.
```

`Lab03/` là nguyên gói đề tải từ Moodle: 2 file PDF (đề + slide) và `Amazon Sale Report.csv`
(dữ liệu gốc, gitignored vì 66MB). Không sửa gì trong đó. Khi cần regenerate dữ liệu sạch,
copy CSV gốc sang `data/` rồi chạy `src/clean.ipynb`.

## 2. Chia việc nhóm

| TV | Task | File sở hữu | Vai phụ |
|----|------|-------------|---------|
| 1 | 1-1 | `common/MRCommon.scala`, `Task_1-1/` | Lead Coder |
| 2 | 1-2 | `Task_1-2/` | Data QC |
| 3 | 2-1 | `common/SparkCommon.scala`, `src/docker/`, `Task_2-1/` | **Report Editor** |
| 4 | 2-2 | `Task_2-2/` | **Submission Manager** |

---

## 3. Dữ liệu — QUAN TRỌNG

`data/asr.csv` là bản **đã tiền xử lý** bằng `src/clean.ipynb`, KHÔNG phải file gốc:
- Chuẩn hoá `ship-state`: gộp cách viết trùng (`NEW DELHI`→`DELHI`, `ORISSA`→`ODISHA`,
  `RJ`→`RAJASTHAN`, `PB`→`PUNJAB`…), sửa lỗi chính tả.
- Loại 33 dòng thiếu `ship-state` **và 1 dòng state không hợp lệ `APO`** (index=45187).
- Kết quả: **128.975 → 128.941 dòng, 36 bang** (34 dòng bị loại tổng cộng).

**Cả 4 task PHẢI dùng chung `data/asr.csv` này.** Không ai dùng file gốc `Amazon Sale Report.csv`
— nếu không, chuẩn hoá state lệch nhau giữa các task → bài ghép rời, mất điểm nhất quán.
State normalization đã làm ở `src/clean.ipynb`, KHÔNG lặp lại trong code Spark/MapReduce.

---

## 4. Chạy (từng bước)

### 4.1. Dựng container (1 lần, ~10 phút build lần đầu)
```bash
cd src/docker
docker compose up -d --build     # dựng image Hadoop + Spark, chạy nền
docker exec -it hcmus-lab3 bash  # vào shell container
```
Repo được mount vào `/lab` trong container (xem `docker-compose.yml`: `../.. → /lab`).
Nên `data/asr.csv` truy cập tại `/lab/data/asr.csv`.

### 4.2. Build jar chung (trong container)
```bash
cd /lab/src && mvn clean package
# → target/lab3-1.0.jar (đã bundle scala-library + opencsv). VERIFIED: BUILD SUCCESS.
```

### 4.3. Chạy Task 2-1 (Spark) — VERIFIED
```bash
spark-submit --class Task21 /lab/src/target/lab3-1.0.jar \
  "file:///lab/data/asr.csv" \
  "file:///lab/out/Task_2-1_parquet" \
  "file:///lab/out/Task_2-1.parquet"
```
3 tham số = input CSV, thư mục part tạm, file parquet đích (khớp default trong `Task21.scala`).
Task tự copy part-file ra `Task_2-1.parquet` (1 file phẳng, không cần `mv` tay).

Kết quả đúng: `cities=1434 totalOrders=6906 totalQualifiedOrders=0 maxQualified=0`.
In thêm `writeStages=11` (số stage của action ghi, dùng cho report).

### 4.4. Chạy Task 2-2 (Spark)
```bash
spark-submit --class Task22 /lab/src/target/lab3-1.0.jar <in.csv> <outDir> <target.parquet>
```

### 4.5. Chạy Task 1-1, 1-2 (MapReduce) — cần HDFS
```bash
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /lab/data/asr.csv /data/asr.csv
hadoop jar /lab/src/target/lab3-1.0.jar Task11 /data/asr.csv /out/task11
hadoop jar /lab/src/target/lab3-1.0.jar Task12 /data/asr.csv /out/task12
```

### 4.6. Gộp kết quả về 1 file đúng tên
Task 2-1 tự gộp. MapReduce ghi ra thư mục part → đổi tên:
```bash
mv out/task11/part-*.csv out/Task_1-1.csv
mv out/task12/part-*.csv out/Task_1-2.csv
```
**Cấm** `getmerge` với parquet (hỏng footer, không đọc được).

---

## 5. Kiểm nhanh trước nộp
```python
import pandas as pd
d = pd.read_parquet('out/Task_2-1.parquet')
print(d.shape)                      # (1434, 4)
print(list(d.columns))              # ['city','total_orders','qualified_orders','pct_cancelled']
print(d['total_orders'].sum())      # 6906
print(d['pct_cancelled'].unique())  # [0.]
```

---

## 6. Đóng gói nộp Moodle

```bash
bash scripts/package-submission.sh   # → 23127371.zip đúng cây đề
```

Cây ZIP nộp (KHÔNG chứa data/out/target):
```
23127371/
├── src/  (Task_1-1..2-2, common, docker, pom.xml, clean.ipynb)
└── docs/ (Report.pdf, drive_link.txt, README.md)
```

4 file kết quả (`Task_1-1.csv`, `Task_1-2.csv`, `Task_2-1.parquet`, `Task_2-2.parquet`) upload
lên **Google Drive** trong folder tên `23127371/`, dán link vào `docs/drive_link.txt`.

**Trước nộp phải có:**
- [ ] `report/Report.pdf` build từ `report/main.tex` (Overleaf / MiKTeX)
- [ ] 4 file kết quả trên Drive + link thật trong `drive_link.txt`
- [ ] Khoá quyền edit Drive sau deadline (sửa sau = huỷ điểm)

---

## 7. Trạng thái

| Task | Code | Report | Verified |
|------|------|--------|----------|
| 2-1 | ✅ | ✅ `03-task21.tex` | ✅ build + run + parquet |
| 1-1 | ⏳ TV1 | ⏳ | — |
| 1-2 | ⏳ TV2 | ⏳ | — |
| 2-2 | ⏳ TV4 | ⏳ | — |
