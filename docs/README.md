# Lab 03 — Advanced MapReduce & Spark Structured APIs

Bài nộp môn **Nhập môn Phân tích Dữ liệu lớn** — Trường ĐH Khoa học Tự nhiên, ĐHQG-HCM.

Bốn bài toán trên tập dữ liệu `Amazon Sale Report` (đơn hàng Amazon India,
31/03–29/06/2022), cài đặt bằng **Scala** trên **Hadoop MapReduce** và **Apache Spark
DataFrame API**, chạy trong môi trường Docker (Hadoop 3.4.1 + Spark 3.5.3, Java 8).

| Task | Framework | Nội dung | File kết quả |
|------|-----------|----------|--------------|
| 1-1 | MapReduce | Sliding window độ dài động: size bán chạy nhất mỗi (bang, ngày) | `Task_1-1.csv` |
| 1-2 | MapReduce | Median variety (số SKU distinct) theo (bang, tháng), style ≥ XXL | `Task_1-2.csv` |
| 2-1 | Spark DataFrame | % đơn Cancelled+Standard thoả điều kiện promotion + ngưỡng bang | `Task_2-1.parquet` |
| 2-2 | Spark DataFrame | Độ lệch chuẩn theo percentile động P90/P80 mỗi (SKU, tháng) | `Task_2-2.parquet` |

---

## 1. Cấu trúc thư mục

```
src/
├── pom.xml                 Build chung 4 task (Maven, shade plugin)
├── clean.ipynb             Tiền xử lý dữ liệu → asr.csv
├── docker/                 Dockerfile, docker-compose.yml, config Hadoop
├── common/
│   ├── MRCommon.scala      Tầng chung MapReduce (parse CSV, ngày, size)
│   └── SparkCommon.scala   Tầng chung Spark (đọc CSV, chuẩn hoá, đếm promotion)
├── Task_1-1/Task11.scala
├── Task_1-2/Task12.scala
├── Task_2-1/Task21.scala
└── Task_2-2/Task22.scala

docs/
├── Report.pdf              Báo cáo phân tích 4 bài
├── drive_link.txt          Link Google Drive chứa 4 file kết quả
└── README.md               File này
```

---

## 2. Tiền xử lý dữ liệu

Dữ liệu gốc `Amazon Sale Report.csv` được làm sạch bằng `clean.ipynb` trước khi chạy:

- Chuẩn hoá cột `ship-state`: gộp các cách viết trùng của cùng một bang
  (`NEW DELHI`→`DELHI`, `ORISSA`→`ODISHA`, `RJ`→`RAJASTHAN`, `PB`→`PUNJAB`,
  `PONDICHERRY`→`PUDUCHERRY`…), sửa lỗi chính tả, ánh xạ mã bang theo thành phố.
- Loại các dòng thiếu `ship-state` và dòng có state không hợp lệ (`APO`).
- Kết quả: `asr.csv` (128.975 → 128.941 dòng; 36 bang; loại 34 dòng).

Cả 4 task đọc chung `asr.csv` này. Việc chuẩn hoá bang được thực hiện một lần ở
bước tiền xử lý, không lặp lại trong mã Spark/MapReduce.

Đặt `asr.csv` vào thư mục `data/` ở gốc dự án trước khi chạy.

---

## 3. Môi trường

Toàn bộ chạy trong Docker (kế thừa môi trường Hadoop từ Lab 1, bổ sung Spark):

```bash
cd src/docker
docker compose up -d --build      # dựng image Hadoop 3.4.1 + Spark 3.5.3, chạy nền
docker exec -it hcmus-lab3 bash   # vào shell container
```

Thư mục dự án được mount vào `/lab` trong container, nên `data/asr.csv` truy cập
tại `/lab/data/asr.csv`.

---

## 4. Build

```bash
cd /lab/src
mvn clean package
# → target/lab3-1.0.jar (đã đóng gói scala-library + opencsv)
```

---

## 5. Chạy từng task

### Task 2-1 (Spark)
```bash
spark-submit --class Task21 /lab/src/target/lab3-1.0.jar \
  "file:///lab/data/asr.csv" \
  "file:///lab/out/Task_2-1_parquet" \
  "file:///lab/out/Task_2-1.parquet"
```
Ba tham số: đường dẫn CSV đầu vào, thư mục part tạm, file parquet đích. Chương trình
tự gộp part-file thành một file `Task_2-1.parquet` đọc được bằng Pandas/Spark.

### Task 2-2 (Spark)
```bash
spark-submit --class Task22 /lab/src/target/lab3-1.0.jar \
  "file:///lab/data/asr.csv" \
  "file:///lab/out/Task_2-2_parquet" \
  "file:///lab/out/Task_2-2.parquet"
```

### Task 1-1, 1-2 (MapReduce, chạy trên HDFS)
```bash
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /lab/data/asr.csv /data/asr.csv

hadoop jar /lab/src/target/lab3-1.0.jar Task11 /data/asr.csv /out/task11
hadoop jar /lab/src/target/lab3-1.0.jar Task12 /data/asr.csv /out/task12
```

Gộp kết quả MapReduce về một file CSV:
```bash
hdfs dfs -getmerge /out/task11 Task_1-1.csv
hdfs dfs -getmerge /out/task12 Task_1-2.csv
```

---

## 6. Kiểm tra kết quả

```python
import pandas as pd
d = pd.read_parquet('Task_2-1.parquet')
print(d.shape)             # (1434, 4)
print(list(d.columns))     # ['city', 'total_orders', 'qualified_orders', 'pct_cancelled']
```

Bốn file kết quả (`Task_1-1.csv`, `Task_1-2.csv`, `Task_2-1.parquet`,
`Task_2-2.parquet`) được cung cấp qua Google Drive — xem `drive_link.txt`.

---

## 7. Ghi chú

- Toàn bộ lời giải viết bằng Scala, chỉ dùng DataFrame/Dataset API (không dùng
  Spark SQL dạng chuỗi cho các bài Structured APIs).
- Các giả định khi đọc đề (định nghĩa "shipped", chuẩn hoá bang, thang bậc size,
  xử lý giá trị null, cách hiểu các điều kiện mập mờ) được trình bày chi tiết trong
  `Report.pdf`.
