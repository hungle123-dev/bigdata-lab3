# Lab 03 — Advanced MapReduce & Spark Structured APIs

Bài nộp môn Nhập môn Phân tích Dữ liệu lớn — Trường ĐH Khoa học Tự nhiên, ĐHQG-HCM.

Bốn bài toán trên tập dữ liệu Amazon Sale Report (đơn hàng Amazon India,
31/03–29/06/2022). Cả bốn lời giải viết bằng Scala; hai bài Structured APIs chỉ
dùng DataFrame/Dataset API, không dùng Spark SQL dạng chuỗi.

| Task | Framework | Nội dung | Kết quả |
|------|-----------|----------|---------|
| 1-1 | MapReduce | Cửa sổ trượt độ dài động: size bán chạy nhất mỗi (bang, ngày) | `Task_1-1.csv` |
| 1-2 | MapReduce | Median variety (số SKU distinct) theo (bang, tháng), style ≥ XXL | `Task_1-2.csv` |
| 2-1 | Spark DataFrame | % đơn Cancelled + Standard thoả điều kiện promotion và ngưỡng bang | `Task_2-1.parquet` |
| 2-2 | Spark DataFrame | Độ lệch chuẩn theo percentile động P90/P80 mỗi (SKU, tháng) | `Task_2-2.parquet` |

---

## 1. Cấu trúc bài nộp

```
23127371/
├── src/
│   ├── pom.xml             Build chung 4 task (Maven + shade plugin)
│   ├── clean.ipynb         Tiền xử lý dữ liệu gốc → asr.csv
│   ├── docker/             Dockerfile, docker-compose.yml, config Hadoop
│   ├── Task_1-1/Task11.scala
│   ├── Task_1-2/Task12.scala
│   ├── Task_2-1/Task21.scala
│   └── Task_2-2/Task22.scala
└── docs/
    ├── Report.pdf          Báo cáo phân tích 4 bài
    ├── drive_link.txt      Link Google Drive chứa 4 file kết quả
    └── README.md           File này
```

Mỗi task là một file Scala độc lập. Ngoài cấu trúc mẫu của đề, `src/` có thêm `pom.xml`
để biên dịch, `docker/` để dựng lại môi trường Hadoop và Spark, và `clean.ipynb` để sinh
lại dữ liệu đầu vào — ba thứ cần thiết để chạy và tái lập kết quả.

---

## 2. Chuẩn bị dữ liệu

Bốn task đọc chung một file `asr.csv` — bản đã làm sạch từ dữ liệu gốc
`Amazon Sale Report.csv`. File này không kèm trong bài nộp vì kích thước lớn, cần
sinh lại từ dữ liệu gốc trước khi chạy.

Tạo thư mục `data/` ngang với `src/`, đặt file gốc vào đó:

```
23127371/
├── data/
│   └── Amazon Sale Report.csv
├── src/clean.ipynb
└── docs/
```

Mở `src/clean.ipynb` bằng Jupyter và chạy toàn bộ. Notebook tự xác định thư mục gốc
dự án nên chạy được bất kể mở từ đâu; nó đọc `data/Amazon Sale Report.csv`, ghi ra
`data/asr.csv`, và in bảng thống kê số dòng đã loại.

Nội dung làm sạch:

- Chuẩn hoá cột `ship-state`: viết hoa, bỏ khoảng trắng, gộp các cách viết khác nhau
  của cùng một bang (`NEW DELHI` → `DELHI`, `ORISSA` → `ODISHA`, `RJ` → `RAJASTHAN`,
  `PB` → `PUNJAB`, `PONDICHERRY` → `PUDUCHERRY`), sửa lỗi chính tả, và ánh xạ mã bang
  dựa trên thành phố (`AR` + `ITANAGAR` → `ARUNACHAL PRADESH`).
- Đối chiếu với danh sách 36 bang hợp lệ, loại 33 dòng thiếu `ship-state` và 1 dòng có
  `ship-state` là `APO` — không phải tên bang và không có căn cứ để suy ra bang nào.
- Kết quả: 128.975 → **128.941 dòng, 36 bang**.

Việc chuẩn hoá tên bang chỉ thực hiện ở bước này, không lặp lại trong mã Spark hay
MapReduce.

---

## 3. Dựng môi trường

Toàn bộ chạy trong Docker (Hadoop 3.4.1 + Spark 3.5.3, Java 8), kế thừa môi trường
Hadoop từ Lab 1 và bổ sung Spark. Lần dựng đầu mất khoảng 10 phút để tải và build image.

```bash
cd 23127371/src/docker
docker compose up -d --build
docker exec -it hcmus-lab3 bash
```

Thư mục `23127371/` được mount vào `/lab` trong container: dữ liệu ở `/lab/data`,
mã nguồn ở `/lab/src`, kết quả ghi vào `/lab/out`.

---

## 4. Biên dịch

```bash
cd /lab/src
mvn clean package
```

Kết quả: `target/lab3-1.0.jar`, đã đóng gói sẵn `scala-library`. Bốn task dùng chung file
jar này, phân biệt bằng tên class.

---

## 5. Chạy từng task

### Task 2-1 và 2-2 (Spark)

Mỗi task nhận ba tham số: đường dẫn CSV đầu vào, thư mục part tạm, và file Parquet
đích. Chương trình tự gộp part-file thành một file Parquet duy nhất, đọc được bằng
Pandas hoặc Spark ở chế độ local.

```bash
spark-submit --class Task21 /lab/src/target/lab3-1.0.jar \
  "file:///lab/data/asr.csv" \
  "file:///lab/out/Task_2-1_parquet" \
  "file:///lab/out/Task_2-1.parquet"

spark-submit --class Task22 /lab/src/target/lab3-1.0.jar \
  "file:///lab/data/asr.csv" \
  "file:///lab/out/Task_2-2_parquet" \
  "file:///lab/out/Task_2-2.parquet"
```

### Task 1-1 và 1-2 (MapReduce)

Hai task này đọc dữ liệu từ HDFS, nhận hai tham số: đường dẫn đầu vào và thư mục đầu
ra trên HDFS.

```bash
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /lab/data/asr.csv /data/asr.csv

hadoop jar /lab/src/target/lab3-1.0.jar Task11 /data/asr.csv /out/task11
hadoop jar /lab/src/target/lab3-1.0.jar Task12 /data/asr.csv /out/task12
```

MapReduce ghi kết quả thành nhiều part-file trên HDFS. Gộp về một file CSV trên hệ
thống tệp thường:

```bash
hdfs dfs -getmerge /out/task11 /lab/out/Task_1-1.csv
hdfs dfs -getmerge /out/task12 /lab/out/Task_1-2.csv
```

Sau các bước trên, bốn file kết quả nằm trong `/lab/out`.

---

## 6. Kiểm tra kết quả

```python
import pandas as pd

print(pd.read_csv('out/Task_1-1.csv').shape)
print(pd.read_csv('out/Task_1-2.csv').shape)
print(pd.read_parquet('out/Task_2-1.parquet').shape)
print(pd.read_parquet('out/Task_2-2.parquet').shape)
```

Task 2-1 cho 1.434 thành phố với các cột `city`, `total_orders`, `qualified_orders`,
`pct_cancelled`. Cột `pct_cancelled` bằng 0 ở mọi thành phố; đây là đáp số đúng, lập
luận và số liệu chứng minh trình bày trong `Report.pdf`.

Bốn file kết quả cũng được cung cấp qua Google Drive — xem `drive_link.txt`.

---

## 7. Giả định khi đọc đề

Đề có một số chỗ chưa nói rõ: định nghĩa "shipped", cách chuẩn hoá tên bang, thang bậc
size, xử lý giá trị null, và cách hiểu vài điều kiện lọc. Cách hiểu đã chọn cho từng bài,
kèm biện luận và số liệu đo được nếu hiểu theo cách khác, trình bày trong `Report.pdf`.
