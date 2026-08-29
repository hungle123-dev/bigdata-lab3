# Lab 3 — Advanced MapReduce & Spark Structured APIs

Nhóm 4 người · RepresentativeID **23127371** · Scala · Docker (Hadoop 3.4.1 + Spark 3.5.3, Java 8).

Dataset: `Amazon Sale Report.csv` (128.975 dòng, 31/03→29/06/2022). **KHÔNG commit** — tải từ Moodle, đặt vào `data/`.

## Chốt chung
Đọc **`ASSUMPTIONS.md` TRƯỚC KHI CODE**. Mọi giả định (5 bẫy dữ liệu + 3 chỗ mập mờ) chốt ở đó. Không tự quyết khác.

## Cấu trúc repo
```
Lab3/
├── ASSUMPTIONS.md          ← chốt chung, đọc trước
├── data/                   ← CSV để đây (gitignored)
├── out/                    ← kết quả xuất (gitignored)
├── src/
│   ├── pom.xml             ← build chung 4 task
│   ├── docker/             ← Dockerfile (Lab1 + Spark), compose, config
│   ├── common/             ← MRCommon.scala (TV1), SparkCommon.scala (TV3)
│   ├── Task_1-1/  Task_1-2/  Task_2-1/  Task_2-2/
├── report/                 ← LaTeX, mỗi người 1 file .tex
└── scripts/                ← run + đóng gói nộp
```

## Chia việc
| TV | Task | File sở hữu |
|----|------|-------------|
| 1 | 1-1 | `common/MRCommon.scala`, `Task_1-1/` |
| 2 | 1-2 | `Task_1-2/` |
| 3 | 2-1 | `common/SparkCommon.scala`, `src/docker/`, `Task_2-1/`, ghép Report |
| 4 | 2-2 | `Task_2-2/`, đóng ZIP nộp |

## Chạy

### 1. Dựng container (1 lần, ~10 phút build)
```bash
cd src/docker
docker compose up -d --build      # build image, chạy Hadoop + có sẵn Spark
docker exec -it hcmus-lab3 bash
```

### 2. Đưa dữ liệu lên HDFS (cho Task MapReduce)
```bash
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /lab/data/asr.csv /data/asr.csv
```

### 3. Build jar chung
```bash
cd /lab/src && mvn clean package
# ra target/lab3-1.0.jar (đã bundle scala-library + opencsv) — VERIFIED: BUILD SUCCESS
```

### 4. Chạy từng task
```bash
# Task 2-1 (Spark) — đọc CSV local, tự xuất Task_2-1.parquet (1 file phẳng)
# 3 arg = input, thư mục part tạm, file parquet đích. Khớp default trong Task21.scala.
spark-submit --class Task21 /lab/src/target/lab3-1.0.jar \
  "file:///lab/data/asr.csv" \
  "file:///lab/out/Task_2-1_parquet" \
  "file:///lab/out/Task_2-1.parquet"

# Task 2-2 (Spark)
spark-submit --class Task22 /lab/src/target/lab3-1.0.jar ...

# Task 1-1, 1-2 (MapReduce)
hadoop jar /lab/src/target/lab3-1.0.jar Task11 /data/asr.csv /out/task11
```

### 5. Gộp về 1 file đúng tên
Task 2-1 **tự** copy part-file ra `Task_2-1.parquet` (arg thứ 3). Các task khác:
```bash
# CSV (task MapReduce)
mv out/task11/part-*.csv out/Task_1-1.csv
```
**Cấm** `getmerge` với parquet (hỏng footer). 4 file kết quả upload lên Google Drive.

## Kiểm nhanh trước nộp
```python
import pandas as pd
d = pd.read_parquet('out/Task_2-1.parquet')
print(d.shape)                    # (1434, 4)
print(d['total_orders'].sum())    # 6906
print(d['pct_cancelled'].unique())# [0.]
```

## Nộp
- ZIP: `scripts/package-submission.sh` → `23127371.zip`
- Drive: 4 file kết quả trong folder `23127371/`, dán link vào `docs/drive_link.txt`
- **Khoá quyền edit Drive sau deadline** (sửa sau = huỷ điểm).
