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
hdfs dfs -put -f "/lab/Lab3/data/Amazon Sale Report.csv" /data/asr.csv
```

### 3. Build jar chung
```bash
cd /lab/Lab3/src && mvn -q clean package
# ra target/lab3-1.0.jar (đã bundle scala-library + opencsv)
```

### 4. Chạy từng task
```bash
# Task 2-1 (Spark) — đọc CSV local, xuất parquet
spark-submit --class Task21 target/lab3-1.0.jar \
  "/lab/Lab3/data/Amazon Sale Report.csv" /lab/Lab3/out/Task_2-1_parquet

# Task 2-2 (Spark)
spark-submit --class Task22 target/lab3-1.0.jar ...

# Task 1-1, 1-2 (MapReduce)
hadoop jar target/lab3-1.0.jar Task11 /data/asr.csv /out/task11
```

### 5. Gộp về 1 file đúng tên (đề bắt buộc single file)
Spark ghi ra thư mục chứa `part-*`. Đổi tên:
```bash
# Parquet
mv out/Task_2-1_parquet/part-*.snappy.parquet out/Task_2-1.parquet
# CSV
mv out/task11/part-*.csv out/Task_1-1.csv
```
**Cấm** `getmerge` với parquet (hỏng footer). 4 file kết quả upload lên Google Drive.

## Kiểm nhanh trước nộp
```python
import pandas as pd
pd.read_parquet('out/Task_2-1.parquet').shape   # ~1434 dòng, pct toàn 0
```

## Nộp
- ZIP: `scripts/package-submission.sh` → `23127371.zip`
- Drive: 4 file kết quả trong folder `23127371/`, dán link vào `docs/drive_link.txt`
- **Khoá quyền edit Drive sau deadline** (sửa sau = huỷ điểm).
