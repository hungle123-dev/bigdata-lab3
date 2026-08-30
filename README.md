# Lab 03 — Advanced MapReduce & Spark Structured APIs

Nhóm 4 người · RepresentativeID `23127371` · Scala · Docker (Hadoop 3.4.1 + Spark 3.5.3, Java 8)

## Cấu trúc

Repo dựng đúng cây nộp của đề (trang 6-7):

```
src/
├── Task_1-1/Task11.scala   TV1
├── Task_1-2/Task12.scala   TV2
├── Task_2-1/Task21.scala   TV3 — xong
├── Task_2-2/Task22.scala   TV4
├── pom.xml                 build chung 4 task
├── docker/                 môi trường Hadoop + Spark
└── clean.ipynb             tiền xử lý → data/asr.csv

docs/
├── drive_link.txt          link Drive chứa 4 file kết quả
└── README.md               bản nộp cho giảng viên

data/asr.csv                dữ liệu đã clean, input chung 4 task
out/                        kết quả chạy (không commit)
Lab03/                      đề bài gốc — PDF đề + slide. Chỉ tham khảo, không nộp.
```

Đề bài: `Lab03/Lab 3 - MR-Spark.pdf`. Slide tham khảo: `Lab03/Lab3_Slide_ref.pdf`.

Mỗi task là một file Scala độc lập. Ai làm task nào chỉ sửa file của task đó.

## Dữ liệu

`data/asr.csv` là bản đã tiền xử lý bằng `src/clean.ipynb` từ `Amazon Sale Report.csv`:
chuẩn hoá `ship-state`, loại dòng thiếu state và state không hợp lệ.
128.975 → 128.941 dòng, 36 bang.

Cả 4 task dùng chung file này. Mỗi người tự đọc đề để chốt cách hiểu các điều kiện của
bài mình.

## Chạy

```bash
# 1. Dựng container (lần đầu ~10 phút)
cd src/docker
docker compose up -d --build
docker exec -it hcmus-lab3 bash

# 2. Build jar (trong container)
cd /lab/src && mvn clean package

# 3. Task Spark
spark-submit --class Task21 /lab/src/target/lab3-1.0.jar \
  "file:///lab/data/asr.csv" \
  "file:///lab/out/Task_2-1_parquet" \
  "file:///lab/out/Task_2-1.parquet"

# 4. Task MapReduce (cần HDFS)
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /lab/data/asr.csv /data/asr.csv
hadoop jar /lab/src/target/lab3-1.0.jar Task11 /data/asr.csv /out/task11
```

Repo được mount vào `/lab` trong container: `data/` → `/lab/data`, `src/` → `/lab/src`,
kết quả ghi vào `/lab/out`.

## Nộp

- ZIP: folder `23127371/` chứa `src/` + `docs/`
- Drive: folder `23127371/` chứa 4 file kết quả
- Report viết trên Overleaf hoặc Google Docs, xuất PDF vào `docs/Report.pdf`
