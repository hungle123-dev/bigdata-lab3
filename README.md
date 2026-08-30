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
└── drive_link.txt          link Drive chứa 4 file kết quả
                            (Report.pdf và README.md thêm vào khi 4 task xong)

data/asr.csv                dữ liệu đã clean, input chung 4 task
out/                        kết quả chạy (không commit)
Lab03/                      đề bài gốc — PDF đề, slide, CSV gốc. Chỉ tham khảo, không nộp.
```

Đề bài: `Lab03/Lab 3 - MR-Spark.pdf`. Slide tham khảo: `Lab03/Lab3_Slide_ref.pdf`.

Mỗi task là một file Scala độc lập. Ai làm task nào chỉ sửa file của task đó.

## Dữ liệu

`data/asr.csv` là bản đã tiền xử lý bằng `src/clean.ipynb` từ `Amazon Sale Report.csv`:
chuẩn hoá `ship-state`, loại dòng thiếu state và state không hợp lệ.
128.975 → 128.941 dòng, 36 bang.

**Cả 4 task dùng chung `data/asr.csv` này**, không dùng file gốc. Mỗi người tự đọc đề để
chốt cách hiểu các điều kiện của bài mình.

File gốc `Amazon Sale Report.csv` có sẵn trong `data/` và `Lab03/` (2 bản giống nhau) —
chỉ cần khi muốn chạy lại `src/clean.ipynb` để sinh lại `asr.csv`.

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

Cây ZIP theo đề (trang 6-7):

```
23127371/
├── src/    Task_1-1..2-2, pom.xml, docker/, clean.ipynb
└── docs/   Report.pdf, drive_link.txt, README.md (optional)
```

- Drive: folder `23127371/` chứa đúng 4 file kết quả, dán link vào `docs/drive_link.txt`
- Report viết trên Overleaf hoặc Google Docs, xuất PDF vào `docs/Report.pdf`
- Không kèm `data/`, `out/`, `target/`, `Lab03/` vào ZIP
- Khoá quyền edit Drive sau deadline (sửa sau = huỷ điểm)
