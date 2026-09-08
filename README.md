# Lab 03: Advanced MapReduce & Spark Structured APIs

**Course:** Big Data Concepts & Technologies  
**Tech Stack:** Scala 2.12 | Apache Hadoop 3.3.6 | Apache Spark 3.4.1 / 3.5.3 | Java 8  

---

## 1. Overview & Tasks Summary

This project implements large-scale data processing algorithms for **Lab 03** using Apache Hadoop MapReduce and Apache Spark. All solutions process the dataset `asr.csv` (128,941 transaction records).

| Task ID | Description | Framework | Output Artifact |
| :--- | :--- | :--- | :--- |
| **Task 1.1** | Dynamic Sliding Window & Tie-Breaking | MapReduce | `Task_1-1.csv` |
| **Task 1.2** | State-Level Median Style Variety | MapReduce | `Task_1-2.csv` |
| **Task 2.1** | Cancelled Order Ratio & Promotion Validity | Spark DataFrame | `Task_2-1.parquet` |
| **Task 2.2** | Dynamic Percentiles ($P_{90}, P_{80}$) & Population StdDev ($\sigma_{\text{pop}}$) | Spark DataFrame API | `Task_2-2.parquet` |

---

## 2. Submission Directory Structure

Per the official assignment guidelines (**Lab 3 - MR-Spark.pdf**, Section 3):

### Compressed Archive (`<RepresentativeID>.zip`)
```text
<RepresentativeID>/
├── src/
│   ├── Task_1-1/
│   │   └── Task11.scala
│   ├── Task_1-2/
│   │   └── Task12.scala
│   ├── Task_2-1/
│   │   └── Task21.scala
│   └── Task_2-2/
│       └── Task22.scala
└── docs/
    ├── Report.pdf
    ├── drive_link.txt
    └── README.md
```

### Google Drive Output Directory (`<RepresentativeID>/`)
The `drive_link.txt` file contains the URL to the shared Google Drive folder organized as follows:
```text
<RepresentativeID>/
├── Task_1-1.csv
├── Task_1-2.csv
├── Task_2-1.parquet
└── Task_2-2.parquet
```

---

## 3. Environment & HDFS Setup

```bash
# Start Hadoop and Spark cluster services
service ssh start && start-dfs.sh && start-yarn.sh

# Upload preprocessed dataset to HDFS
hdfs dfs -mkdir -p /input
hdfs dfs -put -f asr.csv /input/
```

---

## 4. Execution Guide

---

### Task 1.1 — MapReduce Dynamic Sliding Window
```bash
# Compile and submit Hadoop MapReduce job
hadoop jar lab3-1.0.jar Task11 /input/asr.csv /output/task11

# Merge HDFS output to local CSV
hdfs dfs -getmerge /output/task11 Task_1-1.csv
```

---

### Task 1.2 — MapReduce State-Level Median Variety
```bash
# Submit Hadoop MapReduce job
hadoop jar lab3-1.0.jar Task12 /input/asr.csv /output/task12

# Merge HDFS output to local CSV
hdfs dfs -getmerge /output/task12 Task_1-2.csv
```

---

### Task 2.1 — Spark Cancelled Order Percentage
```bash
# Submit Spark job
spark-submit --class Task21 lab3-1.0.jar \
  "hdfs://localhost:9000/input/asr.csv" \
  "hdfs://localhost:9000/output/task21_temp" \
  "file:///path/to/Task_2-1.parquet"
```

---

### Task 2.2 — Spark Dynamic Percentiles & Population StdDev

#### Method 1: Interactive Execution via Spark Shell (Recommended)
```bash
spark-shell --driver-java-options "-Dfile.encoding=UTF-8" -i src/Task_2-2/Task22.scala
```
*Run inside Scala REPL:*
```scala
Task22.main(Array(
  "hdfs://localhost:9000/input/asr.csv",
  "hdfs://localhost:9000/out_temp",
  "file:///path/to/Task_2-2.parquet"
))
```

#### Method 2: Package Execution via `spark-submit`
```bash
spark-submit --class Task22 lab3-1.0.jar \
  "hdfs://localhost:9000/input/asr.csv" \
  "hdfs://localhost:9000/out_temp" \
  "file:///path/to/Task_2-2.parquet"
```

