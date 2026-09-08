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

### Task 1.1 — MapReduce Dynamic Sliding Window

Processes dynamic sliding windows ($w=5$ or $w=10$ days) across 46 states with algebraic triplet aggregation $\Sigma(1, Qty, Qty^2)$ and cascading tie-breaking rules.

```bash 
# Navigate to Task 1-1 source directory from the project root
cd src/Task_1-1

# 1. Ensure input dataset is available on HDFS
hdfs dfs -mkdir -p /lab2/input
hdfs dfs -put -f /root/lab2/asr.csv /lab2/input/asr.csv

# 2. Compile source code with Hadoop classpath
mkdir -p classes
scalac -classpath "$(hadoop classpath)" -d classes Task11.scala

# 3. Package Fat JAR (bundle Scala runtime to prevent ClassNotFoundException on YARN workers)
SCALA_LIB=$(find /usr -name "scala-library*.jar" 2>/dev/null | head -n 1)
cd classes && jar -xf "$SCALA_LIB" && cd ..
jar -cvf Task1_1.jar -C classes .

# 4. Submit Hadoop MapReduce Job using full package and object name
# Arguments: <input_hdfs_csv> <output_local_csv>
hadoop jar Task1_1.jar fit.bigdata.lab2.Task1_1_SlidingWindowMR \
  /lab2/input/asr.csv \
  /root/lab2/Task_1-1.csv

# 5. Quick Output Validation
head -n 5 /root/lab2/Task_1-1.csv
wc -l /root/lab2/Task_1-1.csv
# Expected output: 3,474 lines (1 header + 3,473 data records)

# Validate boundary conditions
grep "MAHARASHTRA" /root/lab2/Task_1-1.csv | tail -n 1   # Dynamic w=5: ends cleanly at 07-04-22
grep "DELHI" /root/lab2/Task_1-1.csv | tail -n 1         # Dynamic w=10: ends at 07-09-22
```

---

### Task 1.2 — MapReduce State-Level Median Variety
```bash
# Navigate to the Task 1-2 project directory
cd /root/lab3/Task_1-2

# 1. Locate the Scala library JAR
SCALA_LIB=$(find /usr -name "scala-library*.jar" 2>/dev/null | head -n 1)

# 2. Compile the Scala source file
scalac -classpath "$(hadoop classpath)" -d classes Task_1-2.scala

# 3. Extract the Scala runtime into the classes directory to avoid missing Scala classes when running on YARN
cd classes
jar -xf "$SCALA_LIB"
cd ..

# 4. Package the compiled classes and Scala runtime into a Fat JAR
jar -cvf Task1_2.jar -C classes .

# 5. Submit Hadoop MapReduce job
hadoop jar Task1_2.jar Task_1_2 \
/input/asr.csv \
/output/task12_intermediate \
/output/task12

# The program automatically generates the final CSV in the current local directory:
Task_1-2.csv
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

