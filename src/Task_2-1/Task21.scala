import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TASK 2-1 · Spark DataFrame API (KHÔNG raw Spark SQL)
 * ================================================================
 * Với mỗi THÀNH PHỐ: % đơn Cancelled + Standard thoả CẢ HAI điều kiện:
 *   (1) có >= 3 promotion hợp lệ (active period >= 2 ngày; kể cả mã Amazon)
 *   (2) amount < trung bình đơn Merchant + Courier=Shipped của BANG đó
 *
 * Input: data/asr.csv (đã clean state bằng clean.ipynb — 128.941 dòng, 36 bang).
 *
 * Pipeline 4 khối:
 *   A  tuổi thọ mã KM   groupBy(promo).agg(min/max) -> 284 mã, 185 hợp lệ
 *   B  ngưỡng bang      Merchant AND Courier=Shipped -> 31.871 dòng, 36 bang
 *   C  tập đơn          LEFT join A (đếm mã hợp lệ) + LEFT join B -> 6.906 đơn
 *   D  gộp city         % đơn thoả -> 1.434 city
 *
 * Kết quả: 0% mọi thành phố (mọi đơn Cancelled+Standard đều không có promotion).
 *
 * Chạy:
 *   spark-submit --class Task21 /lab/src/target/lab3-1.0.jar \
 *     "file:///lab/data/asr.csv" \
 *     "file:///lab/out/Task_2-1_parquet" \
 *     "file:///lab/out/Task_2-1.parquet"
 */
object Task21 {

  /** Đọc CSV. multiLine + quote để cột promotion-ids (có dấu phẩy trong ngoặc kép) không vỡ field. */
  def readCsv(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)

  /** Thêm cột dẫn xuất: ngày (MM-dd-yy), city chuẩn hoá, amount kiểu Double. */
  def normalize(df: DataFrame): DataFrame =
    df.withColumn("d", to_date(col("Date"), "MM-dd-yy"))
      .withColumn("city", upper(trim(col("ship-city"))))
      .withColumn("state", upper(trim(col("ship-state"))))
      .withColumn("amount", col("Amount").cast("double"))

  /**
   * Dựng pipeline A->D. Mỗi lần gọi tạo QueryExecution mới nên explain phản ánh
   * đúng cấu hình hiện tại (gọi explain 2 lần trên cùng Dataset sẽ dùng plan cũ).
   */
  def buildPipeline(base: DataFrame): DataFrame = {
    // A: tuổi thọ mỗi mã KM; giữ mã sống >= 2 ngày. groupBy thu 129k dòng về 284 mã.
    val validPromos = base
      .select(col("d"), explode(split(col("promotion-ids"), ",")).as("promo"))
      .withColumn("promo", trim(col("promo")))
      .filter(col("promo") =!= "" && col("d").isNotNull)
      .groupBy("promo")
      .agg(min("d").as("first_d"), max("d").as("last_d"))
      .withColumn("active_days", datediff(col("last_d"), col("first_d")))
      .filter(col("active_days") >= 2)
      .select("promo")

    // B: ngưỡng trung bình theo BANG — 2 điều kiện Merchant AND Courier=Shipped.
    val stateThreshold = base
      .filter(col("Fulfilment") === "Merchant" && col("Courier Status") === "Shipped")
      .filter(col("state").isNotNull && col("state") =!= "")
      .groupBy("state")
      .agg(avg("amount").as("state_avg"))

    // C: ứng viên = Status chứa "cancelled" AND Standard.
    // Khoá dùng cột "index" có sẵn (int duy nhất), không dùng monotonically_increasing_id
    // vì hàm đó không ổn định giữa các lần đánh giá.
    val candidates = base
      .filter(lower(col("Status")).contains("cancelled") &&
              col("ship-service-level") === "Standard")
      .withColumn("row_id", col("index").cast("long"))
      .select("row_id", "city", "state", "amount", "promotion-ids")

    // Đếm mã hợp lệ mỗi đơn. LEFT join ở dưới để đơn không có mã vẫn nằm ở mẫu số.
    val validCount = candidates
      .select(col("row_id"), explode(split(col("promotion-ids"), ",")).as("promo"))
      .withColumn("promo", trim(col("promo")))
      .filter(col("promo") =!= "")
      .join(validPromos, Seq("promo"), "inner")
      .groupBy("row_id")
      .agg(count("*").as("n_valid"))

    val enriched = candidates
      .join(validCount, Seq("row_id"), "left")
      .withColumn("n_valid", coalesce(col("n_valid"), lit(0)))
      .join(stateThreshold, Seq("state"), "left")
      .withColumn("qualified",
        when(col("n_valid") >= 3 &&
             col("amount").isNotNull &&
             col("amount") < col("state_avg"), lit(1)).otherwise(lit(0)))

    // D: gộp theo thành phố.
    enriched
      .filter(col("city").isNotNull && col("city") =!= "")
      .groupBy("city")
      .agg(count("*").as("total_orders"),
           sum("qualified").as("qualified_orders"))
      .withColumn("pct_cancelled",
        round(col("qualified_orders") / col("total_orders") * 100, 4))
      .orderBy("city")
  }

  /** Ghi 1 file parquet phẳng: coalesce(1) vào thư mục tạm rồi copy part-file ra target. */
  def writeSingleParquet(df: DataFrame, dir: String, target: String): Unit = {
    df.coalesce(1).write.mode("overwrite").parquet(dir)
    val hconf = df.sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(dir), hconf)
    val part = fs.globStatus(new org.apache.hadoop.fs.Path(dir + "/part-*.parquet"))(0).getPath
    val dst = new org.apache.hadoop.fs.Path(target)
    fs.delete(dst, false)
    org.apache.hadoop.fs.FileUtil.copy(fs, part, fs, dst, false, hconf)
  }

  def main(args: Array[String]): Unit = {
    val in     = if (args.length > 0) args(0) else "file:///lab/data/asr.csv"
    val out    = if (args.length > 1) args(1) else "file:///lab/out/Task_2-1_parquet"
    val target = if (args.length > 2) args(2) else "file:///lab/out/Task_2-1.parquet"

    val spark = SparkSession.builder()
      .appName("Task2-1-CancelledPromotions")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", "16")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val base = normalize(readCsv(spark, in))

      // Tắt AQE để explain in kế hoạch tĩnh, phản ánh đúng khác biệt broadcast.
      spark.conf.set("spark.sql.adaptive.enabled", false)

      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
      println("========== EXPLAIN — DEFAULT (broadcast on, AQE off) ==========")
      buildPipeline(base).explain(true)

      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
      println("========== EXPLAIN — autoBroadcastJoinThreshold=-1 (AQE off) ==========")
      buildPipeline(base).explain(true)

      // Đếm số stage của riêng action write: cô lập bằng job group rồi lấy distinct stageIds.
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
      val sc = spark.sparkContext
      sc.setJobGroup("task21-write", "Task21 write output parquet")
      writeSingleParquet(buildPipeline(base), out, target)
      val writeStages = sc.statusTracker.getJobIdsForGroup("task21-write")
        .flatMap(jid => sc.statusTracker.getJobInfo(jid).map(_.stageIds()).getOrElse(Array.empty[Int]))
        .distinct
      sc.clearJobGroup()
      println(s"[Task21] writeStages(distinct stageIds, AQE off, broadcast on)=${writeStages.length}")

      // Verify sau khi ghi, đọc lại file kết quả (không lẫn vào phép đếm stage).
      val res = spark.read.parquet(target)
      println(s"[Task21] cities=${res.count()} " +
              s"totalOrders=${res.agg(sum("total_orders")).first().getLong(0)} " +
              s"totalQualifiedOrders=${res.agg(sum("qualified_orders")).first().getLong(0)} " +
              s"maxQualified=${res.agg(max("qualified_orders")).first().getLong(0)}")
    } finally {
      spark.stop()
    }
  }
}
