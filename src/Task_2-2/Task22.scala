import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TASK 2-2: Spark Structured APIs (DataFrame API) - Dynamic Percentile Threshold
 * ===============================================================================
 * For each SKU in each month (SKU, month):
 *   1. Count promotions for each order from promotion-ids column.
 *   2. Implement 2 methods for dynamic percentile threshold (P90 and P80):
 *      - Method 1 (Approximate): Using percentile_approx (Pure DataFrame API, no SQL string).
 *      - Method 2 (Exact): Using Spark Window functions (Nearest-Rank method).
 *   3. Calculate Population Standard Deviation (stddev_pop) of Amount for orders >= threshold.
 *   4. If qualified count < 2 or stddev_pop is null, set stddev = 0.0.
 *   5. Perform 5-run benchmarking (plus 1 warm-up run) for statistical mean & stddev.
 */
object Task22 {

  def readCsv(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)

  def prepareData(df: DataFrame): DataFrame = {
    val countPromosUdf = udf((promoStr: String) => {
      if (promoStr == null || promoStr.trim.isEmpty) 0
      else {
        promoStr.split(",")
          .map(_.trim)
          .filter(_.nonEmpty)
          .length
      }
    })

    df.withColumn("parsed_date", to_date(col("Date"), "MM-dd-yy"))
      .withColumn("month", date_format(col("parsed_date"), "yyyy-MM"))
      .withColumn("sku", upper(trim(col("SKU"))))
      .withColumn("amount", col("Amount").cast("double"))
      .withColumn("n_promos", countPromosUdf(col("promotion-ids")))
      .filter(col("sku").isNotNull && col("sku") =!= "" && col("month").isNotNull && col("month") =!= "")
  }

  /**
   * METHOD 1: Pure DataFrame API using percentile_approx function (no SQL strings).
   */
  def computeApproxPercentiles(preparedDf: DataFrame): DataFrame = {
    val thresholds = preparedDf
      .groupBy("sku", "month")
      .agg(
        percentile_approx(col("n_promos"), lit(0.90), lit(10000)).as("p90_thresh"),
        percentile_approx(col("n_promos"), lit(0.80), lit(10000)).as("p80_thresh"),
        count("*").as("total_group_orders")
      )

    val joined = preparedDf.join(thresholds, Seq("sku", "month"), "inner")

    val p90Selected = joined.filter(col("n_promos") >= col("p90_thresh"))
    val p90Agg = p90Selected
      .groupBy("sku", "month")
      .agg(
        count(when(col("amount").isNotNull, 1)).as("p90_valid_count"),
        stddev_pop("amount").as("p90_stddev_raw")
      )
      .withColumn("p90_stddev_approx",
        when(col("p90_valid_count") < 2 || col("p90_stddev_raw").isNull, lit(0.0))
          .otherwise(coalesce(col("p90_stddev_raw"), lit(0.0)))
      )

    val p80Selected = joined.filter(col("n_promos") >= col("p80_thresh"))
    val p80Agg = p80Selected
      .groupBy("sku", "month")
      .agg(
        count(when(col("amount").isNotNull, 1)).as("p80_valid_count"),
        stddev_pop("amount").as("p80_stddev_raw")
      )
      .withColumn("p80_stddev_approx",
        when(col("p80_valid_count") < 2 || col("p80_stddev_raw").isNull, lit(0.0))
          .otherwise(coalesce(col("p80_stddev_raw"), lit(0.0)))
      )

    thresholds
      .join(p90Agg.select("sku", "month", "p90_stddev_approx"), Seq("sku", "month"), "left")
      .join(p80Agg.select("sku", "month", "p80_stddev_approx"), Seq("sku", "month"), "left")
      .withColumn("p90_stddev_approx", coalesce(col("p90_stddev_approx"), lit(0.0)))
      .withColumn("p80_stddev_approx", coalesce(col("p80_stddev_approx"), lit(0.0)))
      .withColumnRenamed("p90_thresh", "p90_thresh_approx")
      .withColumnRenamed("p80_thresh", "p80_thresh_approx")
  }

  /**
   * METHOD 2: Spark Window Functions (Nearest-Rank Method)
   */
  def computeExactPercentiles(preparedDf: DataFrame): DataFrame = {
    val windowSorted = Window.partitionBy("sku", "month").orderBy("n_promos")
    val windowGroup  = Window.partitionBy("sku", "month")

    val ranked = preparedDf
      .withColumn("row_num", row_number().over(windowSorted))
      .withColumn("total_count", count("*").over(windowGroup))

    val withTargetIndices = ranked
      .withColumn("idx_p90", ceil(col("total_count") * 0.90).cast("int"))
      .withColumn("idx_p80", ceil(col("total_count") * 0.80).cast("int"))

    val exactThresholds = withTargetIndices
      .groupBy("sku", "month")
      .agg(
        max(when(col("row_num") === col("idx_p90"), col("n_promos"))).as("p90_thresh_exact"),
        max(when(col("row_num") === col("idx_p80"), col("n_promos"))).as("p80_thresh_exact")
      )

    val joined = preparedDf.join(exactThresholds, Seq("sku", "month"), "inner")

    val p90ExactAgg = joined
      .filter(col("n_promos") >= col("p90_thresh_exact"))
      .groupBy("sku", "month")
      .agg(
        count(when(col("amount").isNotNull, 1)).as("p90_valid_count"),
        stddev_pop("amount").as("p90_stddev_raw")
      )
      .withColumn("p90_stddev_exact",
        when(col("p90_valid_count") < 2 || col("p90_stddev_raw").isNull, lit(0.0))
          .otherwise(coalesce(col("p90_stddev_raw"), lit(0.0)))
      )

    val p80ExactAgg = joined
      .filter(col("n_promos") >= col("p80_thresh_exact"))
      .groupBy("sku", "month")
      .agg(
        count(when(col("amount").isNotNull, 1)).as("p80_valid_count"),
        stddev_pop("amount").as("p80_stddev_raw")
      )
      .withColumn("p80_stddev_exact",
        when(col("p80_valid_count") < 2 || col("p80_stddev_raw").isNull, lit(0.0))
          .otherwise(coalesce(col("p80_stddev_raw"), lit(0.0)))
      )

    exactThresholds
      .join(p90ExactAgg.select("sku", "month", "p90_stddev_exact"), Seq("sku", "month"), "left")
      .join(p80ExactAgg.select("sku", "month", "p80_stddev_exact"), Seq("sku", "month"), "left")
      .withColumn("p90_stddev_exact", coalesce(col("p90_stddev_exact"), lit(0.0)))
      .withColumn("p80_stddev_exact", coalesce(col("p80_stddev_exact"), lit(0.0)))
  }

  def writeSingleParquet(df: DataFrame, dir: String, target: String): Unit = {
    df.coalesce(1).write.mode("overwrite").parquet(dir)
    val hconf = df.sparkSession.sparkContext.hadoopConfiguration
    val srcPath = new org.apache.hadoop.fs.Path(dir)
    val dstPath = new org.apache.hadoop.fs.Path(target)
    val srcFs = srcPath.getFileSystem(hconf)
    val dstFs = dstPath.getFileSystem(hconf)

    val part = srcFs.globStatus(new org.apache.hadoop.fs.Path(dir + "/part-*.parquet"))(0).getPath
    if (dstFs.exists(dstPath)) {
      dstFs.delete(dstPath, false)
    }
    org.apache.hadoop.fs.FileUtil.copy(srcFs, part, dstFs, dstPath, false, hconf)
  }

  def main(args: Array[String]): Unit = {
    val in     = if (args.length > 0) args(0) else "hdfs://localhost:9000/input/asr.csv"
    val out    = if (args.length > 1) args(1) else "hdfs://localhost:9000/out_temp"
    val target = if (args.length > 2) args(2) else "file:///lab/bigdata-lab3/data/Task_2-2.parquet"

    val spark = SparkSession.builder()
      .appName("Task2-2-DynamicPercentileThreshold")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", "16")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      println(s"[Task22] Reading and preparing data from: $in")
      val prepared = prepareData(readCsv(spark, in)).cache()

      val totalRecords = prepared.count()
      println(s"[Task22] Total valid records: $totalRecords")

      // --- BENCHMARKING (1 Warm-up + 5 Measured Runs) ---
      println("\n[Task22] Running Warm-up execution...")
      computeApproxPercentiles(prepared).count()
      computeExactPercentiles(prepared).count()

      println("\n[Task22] Executing 5-run Benchmark Loop...")
      val numRuns = 5
      val approxTimes = new Array[Long](numRuns)
      val exactTimes = new Array[Long](numRuns)

      var approxRes: DataFrame = null
      var exactRes: DataFrame = null

      for (i <- 0 until numRuns) {
        val t0A = System.currentTimeMillis()
        approxRes = computeApproxPercentiles(prepared)
        val cA = approxRes.count()
        val t1A = System.currentTimeMillis()
        approxTimes(i) = t1A - t0A

        val t0E = System.currentTimeMillis()
        exactRes = computeExactPercentiles(prepared)
        val cE = exactRes.count()
        val t1E = System.currentTimeMillis()
        exactTimes(i) = t1E - t0E
        println(f"  Run ${i + 1}: Approx = ${approxTimes(i)}%4d ms | Exact = ${exactTimes(i)}%4d ms")
      }

      val approxMean = approxTimes.sum.toDouble / numRuns
      val approxStd = math.sqrt(approxTimes.map(t => math.pow(t - approxMean, 2)).sum / (numRuns - 1))

      val exactMean = exactTimes.sum.toDouble / numRuns
      val exactStd = math.sqrt(exactTimes.map(t => math.pow(t - exactMean, 2)).sum / (numRuns - 1))

      // --- METRICS & COMPARISON ---
      val combined = approxRes.join(exactRes, Seq("sku", "month"), "inner")
        .withColumn("p90_thresh_diff", abs(col("p90_thresh_approx") - col("p90_thresh_exact")))
        .withColumn("p80_thresh_diff", abs(col("p80_thresh_approx") - col("p80_thresh_exact")))
        .withColumn("p90_stddev_diff", abs(col("p90_stddev_approx") - col("p90_stddev_exact")))
        .withColumn("p80_stddev_diff", abs(col("p80_stddev_approx") - col("p80_stddev_exact")))
        .orderBy("sku", "month")

      val approxCount = approxRes.count()
      val diffP90ThreshCount = combined.filter(col("p90_thresh_diff") > 0.0001).count()
      val diffP90StddevCount = combined.filter(col("p90_stddev_diff") > 0.0001).count()

      println(f"\n==================== TASK 2.2 BENCHMARK REPORT (5-RUN MEAN ± STD) ====================")
      println(f"- Approximate Method (percentile_approx) : $approxMean%6.2f ms ± $approxStd%5.2f ms")
      println(f"- Exact Method (Window Functions)       : $exactMean%6.2f ms ± $exactStd%5.2f ms")
      println(f"- Total SKU-Month Groups Analyzed        : $approxCount")
      println(f"- Groups with P90 Threshold Difference   : $diffP90ThreshCount / $approxCount (${diffP90ThreshCount.toDouble / approxCount * 100}%5.2f%%)")
      println(f"- Groups with P90 StdDev Difference      : $diffP90StddevCount / $approxCount (${diffP90StddevCount.toDouble / approxCount * 100}%5.2f%%)")
      println(f"======================================================================================")

      println(s"\n[Task22] Saving Parquet result to: $target")
      writeSingleParquet(combined, out, target)
      println(s"[Task22] Task_2-2.parquet created successfully!")

    } finally {
      spark.stop()
    }
  }
}
