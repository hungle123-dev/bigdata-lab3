import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TASK 2-1 · Spark DataFrame API (KHÔNG raw SQL)
 * ---------------------------------------------------------------
 * Với mỗi THÀNH PHỐ: % đơn Cancelled + Standard thoả CẢ HAI:
 *   (1) có >= 3 promotion HỢP LỆ (active period >= 2 ngày; kể cả mã Amazon)
 *   (2) amount < trung bình đơn Merchant + Courier=Shipped của BANG đó
 *
 * Pipeline 4 khối — xem ASSUMPTIONS.md mục C4:
 *   A  tuổi thọ mã KM   groupBy(promo).agg(min/max)   284 -> 185 mã hợp lệ
 *   B  ngưỡng bang      Merchant AND Courier=Shipped  31.881 dòng -> 40 bang
 *   C  tập đơn          LEFT join A (đếm mã hợp lệ), LEFT join B (so ngưỡng)  6.909 đơn
 *   D  gộp city         % đơn thoả (nValid>=3 AND amount<thr)  ~1.435 city
 *
 * Đáp số kỳ vọng: 0% mọi thành phố (đã chứng minh — KHÔNG nới điều kiện).
 * Report (a)(b)(c): dựng lại pipeline dưới 2 conf, explain mỗi lần.
 */
object Task21 {

  /**
   * Dựng pipeline A->D và trả DataFrame kết quả.
   * QUAN TRỌNG: mỗi lần gọi tạo QueryExecution MỚI -> explain phản ánh conf hiện tại.
   * Gọi 1 lần dùng cùng Dataset thì explain lần 2 dùng lại plan cũ (đã memoize) -> sai.
   */
  def buildPipeline(base: DataFrame): DataFrame = {
    // KHỐI A: tuổi thọ mã KM, giữ mã sống >= 2 ngày (groupBy min/max, KHÔNG window)
    val validPromos = base
      .select(col("d"), explode(split(col("promotion-ids"), ",")).as("promo"))
      .withColumn("promo", trim(col("promo")))
      .filter(col("promo") =!= "" && col("d").isNotNull)
      .groupBy("promo")
      .agg(min("d").as("first_d"), max("d").as("last_d"))
      .withColumn("active_days", datediff(col("last_d"), col("first_d")))
      .filter(col("active_days") >= 2)          // C4: between = phép trừ -> 185 mã
      .select("promo")

    // KHỐI B: ngưỡng trung bình theo BANG (Merchant AND Courier=Shipped -> 40 bang)
    val stateThreshold = base
      .filter(col("Fulfilment") === "Merchant" && col("Courier Status") === "Shipped")
      .groupBy("state")
      .agg(avg("amount").as("state_avg"))

    // KHỐI C: ứng viên = Status chứa "cancelled" AND Standard -> 6.909 dòng
    val candidates = base
      .filter(lower(col("Status")).contains("cancelled") &&
              col("ship-service-level") === "Standard")
      .withColumn("row_id", monotonically_increasing_id())
      .select("row_id", "city", "state", "amount", "promotion-ids")

    // Đếm mã HỢP LỆ mỗi đơn; LEFT join để đơn 0 mã vẫn ở mẫu số
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

    // KHỐI D: gộp theo thành phố -> %
    enriched
      .groupBy("city")
      .agg(count("*").as("total_orders"),
           sum("qualified").as("qualified_orders"))
      .withColumn("pct_cancelled",
        round(col("qualified_orders") / col("total_orders") * 100, 4))
      .orderBy("city")
  }

  def main(args: Array[String]): Unit = {
    val in  = if (args.length > 0) args(0) else "file:///lab/data/asr.csv"
    val out = if (args.length > 1) args(1) else "file:///lab/out/Task_2-1_parquet"

    val spark = SparkCommon.session("Task2-1-CancelledPromotions")
    spark.sparkContext.setLogLevel("WARN")
    val base = SparkCommon.normalize(SparkCommon.readRaw(spark, in)).cache()

    // ---- REPORT (a)(b)(c): TẮT AQE để explain in plan tĩnh thật ----
    // (AQE bật thì runtime tự re-broadcast, che mất khác biệt threshold.)
    spark.conf.set("spark.sql.adaptive.enabled", false)

    // Lần 1: mặc định -> 3 BroadcastHashJoin (3 bảng join đều tí hon)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    println("========== EXPLAIN — DEFAULT (broadcast on, AQE off) ==========")
    buildPipeline(base).explain(true)

    // Lần 2: tắt broadcast -> 3 SortMergeJoin + thêm Exchange/Sort
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    println("========== EXPLAIN — autoBroadcastJoinThreshold=-1 (AQE off) ==========")
    buildPipeline(base).explain(true)

    // ---- Thực thi + xuất: trả AQE + broadcast về mặc định ----
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    spark.conf.set("spark.sql.adaptive.enabled", true)
    val result = buildPipeline(base)

    // Verify số (in ra để đối chiếu ASSUMPTIONS)
    val totalQualified = result.agg(sum("qualified_orders")).first().getLong(0)
    println(s"[Task21] cities=${result.count()}  totalQualifiedOrders=$totalQualified (kỳ vọng 0)")

    SparkCommon.writeSingleParquet(result, out)
    println(s"[Task21] wrote $out")
    spark.stop()
  }
}
