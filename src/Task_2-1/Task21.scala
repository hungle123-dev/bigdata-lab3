import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

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
 *   D  gộp city         % đơn thoả (nValid>=3 AND amount<thr)  1.434 city
 *
 * Đáp số kỳ vọng: 0% mọi thành phố (đã chứng minh — KHÔNG nới điều kiện).
 * Report (a)(b)(c): chạy explain 2 lần — mặc định vs tắt broadcast.
 */
object Task21 {
  def main(args: Array[String]): Unit = {
    val in  = if (args.length > 0) args(0) else "/lab/data/Amazon Sale Report.csv"
    val out = if (args.length > 1) args(1) else "/lab/out/Task_2-1_parquet"

    val spark = SparkCommon.session("Task2-1-CancelledPromotions")
    spark.sparkContext.setLogLevel("WARN")

    val base = SparkCommon.normalize(SparkCommon.readRaw(spark, in)).cache()

    // ---- KHỐI A: tuổi thọ mỗi mã KM, giữ mã sống >= 2 ngày ----
    // Bung promotion-ids thành (mã, ngày) rồi gom về min/max — KHÔNG dùng window.
    val validPromos = base
      .select(col("d"), explode(split(col("promotion-ids"), ",")).as("promo"))
      .withColumn("promo", trim(col("promo")))
      .filter(col("promo") =!= "" && col("d").isNotNull)
      .groupBy("promo")
      .agg(min("d").as("first_d"), max("d").as("last_d"))
      .withColumn("active_days", datediff(col("last_d"), col("first_d")))
      .filter(col("active_days") >= 2)          // C4: between = phép trừ -> 185 mã
      .select("promo")

    // ---- KHỐI B: ngưỡng trung bình theo BANG ----
    // Merchant AND Courier Status == "Shipped" (2 điều kiện, không phải 1) -> 40 bang.
    val stateThreshold = base
      .filter(col("Fulfilment") === "Merchant" && col("Courier Status") === "Shipped")
      .groupBy("state")
      .agg(avg("amount").as("state_avg"))

    // ---- KHỐI C: tập đơn ứng viên + đếm mã hợp lệ mỗi đơn ----
    // Ứng viên = Status chứa "cancelled" AND ship-service-level == Standard -> 6.909 dòng.
    // Gán id dòng để LEFT join lại sau khi đếm (mỗi dòng CSV = 1 đơn, xem C4).
    val candidates = base
      .filter(lower(col("Status")).contains("cancelled") &&
              col("ship-service-level") === "Standard")
      .withColumn("row_id", monotonically_increasing_id())
      .select("row_id", "city", "state", "amount", "promotion-ids")

    // Đếm số mã HỢP LỆ mỗi đơn: bung mã của ứng viên, giữ mã thuộc validPromos.
    // LEFT join để đơn không có mã vẫn còn ở mẫu số (nValid = 0).
    val validCount = candidates
      .select(col("row_id"), explode(split(col("promotion-ids"), ",")).as("promo"))
      .withColumn("promo", trim(col("promo")))
      .filter(col("promo") =!= "")
      .join(validPromos, Seq("promo"), "inner")   // giữ mã hợp lệ
      .groupBy("row_id")
      .agg(count("*").as("n_valid"))

    val enriched = candidates
      .join(validCount, Seq("row_id"), "left")               // LEFT: đơn 0 mã vẫn còn
      .withColumn("n_valid", coalesce(col("n_valid"), lit(0)))
      .join(stateThreshold, Seq("state"), "left")            // dán ngưỡng bang
      .withColumn("qualified",
        when(col("n_valid") >= 3 &&
             col("amount").isNotNull &&
             col("amount") < col("state_avg"), lit(1)).otherwise(lit(0)))

    // ---- KHỐI D: gộp theo thành phố -> % ----
    val result = enriched
      .groupBy("city")
      .agg(count("*").as("total_orders"),
           sum("qualified").as("qualified_orders"))
      .withColumn("pct_cancelled",
        round(col("qualified_orders") / col("total_orders") * 100, 4))
      .orderBy("city")

    // ---- REPORT (a)(b)(c): explain 2 lần ----
    // Lần 1: mặc định (kỳ vọng 3 BroadcastHashJoin, 4 Exchange, 0 Sort).
    println("========== EXPLAIN — DEFAULT ==========")
    result.explain(true)
    // Lần 2: tắt broadcast (kỳ vọng 3 SortMergeJoin, 7 Exchange, 6 Sort).
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    println("========== EXPLAIN — autoBroadcastJoinThreshold=-1 ==========")
    result.explain(true)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024) // trả mặc định

    // ---- Xuất 1 file parquet (đổi tên part-*.parquet -> Task_2-1.parquet ở README) ----
    SparkCommon.writeSingleParquet(result, out)
    println(s"[Task21] wrote $out  (rows=${result.count()})")

    spark.stop()
  }
}
