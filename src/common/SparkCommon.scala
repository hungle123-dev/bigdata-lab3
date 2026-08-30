import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Tầng chung cho Task 2-1 và 2-2 (Spark).
 * Chốt theo ASSUMPTIONS.md — sửa ở đây, không rải rác trong từng task.
 */
object SparkCommon {

  /** SparkSession local; shuffle partitions hạ xuống 16 (dữ liệu ~129k dòng, mặc định 200 phí). */
  def session(app: String): SparkSession =
    SparkSession.builder()
      .appName(app)
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", "16")
      .getOrCreate()

  /**
   * Đọc CSV thô. multiLine + quote để cột promotion-ids (chứa dấu phẩy trong ngoặc kép)
   * không vỡ field. header=true, KHÔNG inferSchema (giữ mọi cột dạng String, tự ép sau).
   */
  def readRaw(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)

  /**
   * Chuẩn hoá cột dùng chung + thêm cột dẫn xuất. Trả DF có thêm:
   *   d      : Date (parse MM-dd-yy)          — bẫy ③
   *   state  : upper(trim(ship-state))        — input đã clean state (36 bang)
   *   city   : upper(trim(ship-city))
   *   amount : Amount ép Double (null giữ null) — bẫy ⑤
   *   nPromo : số mã trong promotion-ids       — parser đã tách bằng multiLine
   */
  def normalize(df: DataFrame): DataFrame =
    df.withColumn("d", to_date(col("Date"), "MM-dd-yy"))
      .withColumn("state", upper(trim(col("ship-state"))))
      .withColumn("city", upper(trim(col("ship-city"))))
      .withColumn("amount", col("Amount").cast("double"))
      .withColumn("nPromo", promoCount(col("promotion-ids")))

  /**
   * Đếm số mã khuyến mãi trong một ô promotion-ids.
   * Ô trống/null → 0. Kể cả mã Amazon (đề: "all promotions including Amazon's").
   * Tách bằng dấu phẩy — an toàn vì Spark đã bóc ô ra khỏi quote CSV.
   */
  def promoCount(c: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    when(c.isNull || trim(c) === "", lit(0))
      .otherwise(size(split(c, ",")))

  /**
   * Ghi DataFrame ra ĐÚNG MỘT file parquet đọc được bằng filesystem thường.
   * coalesce(1) ghi vào thư mục tạm, rồi copy part-*.parquet ra `target` (1 file phẳng).
   * KHÔNG getmerge (hỏng footer parquet).
   */
  def writeSingleParquet(df: DataFrame, dir: String, target: String): Unit = {
    df.coalesce(1).write.mode("overwrite").parquet(dir)
    val hconf = df.sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(dir), hconf)
    val part = fs.globStatus(new org.apache.hadoop.fs.Path(dir + "/part-*.parquet"))(0).getPath
    val dst = new org.apache.hadoop.fs.Path(target)
    fs.delete(dst, false)
    org.apache.hadoop.fs.FileUtil.copy(fs, part, fs, dst, false, hconf)
  }
}
