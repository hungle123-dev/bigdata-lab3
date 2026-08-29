import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * TASK 2-2 · Spark DataFrame · Percentile động P90/P80  [TV4 — Thống kê & Tối ưu Spark]
 * ================================================================
 * Đề: mỗi (SKU, tháng), tính stddev_pop(Amount) của các đơn có số mã KM >= ngưỡng percentile.
 *   - nPromo = số mã trong promotion-ids (kể cả Amazon; ô trống=0)  [SparkCommon.promoCount]
 *   - ngưỡng ĐỘNG mỗi nhóm: P90 (top 10% dày mã), P80 (top 20%)
 *   - giữ đơn có nPromo >= ngưỡng -> stddev_pop (CHIA N, ddof=0 — KHÔNG stddev chia N-1!)
 *   - nhóm sau lọc < 2 đơn -> sd = 0.0  (coalesce null -> 0)
 *
 * BẮT BUỘC 2 cách tính ngưỡng + so sánh:
 *   (1) percentile_approx / approx_percentile có sẵn
 *   (2) tự implement EXACT = nội suy tuyến tính (ASSUMPTIONS C3) — đối trọng thật của approx
 *       (nếu chọn nearest-rank sẽ trùng approx -> report phần so sánh rỗng)
 *   dùng Window + row_number/percent_rank trên (SKU,tháng), KHÔNG UDF.
 *
 * Report: (a) độ chính xác ngưỡng, (b) thời gian (chạy >=5 lần, mean+sd), (c) nhóm lệch tập đơn.
 *   + repartition: 16.486 nhóm, max 426 đơn (~222KB) -> KHÔNG nhóm nào >1000 -> trả lời
 *     "không cần repartition, đây là số đo". Vấn đề thật: shuffle.partitions mặc định 200 phí.
 *
 * Xuất Task_2-2.parquet, 4 cột: sku, month, sd_p90_exact, sd_p90_approx, sd_p80_exact, sd_p80_approx.
 *
 * TODO(TV4):
 *   - đếm nPromo/đơn, group (SKU, tháng).
 *   - ngưỡng approx: percentile_approx(nPromo, 0.9/0.8) per group.
 *   - ngưỡng exact: nội suy tuyến tính tự code bằng Window.
 *   - join ngược ngưỡng vào từng đơn, filter >=, stddev_pop, coalesce 0.
 *   - harness benchmark 5 lần -> mean+sd cho report.
 * Dùng: SparkCommon.session, normalize (đã có nPromo), writeSingleParquet.
 */
object Task22 {
  def main(args: Array[String]): Unit = {
    println("TODO(TV4): P90/P80 approx vs exact, stddev_pop, benchmark 5x")
    // spark-submit --class Task22 target/lab3-1.0.jar <in.csv> <out_dir>
  }
}
