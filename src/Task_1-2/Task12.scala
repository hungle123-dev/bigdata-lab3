/**
 * TASK 1-2 · MapReduce · Median variety theo (bang, tháng)  [TV2]
 * ================================================================
 * Đề: median của "variety" theo mỗi (bang, tháng).
 *   - variety của 1 style = số SKU PHÂN BIỆT của style đó trong (bang, tháng)
 *   - chỉ xét style đã từng bán size >= XXL  (MRCommon.rankOf(size) >= 6 — KHÔNG alphabet!)
 *     alphabet đặt 3XL trước XXL -> 18.096 vs 34.627 dòng, lệch gần 2×
 *   - "style từng bán XXL": CHỐT TOÀN CỤC (ASSUMPTIONS C2) — report ghi cả 2 cách + 40/128
 *   - median số phần tử chẵn -> trung bình 2 phần tử giữa
 *
 * MapReduce 2 job:
 *   Job1: khoá (tháng, bang, style, sku) -> đếm SKU distinct bằng đếm lần ĐỔI khoá
 *         khi duyệt tuần tự (không cần HashSet trong bộ nhớ).
 *   Job2: khoá (tháng, bang) -> sort các variety, lấy giữa.
 *
 * Xuất Task_1-2.csv (~128 dòng theo cách trong-nhóm / khác nếu toàn cục).
 *
 * TODO(TV2):
 *   - Xác định style đủ điều kiện (>= XXL) toàn cục: có thể 1 job phụ hoặc filter khi map.
 *   - Job1 secondary sort theo sku để đếm distinct bằng đổi khoá.
 *   - Job2 sort variety, tính median (chẵn -> trung bình 2 giữa).
 *   - Xuất CSV 1 file: state,month,median_variety
 * Dùng: MRCommon.rankOf, parseDate (lấy tháng yyyy-MM), normState, C_STYLE, C_SKU, C_SIZE.
 */
object Task12 {
  def main(args: Array[String]): Unit = {
    println("TODO(TV2): implement Job1 (distinct SKU per style) + Job2 (median per state-month)")
    // hadoop jar target/lab3-1.0.jar Task12 /data/asr.csv /out/task12
  }
}
