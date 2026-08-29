/**
 * TASK 1-1 · MapReduce · Cửa sổ trượt độ dài động  [TV1 — Chuyên gia MapReduce]
 * ================================================================
 * Đề: mỗi (bang, ngày d), size bán chạy nhất trong cửa sổ TRƯỚC ngày d.
 *   - "bought" = Status chứa "shipped" AND Qty > 0   (MRCommon.isShipped, C_QTY)
 *   - cửa sổ L: bang >10.000 đơn bought -> 5 ngày [d-5,d-1]; còn lại -> 10 ngày [d-10,d-1]
 *   - KHÔNG gồm ngày d (off-by-one hay chết)
 *   - tie-break: (1) variance Amount thấp hơn — CHIA N; (2) size nhỏ hơn theo ALPHABET
 *
 * Thiết kế bắt buộc: map-to-buckets. 1 đơn ngày t phát phiếu cho mọi ngày kết quả
 *   d thuộc [t+1, t+L] -> khoá (state, d, size), value (1, q, q^2).
 * Bộ ba (count, sum, sumsq) cộng dồn được -> Combiner gộp tại map + tính variance.
 *
 * CẦN Job 0 chạy trước: đếm tổng đơn bought mỗi bang -> quyết L. Phát qua Distributed Cache.
 *   (chỉ MAHARASHTRA 19.103, KARNATAKA 14.950 vượt 10.000 trên 46 bang)
 *
 * Xuất Task_1-1.csv (~3.696 dòng), ngày tới 2022-07-09 (= 29/06 + 10).
 *
 * TODO(TV1):
 *   - Job0: Mapper đếm bought/bang; Reducer ghi bảng bang->L ra HDFS.
 *   - Job1: setup() đọc bảng L từ Distributed Cache;
 *           Mapper phát phiếu map-to-buckets; Combiner + Reducer gộp (count,sum,sumsq),
 *           chọn quán quân theo tie-break (secondary sort size A->Z, xem Phụ lục A1).
 *   - Xuất CSV 1 file: state,date,size  (coalesce/ghép part-file).
 * Dùng: MRCommon.isShipped, parseDate, addDays, normState, promoCount.
 */
object Task11 {
  def main(args: Array[String]): Unit = {
    println("TODO(TV1): implement Job0 (state->windowLen) + Job1 (map-to-buckets sliding window)")
    // hadoop jar target/lab3-1.0.jar Task11 /data/asr.csv /out/task11
  }
}
