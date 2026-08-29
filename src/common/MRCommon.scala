import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}
import com.opencsv.CSVParserBuilder

/**
 * Tầng chung cho Task 1-1 và 1-2 (MapReduce).
 * Chốt theo ASSUMPTIONS.md — sửa ở đây, không rải rác trong mapper/reducer.
 *
 * MapReduce không có DataFrame reader nên phải tự: parse CSV (opencsv, hiểu quote),
 * parse ngày MM-dd-yy, chuẩn hoá bang, xếp hạng size.
 */
object MRCommon {

  // opencsv parser: cột promotion-ids chứa dấu phẩy trong ngoặc kép — split(",") là SAI.
  private val parser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build()

  /** Tách một dòng CSV thành mảng field, tôn trọng quote. Trả null nếu lỗi. */
  def parseLine(line: String): Array[String] =
    try parser.parseLine(line) catch { case _: Exception => null }

  // Vị trí cột (0-based) theo header của Amazon Sale Report.csv
  val C_DATE = 2; val C_STATUS = 3; val C_FULFIL = 4; val C_SERVICE = 6
  val C_STYLE = 7; val C_SKU = 8; val C_SIZE = 10; val C_COURIER = 12
  val C_QTY = 13; val C_AMOUNT = 15; val C_CITY = 16; val C_STATE = 17
  val C_PROMO = 20

  /** Header line — mapper bỏ qua dòng này. */
  def isHeader(line: String): Boolean = line.startsWith("index,")

  private val inFmt = new SimpleDateFormat("MM-dd-yy", Locale.US)
  private val outFmt = new SimpleDateFormat("yyyy-MM-dd", Locale.US)

  /** Parse ngày MM-dd-yy -> Date. null nếu hỏng. Bẫy ③. */
  def parseDate(s: String): Date =
    try inFmt.parse(s.trim) catch { case _: Exception => null }

  /** Date -> "yyyy-MM-dd" để làm khoá/ghi ra. */
  def fmtDate(d: Date): String = outFmt.format(d)

  /** Cộng n ngày (n âm = lùi) -> chuỗi yyyy-MM-dd. Dùng phát phiếu cửa sổ trượt. */
  def addDays(d: Date, n: Int): String = {
    val c = Calendar.getInstance(); c.setTime(d); c.add(Calendar.DAY_OF_MONTH, n)
    outFmt.format(c.getTime)
  }

  /** upper(trim) tên bang. Input asr.csv đã chuẩn hoá state ở clean.ipynb (37 bang). */
  def normState(s: String): String = if (s == null) "" else s.trim.toUpperCase(Locale.US)

  /** "shipped" = Status (chứa) 'shipped', không phải == 'Shipped'. Bẫy ①. */
  def isShipped(status: String): Boolean =
    status != null && status.toLowerCase(Locale.US).contains("shipped")

  /**
   * Thang số size — bẫy ④. Alphabet đặt 3XL trước XXL nên SAI.
   * Free = 0 (loại khỏi so sánh >= XXL). Size lạ -> -1.
   */
  private val sizeRank = Map(
    "XS" -> 1, "S" -> 2, "M" -> 3, "L" -> 4, "XL" -> 5, "XXL" -> 6,
    "3XL" -> 7, "4XL" -> 8, "5XL" -> 9, "6XL" -> 10, "FREE" -> 0)
  def rankOf(size: String): Int =
    if (size == null) -1 else sizeRank.getOrElse(size.trim.toUpperCase(Locale.US), -1)

  /** Đếm mã KM trong ô promotion-ids (kể cả Amazon). Ô trống -> 0. */
  def promoCount(cell: String): Int =
    if (cell == null || cell.trim.isEmpty) 0 else cell.split(",").count(_.trim.nonEmpty)
}
