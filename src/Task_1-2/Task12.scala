import java.io.{
  BufferedReader,
  InputStreamReader,
  PrintWriter
}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.mutable


object Task_1_2 {

  // ============================================================
  // SIZE RANK
  // ============================================================

  val SIZE_RANK: Map[String, Int] = Map(
    "XS"  -> 1,
    "S"   -> 2,
    "M"   -> 3,
    "L"   -> 4,
    "XL"  -> 5,
    "XXL" -> 6,
    "3XL" -> 7,
    "4XL" -> 8,
    "5XL" -> 9,
    "6XL" -> 10
  )

  val DATE_FORMATTER =
    DateTimeFormatter.ofPattern("MM-dd-yy")


  // ============================================================
  // CSV PARSER
  //
  // KHÔNG dùng:
  //     line.split(",")
  //
  // Vì CSV có thể chứa:
  //     "ABC, XYZ"
  //
  // Parser này xử lý:
  //     - comma ,
  //     - quoted field "..."
  //     - comma bên trong quote
  //     - escaped quote ""
  // ============================================================

  def parseCsvLine(line: String): Array[String] = {

    val fields = mutable.ArrayBuffer[String]()
    val current = new StringBuilder

    var inQuotes = false
    var i = 0

    while (i < line.length) {

      val c = line.charAt(i)

      if (c == '"') {

        if (inQuotes && i + 1 < line.length &&
            line.charAt(i + 1) == '"') {

          // Escaped quote: ""
          current.append('"')
          i += 1

        } else {

          // Start / end quoted field
          inQuotes = !inQuotes
        }

      } else if (c == ',' && !inQuotes) {

        fields += current.toString
        current.clear()

      } else {

        current.append(c)
      }

      i += 1
    }

    // Last field
    fields += current.toString

    fields.toArray
  }


  // ============================================================
  // JOB 1 MAPPER
  //
  // Input:
  //     Original Amazon Sale Report CSV
  //
  // Key:
  //     month | state | style | sku
  //
  // Value:
  //     size rank
  //
  // Important:
  //     CSV is parsed using parseCsvLine()
  // ============================================================

  class Job1Mapper
      extends Mapper[LongWritable, Text, Text, IntWritable] {

    private val outputKey = new Text()
    private val outputValue = new IntWritable()

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[
          LongWritable,
          Text,
          Text,
          IntWritable
        ]#Context
    ): Unit = {

      val line = value.toString.trim

      if (line.nonEmpty) {

        val columns = parseCsvLine(line)

        /*
         * Standard Amazon Sale Report columns:
         *
         * 0  = index
         * 1  = Order ID
         * 2  = Date
         * 3  = Status
         * 4  = Fulfilment
         * 5  = Sales Channel
         * 6  = ship-service-level
         * 7  = Style
         * 8  = SKU
         * 9  = Category
         * 10 = Size
         * 11 = ASIN
         * 12 = Courier Status
         * 13 = Qty
         * 14 = currency
         * 15 = Amount
         * 16 = ship-city
         * 17 = ship-state
         * 18 = ship-postal-code
         * 19 = ship-country
         * 20 = B2B
         * 21 = fulfilled-by
         */

        if (columns.length > 17) {

          val date = columns(2).trim
          val style = columns(7).trim
          val sku = columns(8).trim
          val size = columns(10).trim
          val state = columns(17).trim

          // Skip header
          if (date != "Date" &&
              date.nonEmpty &&
              style.nonEmpty &&
              sku.nonEmpty &&
              state.nonEmpty) {

            try {

              val parsedDate =
                LocalDate.parse(
                  date,
                  DATE_FORMATTER
                )

              val month =
                parsedDate.toString.substring(0, 7)

              val sizeRank =
                SIZE_RANK.getOrElse(size, 0)

              /*
               * Only recognized clothing sizes
               */
              if (sizeRank > 0) {

                val compositeKey =
                  month + "|" +
                  state + "|" +
                  style + "|" +
                  sku

                outputKey.set(compositeKey)
                outputValue.set(sizeRank)

                context.write(
                  outputKey,
                  outputValue
                )
              }

            } catch {
              case _: Exception =>
                // Ignore malformed date
            }
          }
        }
      }
    }
  }


  // ============================================================
  // JOB 1 REDUCER
  //
  // Input:
  //     month|state|style|sku -> sizeRank
  //
  // Because the key already contains SKU,
  // one reducer call corresponds to ONE distinct SKU.
  //
  // If the same SKU appears multiple times with different sizes,
  // keep the maximum size rank.
  //
  // Output:
  //     month|state -> style|maxSizeRank
  // ============================================================

  class Job1Reducer
      extends Reducer[
        Text,
        IntWritable,
        Text,
        Text
      ] {

    private val outputKey = new Text()
    private val outputValue = new Text()

    override def reduce(
        key: Text,
        values: java.lang.Iterable[IntWritable],
        context: Reducer[
          Text,
          IntWritable,
          Text,
          Text
        ]#Context
    ): Unit = {

      val parts =
        key.toString.split("\\|", -1)

      if (parts.length == 4) {

        val month = parts(0)
        val state = parts(1)
        val style = parts(2)

        var maxRank = 0

        val iterator =
          values.iterator()

        while (iterator.hasNext) {

          val rank =
            iterator.next().get()

          if (rank > maxRank) {
            maxRank = rank
          }
        }

        /*
         * One output record = one distinct SKU.
         *
         * Therefore Job 2 can simply count records
         * belonging to each style.
         */

        outputKey.set(
          month + "|" + state
        )

        outputValue.set(
          style + "|" + maxRank
        )

        context.write(
          outputKey,
          outputValue
        )
      }
    }
  }


  // ============================================================
  // JOB 2 MAPPER
  //
  // Just forwards Job 1 output.
  //
  // Input:
  //     month|state    style|maxRank
  //
  // Output:
  //     same key/value
  // ============================================================

  class Job2Mapper
      extends Mapper[
        LongWritable,
        Text,
        Text,
        Text
      ] {

    private val outputKey = new Text()
    private val outputValue = new Text()

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[
          LongWritable,
          Text,
          Text,
          Text
        ]#Context
    ): Unit = {

      val line =
        value.toString.trim

      if (line.nonEmpty) {

        val tabIndex =
          line.indexOf('\t')

        if (tabIndex > 0) {

          val k =
            line.substring(0, tabIndex)

          val v =
            line.substring(tabIndex + 1)

          outputKey.set(k)
          outputValue.set(v)

          context.write(
            outputKey,
            outputValue
          )
        }
      }
    }
  }


  // ============================================================
  // JOB 2 REDUCER
  //
  // For each (month, state):
  //
  // 1. Group records by style
  // 2. Count distinct SKU -> variety
  // 3. Find maximum size rank for each style
  // 4. Keep styles with size >= XXL
  // 5. Sort variety values
  // 6. Calculate median
  //
  // Output:
  //     month,state    median
  // ============================================================

  class Job2Reducer
      extends Reducer[
        Text,
        Text,
        Text,
        Text
      ] {

    private val outputKey = new Text()
    private val outputValue = new Text()

    override def reduce(
        key: Text,
        values: java.lang.Iterable[Text],
        context: Reducer[
          Text,
          Text,
          Text,
          Text
        ]#Context
    ): Unit = {

      /*
       * style -> number of distinct SKU
       */
      val styleVariety =
        mutable.HashMap[String, Int]()

      /*
       * style -> maximum size rank
       */
      val styleMaxRank =
        mutable.HashMap[String, Int]()

      val iterator =
        values.iterator()

      while (iterator.hasNext) {

        val parts =
          iterator.next()
            .toString
            .split("\\|", -1)

        if (parts.length == 2) {

          val style = parts(0)

          val maxRank =
            parts(1).toInt

          // One Job1 record = one distinct SKU
          styleVariety.update(
            style,
            styleVariety.getOrElse(style, 0) + 1
          )

          val currentMax =
            styleMaxRank.getOrElse(
              style,
              0
            )

          if (maxRank > currentMax) {

            styleMaxRank.update(
              style,
              maxRank
            )
          }
        }
      }


      // --------------------------------------------------------
      // Keep only styles that have XXL or larger
      // --------------------------------------------------------

      val qualifiedVarieties =
        styleVariety.collect {

          case (style, variety)
              if styleMaxRank.getOrElse(
                style,
                0
              ) >= 6 =>

            variety

        }.toArray


      if (qualifiedVarieties.nonEmpty) {

        // ------------------------------------------------------
        // Sort
        // ------------------------------------------------------

        val sorted =
          qualifiedVarieties.sorted

        val n =
          sorted.length


        // ------------------------------------------------------
        // Median
        // ------------------------------------------------------

        val median: Double =

          if (n % 2 == 1) {

            // Odd
            sorted(n / 2).toDouble

          } else {

            // Even
            (
              sorted(n / 2 - 1) +
              sorted(n / 2)
            ) / 2.0
          }


        val keyParts =
          key.toString.split("\\|", -1)

        if (keyParts.length == 2) {

          val month = keyParts(0)
          val state = keyParts(1)

          /*
           * Keep Hadoop's internal output format:
           *
           * key = month|state
           * value = median
           *
           * The final CSV conversion happens later.
           */

          outputKey.set(
            month + "|" + state
          )

          outputValue.set(
            median.toString
          )

          context.write(
            outputKey,
            outputValue
          )
        }
      }
    }
  }


  // ============================================================
  // EXPORT HDFS OUTPUT -> LOCAL CSV
  //
  // Final:
  //
  //     Task_1-2.csv
  //
  // Example:
  //
  // month,state,median_variety
  // 2022-03,ANDHRA PRADESH,1.0
  // ...
  // ============================================================

  def exportToCsv(
      conf: Configuration,
      hdfsOutputPath: String,
      localCsvPath: String
  ): Unit = {

    val fs =
      FileSystem.get(
        new URI(hdfsOutputPath),
        conf
      )

    val partFile =
      new Path(
        hdfsOutputPath +
        "/part-r-00000"
      )

    if (!fs.exists(partFile)) {

      fs.close()

      throw new RuntimeException(
        "Cannot find Hadoop output: " +
        partFile
      )
    }


    val input =
      new BufferedReader(
        new InputStreamReader(
          fs.open(partFile),
          StandardCharsets.UTF_8
        )
      )

    val output =
      new PrintWriter(
        localCsvPath,
        "UTF-8"
      )


    try {

      // CSV header
      output.println(
        "month,state,median_variety"
      )


      var line =
        input.readLine()

      while (line != null) {

        if (line.trim.nonEmpty) {

          /*
           * Job2 output:
           *
           * 2022-04|MAHARASHTRA    4.0
           *
           * Convert to:
           *
           * 2022-04,MAHARASHTRA,4.0
           */

          val tabIndex =
            line.indexOf('\t')

          if (tabIndex > 0) {

            val key =
              line.substring(
                0,
                tabIndex
              )

            val median =
              line.substring(
                tabIndex + 1
              )

            val keyParts =
              key.split("\\|", -1)

            if (keyParts.length == 2) {

              output.println(
                keyParts(0) +
                "," +
                keyParts(1) +
                "," +
                median
              )
            }
          }
        }

        line =
          input.readLine()
      }

    } finally {

      input.close()
      output.close()
      fs.close()
    }


    println(
      "Final CSV created: " +
      localCsvPath
    )
  }


  // ============================================================
  // MAIN
  // ============================================================

  def main(
      args: Array[String]
  ): Unit = {

    if (args.length < 3) {

      System.err.println(
        """
          |Usage:
          |
          |hadoop jar <jar> Task_1_2 \
          |<input> <intermediate> <output>
          |
          |Example:
          |
          |hadoop jar Task_1-2.jar Task_1_2 \
          |hdfs://master:9000/lab03/asr.csv \
          |hdfs://master:9000/lab03/task1_2_intermediate \
          |hdfs://master:9000/lab03/task1_2_output
          |""".stripMargin
      )

      System.exit(1)
    }


    val inputPath =
      args(0)

    val intermediatePath =
      args(1)

    val outputPath =
      args(2)


    val conf =
      new Configuration()


    // ==========================================================
    // DELETE OLD OUTPUT
    // ==========================================================

    val intermediateFs =
      FileSystem.get(
        new URI(intermediatePath),
        conf
      )

    val outputFs =
      FileSystem.get(
        new URI(outputPath),
        conf
      )


    val intermediate =
      new Path(intermediatePath)

    val output =
      new Path(outputPath)


    if (intermediateFs.exists(intermediate)) {

      println(
        "Deleting old intermediate output..."
      )

      intermediateFs.delete(
        intermediate,
        true
      )
    }


    if (outputFs.exists(output)) {

      println(
        "Deleting old final Hadoop output..."
      )

      outputFs.delete(
        output,
        true
      )
    }


    intermediateFs.close()
    outputFs.close()


    // ==========================================================
    // JOB 1
    // ==========================================================

    println()
    println(
      "========================================"
    )
    println(
      "JOB 1: SKU Variety"
    )
    println(
      "========================================"
    )


    val job1 =
      Job.getInstance(
        conf,
        "Task 1-2 - Job 1 - SKU Variety"
      )


    job1.setJarByClass(
      Task_1_2.getClass
    )


    job1.setMapperClass(
      classOf[Job1Mapper]
    )

    job1.setReducerClass(
      classOf[Job1Reducer]
    )


    job1.setMapOutputKeyClass(
      classOf[Text]
    )

    job1.setMapOutputValueClass(
      classOf[IntWritable]
    )


    job1.setOutputKeyClass(
      classOf[Text]
    )

    job1.setOutputValueClass(
      classOf[Text]
    )


    FileInputFormat.addInputPath(
      job1,
      new Path(inputPath)
    )


    FileOutputFormat.setOutputPath(
      job1,
      new Path(intermediatePath)
    )


    val job1Success =
      job1.waitForCompletion(true)


    if (!job1Success) {

      System.err.println(
        "JOB 1 FAILED."
      )

      System.exit(1)
    }


    // ==========================================================
    // JOB 2
    // ==========================================================

    println()
    println(
      "========================================"
    )
    println(
      "JOB 2: Median Variety"
    )
    println(
      "========================================"
    )


    val job2 =
      Job.getInstance(
        conf,
        "Task 1-2 - Job 2 - Median Variety"
      )


    job2.setJarByClass(
      Task_1_2.getClass
    )


    job2.setMapperClass(
      classOf[Job2Mapper]
    )

    job2.setReducerClass(
      classOf[Job2Reducer]
    )


    job2.setMapOutputKeyClass(
      classOf[Text]
    )

    job2.setMapOutputValueClass(
      classOf[Text]
    )


    job2.setOutputKeyClass(
      classOf[Text]
    )

    job2.setOutputValueClass(
      classOf[Text]
    )


    /*
     * Only around 128 final groups.
     *
     * One reducer guarantees:
     *
     *     part-r-00000
     *
     * so that we can create one CSV file.
     */

    job2.setNumReduceTasks(1)


    FileInputFormat.addInputPath(
      job2,
      new Path(intermediatePath)
    )


    FileOutputFormat.setOutputPath(
      job2,
      new Path(outputPath)
    )


    val job2Success =
      job2.waitForCompletion(true)


    if (!job2Success) {

      System.err.println(
        "JOB 2 FAILED."
      )

      System.exit(1)
    }


    // ==========================================================
    // EXPORT FINAL CSV
    // ==========================================================

    println()
    println(
      "========================================"
    )
    println(
      "EXPORTING FINAL CSV"
    )
    println(
      "========================================"
    )


    exportToCsv(
      conf,
      outputPath,
      "Task_1-2.csv"
    )


    println()
    println(
      "========================================"
    )
    println(
      "TASK 1-2 COMPLETED"
    )
    println(
      "========================================"
    )

    println(
      "Final file: Task_1-2.csv"
    )
  }
}