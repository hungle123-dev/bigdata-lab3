 package fit.bigdata.lab2

import java.io._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, LocatedFileStatus, RemoteIterator}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object Task1_1_SlidingWindowMR {

  val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("MM-dd-yy")

  def parseCsvLine(line: String): Array[String] = {
    val result = new mutable.ArrayBuffer[String]()
    val cur = new StringBuilder
    var inQuotes = false
    var i = 0
    while (i < line.length) {
      val c = line.charAt(i)
      if (c == '"') inQuotes = !inQuotes
      else if (c == ',' && !inQuotes) {
        result += cur.toString()
        cur.setLength(0)
      } else {
        cur.append(c)
      }
      i += 1
    }
    result += cur.toString()
    result.toArray
  }

  // ==========================================
  // JOB 1: Count Orders per State
  // ==========================================
  class StateCountMapper extends Mapper[LongWritable, Text, Text, LongWritable] {
    private val one = new LongWritable(1L)
    private val stateKey = new Text()

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {
      val line = value.toString
      if (!line.startsWith("index,Order ID") && line.trim.nonEmpty) {
        val tokens = parseCsvLine(line)
        if (tokens.length > 17) {
          val status = tokens(3).trim.toLowerCase
          val qty = try { tokens(13).trim.toInt } catch { case _: Exception => 0 }
          val state = tokens(17).trim.toUpperCase

          if (status.contains("shipped") && qty > 0 && state.nonEmpty) {
            stateKey.set(state)
            context.write(stateKey, one)
          }
        }
      }
    }
  }

  class StateCountReducer extends Reducer[Text, LongWritable, Text, LongWritable] {
    private val totalCount = new LongWritable()

    override def reduce(key: Text, values: java.lang.Iterable[LongWritable], context: Reducer[Text, LongWritable, Text, LongWritable]#Context): Unit = {
      var sum = 0L
      val it = values.iterator()
      while (it.hasNext) {
        sum += it.next().get()
      }
      totalCount.set(sum)
      context.write(key, totalCount)
    }
  }

  // ==========================================
  // JOB 2: Map-to-Buckets & Combine
  // ==========================================
  class SlidingWindowMapper extends Mapper[LongWritable, Text, Text, Text] {
    private val stateWindowMap = mutable.Map[String, Int]()
    private val outKey = new Text()
    private val outVal = new Text()

    override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val configPath = context.getConfiguration.get("window.config.file")
      val fs = FileSystem.get(context.getConfiguration)
      val path = new Path(configPath)

      if (fs.exists(path)) {
        val reader = new BufferedReader(new InputStreamReader(fs.open(path)))
        var line = reader.readLine()
        while (line != null) {
          val parts = line.split("\t")
          if (parts.length >= 2) {
            val state = parts(0).trim.toUpperCase
            val count = parts(1).trim.toLong
            val w = if (count > 10000) 5 else 10
            stateWindowMap.put(state, w)
          }
          line = reader.readLine()
        }
        reader.close()
      }
    }

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val line = value.toString
      if (!line.startsWith("index,Order ID") && line.trim.nonEmpty) {
        val tokens = parseCsvLine(line)
        if (tokens.length > 17) {
          val dateStr = tokens(2).trim
          val status = tokens(3).trim.toLowerCase
          val size = tokens(10).trim
          val qty = try { tokens(13).trim.toDouble } catch { case _: Exception => 0.0 }
          val state = tokens(17).trim.toUpperCase

          if (status.contains("shipped") && qty > 0 && state.nonEmpty && size.nonEmpty) {
            try {
              val orderDate = LocalDate.parse(dateStr, DATE_FORMATTER)
              val windowLen = stateWindowMap.getOrElse(state, 10)

              var i = 1
              while (i <= windowLen) {
                val winDateStr = orderDate.plusDays(i).format(DATE_FORMATTER)
                outKey.set(s"$state|$winDateStr|$size")
                outVal.set(s"1\t$qty\t${qty * qty}")
                context.write(outKey, outVal)
                i += 1
              }
            } catch {
              case _: Exception =>
            }
          }
        }
      }
    }
  }

  class SlidingWindowCombiner extends Reducer[Text, Text, Text, Text] {
    private val combinedVal = new Text()

    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      var totalCount = 0L
      var sumQty = 0.0
      var sumQtySq = 0.0

      val it = values.iterator()
      while (it.hasNext) {
        val parts = it.next().toString.split("\t")
        if (parts.length == 3) {
          totalCount += parts(0).toLong
          sumQty += parts(1).toDouble
          sumQtySq += parts(2).toDouble
        }
      }
      combinedVal.set(s"$totalCount\t$sumQty\t$sumQtySq")
      context.write(key, combinedVal)
    }
  }

  class SlidingWindowReducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val keyParts = key.toString.split("\\|")
      if (keyParts.length == 3) {
        val state = keyParts(0)
        val winDate = keyParts(1)
        val size = keyParts(2)

        var count = 0L
        var sumQty = 0.0
        var sumQtySq = 0.0

        val it = values.iterator()
        while (it.hasNext) {
          val parts = it.next().toString.split("\t")
          if (parts.length == 3) {
            count += parts(0).toLong
            sumQty += parts(1).toDouble
            sumQtySq += parts(2).toDouble
          }
        }
        context.write(new Text(s"$state|$winDate"), new Text(s"$size#$count#$sumQty#$sumQtySq"))
      }
    }
  }

  // ==========================================
  // JOB 3: Mapper & Reducer xu ly Tie-Breaking
  // ==========================================
  class TieBreakerMapper extends Mapper[LongWritable, Text, Text, Text] {
    private val outKey = new Text()
    private val outVal = new Text()

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val line = value.toString.trim
      if (line.nonEmpty) {
        val parts = line.split("\t")
        if (parts.length == 2) {
          outKey.set(parts(0).trim)
          outVal.set(parts(1).trim)
          context.write(outKey, outVal)
        }
      }
    }
  }

  class FinalTieBreakerReducer extends Reducer[Text, Text, Text, Text] {
    private val nullKey = new Text("")
    private val outRow = new Text()

    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val parts = key.toString.split("\\|")
      if (parts.length == 2) {
        val state = parts(0)
        val winDate = parts(1)

        val sizeStats = new mutable.ArrayBuffer[(String, Long, Double)]()
        val it = values.iterator()

        while (it.hasNext) {
          val tokens = it.next().toString.split("#")
          if (tokens.length == 4) {
            val size = tokens(0)
            val count = tokens(1).toLong
            val sumQty = tokens(2).toDouble
            val sumQtySq = tokens(3).toDouble

            val mean = sumQty / count
            val variance = (sumQtySq / count) - (mean * mean)
            sizeStats += ((size, count, variance))
          }
        }

        if (sizeStats.nonEmpty) {
          var maxCount = -1L
          var i = 0
          while (i < sizeStats.length) {
            if (sizeStats(i)._2 > maxCount) maxCount = sizeStats(i)._2
            i += 1
          }

          val topFreq = sizeStats.filter(_._2 == maxCount)

          var minVar = Double.MaxValue
          i = 0
          while (i < topFreq.length) {
            if (topFreq(i)._3 < minVar) minVar = topFreq(i)._3
            i += 1
          }

          val topVar = topFreq.filter(s => math.abs(s._3 - minVar) < 1e-9)

          var winningSize = topVar(0)._1
          i = 1
          while (i < topVar.length) {
            if (topVar(i)._1.compareTo(winningSize) < 0) {
              winningSize = topVar(i)._1
            }
            i += 1
          }

          outRow.set(s"$state,$winDate,$winningSize")
          context.write(nullKey, outRow)
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: Task1_1_SlidingWindowMR <input_hdfs_csv> <output_local_csv>")
      System.exit(1)
    }

    val inputHdfsPath = args(0)
    val outputCsvPath = args(1)

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val tempJob1Out = new Path("/tmp/lab2_task1_1_counts")
    val tempJob2Out = new Path("/tmp/lab2_task1_1_stage1")
    val tempJob3Out = new Path("/tmp/lab2_task1_1_raw")

    if (fs.exists(tempJob1Out)) fs.delete(tempJob1Out, true)
    if (fs.exists(tempJob2Out)) fs.delete(tempJob2Out, true)
    if (fs.exists(tempJob3Out)) fs.delete(tempJob3Out, true)

    // JOB 1: Count orders per state
    val job1 = Job.getInstance(conf, "Task 1.1 - Step 1: Count Orders")
    job1.setJarByClass(Task1_1_SlidingWindowMR.getClass)
    job1.setMapperClass(classOf[StateCountMapper])
    job1.setCombinerClass(classOf[StateCountReducer])
    job1.setReducerClass(classOf[StateCountReducer])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[LongWritable])
    FileInputFormat.addInputPath(job1, new Path(inputHdfsPath))
    FileOutputFormat.setOutputPath(job1, tempJob1Out)
    if (!job1.waitForCompletion(true)) System.exit(1)

    // Merge Job 1
    val stateCountMergedFile = new Path("/tmp/lab2_task1_1_counts_merged.txt")
    if (fs.exists(stateCountMergedFile)) fs.delete(stateCountMergedFile, true)
    val mergeOut = fs.create(stateCountMergedFile)
    val mergeWriter = new BufferedWriter(new OutputStreamWriter(mergeOut))
    val fileIter1: RemoteIterator[LocatedFileStatus] = fs.listFiles(tempJob1Out, false)
    while (fileIter1.hasNext) {
      val st = fileIter1.next()
      if (st.getPath.getName.startsWith("part-")) {
        val in = new BufferedReader(new InputStreamReader(fs.open(st.getPath)))
        var l = in.readLine()
        while (l != null) {
          mergeWriter.write(l + "\n")
          l = in.readLine()
        }
        in.close()
      }
    }
    mergeWriter.close()

    // JOB 2: Map-to-Buckets
    conf.set("window.config.file", stateCountMergedFile.toString)
    val job2 = Job.getInstance(conf, "Task 1.1 - Step 2: Map-to-Buckets")
    job2.setJarByClass(Task1_1_SlidingWindowMR.getClass)
    job2.setMapperClass(classOf[SlidingWindowMapper])
    job2.setCombinerClass(classOf[SlidingWindowCombiner])
    job2.setReducerClass(classOf[SlidingWindowReducer])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job2, new Path(inputHdfsPath))
    FileOutputFormat.setOutputPath(job2, tempJob2Out)
    if (!job2.waitForCompletion(true)) System.exit(1)

    // JOB 3: Tie-Breaking
    val job3 = Job.getInstance(conf, "Task 1.1 - Step 3: Tie-Breaking")
    job3.setJarByClass(Task1_1_SlidingWindowMR.getClass)
    job3.setMapperClass(classOf[TieBreakerMapper])
    job3.setReducerClass(classOf[FinalTieBreakerReducer])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job3, tempJob2Out)
    FileOutputFormat.setOutputPath(job3, tempJob3Out)
    if (!job3.waitForCompletion(true)) System.exit(1)

    val localFile = new File(outputCsvPath)
    if (localFile.exists()) localFile.delete()
    if (localFile.getParentFile != null) localFile.getParentFile.mkdirs()

    val writer = new BufferedWriter(new FileWriter(localFile))
    writer.write("state,window_date,size\n")

    val fileIter3: RemoteIterator[LocatedFileStatus] = fs.listFiles(tempJob3Out, false)
    while (fileIter3.hasNext) {
      val status = fileIter3.next()
      if (status.getPath.getName.startsWith("part-")) {
        val reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath)))
        var line = reader.readLine()
        while (line != null) {
          val trimmed = line.trim
          if (trimmed.nonEmpty) writer.write(trimmed + "\n")
          line = reader.readLine()
        }
        reader.close()
      }
    }
    writer.close()
    println(s"[SUCCESS] Exported result to $outputCsvPath")
  }
}