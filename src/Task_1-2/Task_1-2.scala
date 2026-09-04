import org.apache.spark.sql.functions._

val df = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://master:9000/lab03/asr.csv")

val prepared = df.withColumn("parsed_date",to_date(col("Date"),"MM-dd-yy")).withColumn("month",date_format(col("parsed_date"),"yyyy-MM")).withColumn("size_rank",when(trim(col("Size"))==="XS",1).when(trim(col("Size"))==="S",2).when(trim(col("Size"))==="M",3).when(trim(col("Size"))==="L",4).when(trim(col("Size"))==="XL",5).when(trim(col("Size"))==="XXL",6).when(trim(col("Size"))==="3XL",7).when(trim(col("Size"))==="4XL",8).when(trim(col("Size"))==="5XL",9).when(trim(col("Size"))==="6XL",10))

val styleStats = prepared.groupBy("month","ship-state","Style").agg(countDistinct("SKU").alias("variety"),max("size_rank").alias("max_size_rank"))

val qualifiedStyles = styleStats.filter(col("max_size_rank") >= 6)

val medianData = qualifiedStyles.groupBy("month","ship-state").agg(sort_array(collect_list(col("variety"))).alias("varieties"))

val result = medianData.withColumn("n",size(col("varieties"))).withColumn("median_variety",when(col("n")%2===1,element_at(col("varieties"),((col("n")+1)/2).cast("int"))).otherwise((element_at(col("varieties"),(col("n")/2).cast("int"))+element_at(col("varieties"),(col("n")/2+1).cast("int")))/2.0)).select("month","ship-state","median_variety").orderBy("month","ship-state")

result.show(200,false)

result.coalesce(1).write.option("header","true").mode("overwrite").csv("file:///root/median_variety_output")