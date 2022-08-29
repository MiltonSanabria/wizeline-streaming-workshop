import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingAggregation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StreamingAggregation")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = (
      new StructType()
        .add("ts", DoubleType)
        .add("device", StringType)
        .add("co", DoubleType)
        .add("humidity", DoubleType)
        .add("light", BooleanType)
        .add("lpg", DoubleType)
        .add("motion", BooleanType)
        .add("smoke", DoubleType)
        .add("temp", DoubleType)
      )

    val input_df = (spark
      .read
      .schema(schema)
      .csv("./src/test/resources")
      )

    val sensor_df = input_df.withColumn("ts", col("ts").cast("timestamp"))
    sensor_df.show(truncate = false)

    val groupByDevice = sensor_df.groupBy("device")

    groupByDevice.count().show()


    /*
    groupByDevice.min().show()
    groupByDevice.max().show()
    groupByDevice.sum().show()
    groupByDevice.avg().show()
    */

    val groupByDeviceAggHumidity = sensor_df.groupBy("device").agg(avg("humidity").alias("humidity_avg"))
    groupByDeviceAggHumidity.cache()
    groupByDeviceAggHumidity.createOrReplaceTempView("static_humidity_avg_view")

    val selectFromView = spark.sql(
      """select
        |     device
        |    ,humidity_avg
        |    from
        |    static_humidity_avg_view""".stripMargin)

    selectFromView.show(truncate = false)


    val streaming_df = (
      spark
        .readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .option("rowsPerSecond", 10)
        .csv("./src/test/resources")
      )

    val grouped_streaming_df = (
      streaming_df
        .groupBy(
          col("device"),
          window(col("ts").cast("timestamp"), "2 hour"))
        .agg(avg("humidity").alias("humidity_avg"), avg("lpg").alias("lpg_avg"))
      )

    grouped_streaming_df.isStreaming


    grouped_streaming_df
      .writeStream
      .option("truncate", false)
      .option("numRows", 10)
      .format("console")
      .queryName("streaming_query")
      .outputMode("update")
      .start()
      .awaitTermination()

  }
}
