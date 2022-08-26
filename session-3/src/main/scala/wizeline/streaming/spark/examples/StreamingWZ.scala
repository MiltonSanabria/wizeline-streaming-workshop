package wizeline.streaming.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType,StringType,TimestampType,StructField,StructType}

object StreamingWZ extends Serializable{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Streaming Example")
      .config("spark.streaming.stopGracefullyOnShutDown","true")
      .config("spark.sql.streaming.schemaInference","true")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    val loadSchema = StructType(List(
      StructField("stock", StringType),
      StructField("timestamp",TimestampType),
      StructField("price",DoubleType)
    ))

    val rawDF = spark.readStream
      .format("json")
      .option("path","input")
      .schema(loadSchema)
      .option("multiLine",true)
      .load()

    val aggDF = rawDF
      .withWatermark("timestamp", "2 hour")
      .groupBy(col("stock"),
        window(col("timestamp"),"1 hour", "30 minute"))
      .agg(max("price").alias("MaxPrice"))

    val writterQuery = aggDF.writeStream
      .format("console")
      .option("path","output")
      .option("truncate", false)
      .option("checkpointLocation","checkpoint-dir")
      .outputMode("append")
      .queryName("StocksPrice")
      .trigger(Trigger.ProcessingTime(("10 second")))
      .start()

    logger.info("Program started")
    writterQuery.awaitTermination()

  }

}
