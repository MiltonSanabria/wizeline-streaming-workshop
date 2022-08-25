import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamingWatermarking {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StreamingWatermarking")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // We are going to listen to the following host and port
    val host = "127.0.0.1"
    val port = "9999"

    // Create Streaming DataFrame by reading data from socket.
    val initDF = spark
      .readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    // Create DataFrame  with event_timestamp and val column
    val eventDF = initDF.select(split(col("value"), "#").as("data"))
      .withColumn("event_timestamp", element_at(col("data"), 1).cast("timestamp"))
      .withColumn("val", element_at(col("data"), 2).cast("int"))
      .drop("data")


    // Without watermarking
    /*
    val resultDF = eventDF
      .groupBy(window(col("event_timestamp"), "5 minute"))
      .agg(sum("val").as("sum"))
    */


    val resultDF = eventDF
      .withWatermark("event_timestamp", "10 minutes")
      .groupBy(window(col("event_timestamp"), "5 minute"))
      .agg(sum("val").as("sum"))


    // Write dataframe to console
    resultDF
      .writeStream
      .outputMode("update") // complete, update, append
      //.trigger(Trigger.ProcessingTime("1 minute")) // Default, Fixed interval micro-batches, One-time micro-batch
      .option("truncate", false)
      .option("numRows", 10)
      //.option("checkpointLocation", "checkpoint")
      .format("console")
      .start()
      .awaitTermination()
  }
}
