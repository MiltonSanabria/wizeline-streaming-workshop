import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

object StreamingWatermarking {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StreamingWatermarking")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    // Create Streaming DataFrame by reading data from kafka topic.
    val initDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "data_streaming")
      .option("startingOffsets", "earliest")
      .load()

    // Define schema for kafka message
    val schema = new StructType()
      .add("event_time", TimestampType)
      .add("name", StringType)
      .add("count", IntegerType)



    // Create DataFrame  with event_timestamp and val column
    val eventDF = initDF.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"),schema).as("data")).select("data.*")



    // Without watermarking

//    val resultDF = eventDF
//      .groupBy(window(col("event_time"), "5 minute"), eventDF.col("name"))
//      .agg(sum("count").as("sum"))

    // Using watermark
    val resultDF = eventDF
      .withWatermark("event_time", "10 minutes")
      .groupBy(window(eventDF.col("event_time"), "5 minute"), eventDF.col("name"))
      .agg(sum("count").as("sum"))


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
