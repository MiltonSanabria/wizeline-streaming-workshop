import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object StreamingIntro {
  def main(args: Array[String]): Unit = {
    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Kafka Source Example")
      .getOrCreate()

    // Set Spark logging level to ERROR.
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val initDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("failOnDataLoss","false") // Whether to fail the query when it's possible that data is lost (e.g., topics are deleted, or offsets are out of range).
      .load()
      .select(col("value").cast("string"))

    val wordCount = initDf
      .select(explode(split(col("value"), " ")).alias("word"))
      .groupBy("word")
      .count()

    val result = wordCount.select(to_json(struct($"word", $"count")).alias("value"))

    // Save in a Kafka Topic
    result
      .writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "testConsumerSpark")
      .option("checkpointLocation", "checkpoint/kafka_checkpoint")
      .start()



    // Print in Console, runs micro-batch as soon as it can
    result
       .writeStream
       .outputMode(OutputMode.Update())
       .format("console")
       .option("truncate","false")
       .trigger(Trigger.ProcessingTime("2 seconds"))
       .start()
       .awaitTermination()
  }
}
