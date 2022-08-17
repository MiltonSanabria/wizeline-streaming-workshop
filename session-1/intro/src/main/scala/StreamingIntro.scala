import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object StreamingIntro {
  def main(args: Array[String]): Unit = {
    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Kafka Source")
      .getOrCreate()

    // Set Spark logging level to ERROR.
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val initDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .select(col("value").cast("string"))

    val wordCount = initDf
      .select(explode(split(col("value"), " ")).alias("word"))
      .groupBy("word")
      .count()

    val result = wordCount.select(to_json(struct($"word", $"count")).alias(("value")))

    result
      .writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "testConsumer")
      .option("checkpointLocation", "checkpoint/kafka_checkpoint")
      .start()
      .awaitTermination()
  }
}
