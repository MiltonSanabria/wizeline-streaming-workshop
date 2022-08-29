import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamingJoining {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StreamingJoining")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    val impressions = spark
      .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
      .select($"value".as("adId"), $"timestamp".as("impressionTime"))

    val clicks = spark
      .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
      .where((rand() * 100).cast("integer") < 10)       // 10 out of every 100 impressions result in a click
      .select(($"value" - 50).as("adId"), $"timestamp".as("clickTime"))   // -100 so that a click with same id as impression is generated much later.
      .where("adId > 0")

    var join = impressions.join(clicks, "adId")

    // Write dataframe to console
    var resultJoin = join
      .writeStream
      .outputMode("append") // complete, update, append
      //.trigger(Trigger.ProcessingTime("1 minute")) // Default, Fixed interval micro-batches, One-time micro-batch
      .option("truncate", value = false)
      .option("numRows", 10)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "checkpoint-2")
      .format("console")
      .start()



  }
}
