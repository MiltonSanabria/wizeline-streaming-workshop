import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._


object ClickstreamAnalysisStructuredStreaming {
  case class Click(userId: String, timestamp: String, url: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ClickstreamAnalysisStructuredStreaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val clicks = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .as[String]
      .map(parseClick)

    val df = clicks.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    val windowedCounts = df
      .groupBy(
        window(col("timestamp").cast("timestamp"), "10 minutes", "5 minutes"),
        $"userId"
      )
      .agg(count($"userId").alias("clickCount"))
      .orderBy($"window", $"clickCount".desc)

    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def parseClick(line: String): Click = {
    val parts = line.split("#")
    Click(parts(0), parts(1), parts(2))
  }
}
