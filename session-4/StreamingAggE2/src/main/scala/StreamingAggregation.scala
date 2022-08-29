import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingAggregation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StreamingMultiAggregation")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val schema = StructType(Seq(
      StructField("transaction_id", LongType, true),
      StructField("card_id", StringType, true),
      StructField("user_id", StringType, true),
      StructField("purchase_id", LongType, true),
      StructField("store_id", LongType, true)
    ))


    val inputDF = (
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions")
        .load()
      )

    val transactionDF = inputDF.selectExpr("CAST(value AS STRING) as json",
      "CAST(timestamp as TIMESTAMP) as event_timestamp")
      .select(from_json(col("json"), schema)
        .alias("transactions"), col("event_timestamp"))
      .select(col("transactions.*"), col("event_timestamp"))
      .cube(window(col("event_timestamp"), "5 minute"), col("user_id")).count()


    transactionDF
      .writeStream
      .outputMode("update")
      .option("truncate", false)
      .option("numRows", 10)
      .format("console")
      .start()
      .awaitTermination()
  }

}
