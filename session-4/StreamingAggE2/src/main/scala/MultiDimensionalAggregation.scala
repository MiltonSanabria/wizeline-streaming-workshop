import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MultiDimensionalAggregation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StreamingMultiAggregation")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")

    sales.createOrReplaceTempView("sales")

    // Group By
    val groupByCityAndYear = sales
      .groupBy("city", "year") // <-- subtotals (city, year)
      .agg(sum("amount") as "amount")

    val groupByCityOnly = sales
      .groupBy("city") // <-- subtotals (city)
      .agg(sum("amount") as "amount")
      .select($"city", lit(null) as "year", $"amount") // <-- year is null

    val withUnion = groupByCityAndYear
      .union(groupByCityOnly)
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)

    withUnion.show

    // Grouping Sets
    val withGroupingSets = spark.sql(
      """
      SELECT city, year, SUM(amount) as amount
      FROM sales
      GROUP BY city, year
      GROUPING SETS ((city, year), (city))
      ORDER BY city DESC NULLS LAST, year ASC NULLS LAST
      """)

    withGroupingSets.show


    // The above query is semantically equivalent to the following
    val q1 = sales
      .groupBy("city", "year") // <-- subtotals (city, year)
      .agg(sum("amount") as "amount")
    val q2 = sales
      .groupBy("city") // <-- subtotals (city)
      .agg(sum("amount") as "amount")
      .select($"city", lit(null) as "year", $"amount") // <-- year is null
    val q3 = sales
      .groupBy() // <-- grand total
      .agg(sum("amount") as "amount")
      .select(lit(null) as "city", lit(null) as "year", $"amount") // <-- city and year are null
    val qq = q1
      .union(q2)
      .union(q3)
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)

    qq.show


    // Roll Up
    val withRollup = sales
      .rollup("city", "year")
      .agg(sum("amount") as "amount", grouping_id() as "gid")
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
      .filter(grouping_id() =!= 3)
      .select("city", "year", "amount")

    withRollup.show

    // Cube
    val q = sales.cube("city", "year")
      .agg(sum("amount") as "amount")
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)

    q.show




    /*
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

     */
  }

}
