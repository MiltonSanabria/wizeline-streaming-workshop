import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

object MovieRatingsStructuredStreaming {
  case class Rating(userId: Int, movieId: Int, rating: Double)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MovieRatingsStructuredStreaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val ratings = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .as[String]
      .map(parseRating)

    val movieRatings = ratings.select($"movieId", $"rating")

    val movieAggregates = movieRatings.groupBy($"movieId")
      .agg(avg($"rating").alias("averageRating"), count($"rating").alias("numRatings"))

    val query = movieAggregates.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def parseRating(line: String): Rating = {
    val parts = line.split("#")
    Rating(parts(0).toInt, parts(1).toInt, parts(2).toDouble)
  }

}
