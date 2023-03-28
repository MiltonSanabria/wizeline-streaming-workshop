import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object MovieRatings {
  case class Rating(userId: Int, movieId: Int, rating: Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MovieRatings").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("/tmp/checkpoint/")

    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val ratings = lines.map(parseRating)

    val movieRatings = ratings.map(rating => (rating.movieId, rating.rating))

    val movieAggregates = movieRatings.updateStateByKey(updateMovieAggregate)

    movieAggregates.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def parseRating(line: String): Rating = {
    val parts = line.split("#")
    Rating(parts(0).toInt, parts(1).toInt, parts(2).toDouble)
  }

  def updateMovieAggregate(newRatings: Seq[Double], currentAggregate: Option[(Double, Int)]): Option[(Double, Int)] = {
    val newCount = newRatings.size
    val newSum = newRatings.sum
    val currentCount = currentAggregate.map(_._2).getOrElse(0)
    val currentSum = currentAggregate.map(_._1 * currentCount).getOrElse(0.0)
    val totalCount = currentCount + newCount
    val totalSum = currentSum + newSum
    Some(totalSum / totalCount, totalCount)
  }
}
