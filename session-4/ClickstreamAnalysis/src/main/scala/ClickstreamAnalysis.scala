import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ClickstreamAnalysis {
  case class Click(ip: String, timestamp: Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ClickstreamAnalysis").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)

    val clicks = lines.map(parseClick)

    val windowedClicks = clicks.window(Seconds(30), Seconds(10))

    val clickCounts = windowedClicks.countByValue()

    clickCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def parseClick(line: String): Click = {
    val parts = line.split(",")
    Click(parts(0), parts(1).toDouble )
  }
}
