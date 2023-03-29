import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AggregationExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AggregationExample").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    val windowedWordCounts = wordCounts.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))

    windowedWordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}


/*
=> 1
cat dog
dog dog

=> 2
owl cat

=> 3
dog owl
 */